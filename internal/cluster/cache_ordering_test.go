package cluster

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube/kubetest"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakediscovery "k8s.io/client-go/discovery/fake"
	testcore "k8s.io/client-go/testing"

	"github.com/octopusdeploy/kubernetes-monitor/internal/kubernetes"
)

type MockCachedDiscoveryClient struct {
	*fakediscovery.FakeDiscovery
}

func (m *MockCachedDiscoveryClient) Fresh() bool { return true }
func (m *MockCachedDiscoveryClient) Invalidate() {}

// TestGetMonitoredResources_AfterOutOfOrderPopulate_DoesNotCrash populates the
// real cluster cache with watch events in child-before-parents order, then
// calls getMonitoredResources. Crashes if onPopulateResourceInfoHandler is
// reverted to the pre-#125 conditional return, unless the sanitising call
// sites tolerate a nil manifest.
func TestGetMonitoredResources_AfterOutOfOrderPopulate_DoesNotCrash(t *testing.T) {
	const namespace = "default"

	deploymentUID := types.UID(uuid.NewString())
	deployment := kubernetes.NewUnstructuredBuilder().
		WithUID(deploymentUID).
		WithAPIVersion("apps/v1").WithKind("Deployment").
		WithNamespace(namespace).WithName("app").
		Build()

	rsUID := types.UID(uuid.NewString())
	replicaSet := kubernetes.NewUnstructuredBuilder().
		WithUID(rsUID).
		WithAPIVersion("apps/v1").WithKind("ReplicaSet").
		WithNamespace(namespace).WithName("app-rs").
		WithOwnerReference(metav1.OwnerReference{
			APIVersion: "apps/v1", Kind: "Deployment", Name: "app", UID: deploymentUID,
		}).
		Build()

	pod := kubernetes.NewUnstructuredBuilder().
		WithUID(types.UID(uuid.NewString())).
		WithAPIVersion("v1").WithKind("Pod").
		WithNamespace(namespace).WithName("app-pod").
		WithOwnerReference(metav1.OwnerReference{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "app-rs", UID: rsUID,
		}).
		Build()

	// The fake client doesn't stamp resourceVersions; RetryWatcher drops
	// events whose RVs are empty.
	deployment.SetResourceVersion("1")
	replicaSet.SetResourceVersion("1")
	pod.SetResourceVersion("1")

	desired := NewDesiredResourceBuilder().
		WithKind("Deployment").WithApiVersion("apps/v1").
		WithNamespace(namespace).WithName("app").
		WithManifest(deployment).
		Build()

	appList := NewApplicationInstanceList()
	appInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desired}).
		Build()
	appList.UpsertApplicationInstance(&appInstance)

	cc, dyn, disc := newFakeClusterCacheForOrderingTest(t, appList)
	defer cc.Invalidate()

	events := make(chan kube.ResourceKey, 16)
	cc.OnResourceUpdated(func(newRes, _ *cache.Resource, _ map[kube.ResourceKey]*cache.Resource) {
		if newRes == nil {
			return
		}
		events <- kube.NewResourceKey(
			newRes.Ref.GroupVersionKind().Group, newRes.Ref.Kind,
			newRes.Ref.Namespace, newRes.Ref.Name,
		)
	})

	if err := cc.EnsureSynced(); err != nil {
		t.Fatalf("EnsureSynced: %v", err)
	}
	// Watch goroutines attach async after EnsureSynced.
	time.Sleep(200 * time.Millisecond)

	createAndWait(t, dyn, namespace, pod, podGVR, kube.GetResourceKey(pod), events)
	createAndWait(t, dyn, namespace, replicaSet, rsGVR, kube.GetResourceKey(replicaSet), events)
	createAndWait(t, dyn, namespace, deployment, deploymentGVR, kube.GetResourceKey(deployment), events)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	testCluster := &Cluster{
		ClusterId:             ClusterId("test"),
		ApplicationInstances:  appList,
		mutex:                 NewMutexWithLogging(logger, "Cluster"),
		logger:                logger,
		clusterCache:          cc,
		dynamicClient:         dyn,
		cachedDiscoveryClient: disc,
		// getMonitoredResources no longer syncs — callers do — so the watch-driven state survives into the assertion
	}

	desiredMap := map[kube.ResourceKey]*DesiredResource{desired.ResourceKey(): &desired}
	if _, _, _, _, err := testCluster.getMonitoredResources(desiredMap, "salt", true); err != nil {
		t.Fatalf("getMonitoredResources: %v", err)
	}
}

var (
	podGVR        = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	rsGVR         = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}
	deploymentGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
)

func newFakeClusterCacheForOrderingTest(
	t *testing.T,
	appList *ApplicationInstanceList,
) (cache.ClusterCache, dynamic.Interface, discovery.CachedDiscoveryInterface) {
	t.Helper()

	client := fake.NewSimpleDynamicClient(scheme.Scheme)
	discoveryFake := &testcore.Fake{Resources: []*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Namespaced: true, Kind: "Pod"},
			},
		},
		{
			GroupVersion: "apps/v1",
			APIResources: []metav1.APIResource{
				{Name: "deployments", Namespaced: true, Kind: "Deployment"},
				{Name: "replicasets", Namespaced: true, Kind: "ReplicaSet"},
			},
		},
	}}
	discoveryClient := MockCachedDiscoveryClient{&fakediscovery.FakeDiscovery{Fake: discoveryFake}}

	// Stamp a resourceVersion on list responses — gitops-engine rejects empty RVs.
	base := client.ReactionChain[0]
	client.PrependReactor("list", "*", func(action testcore.Action) (bool, runtime.Object, error) {
		handled, ret, err := base.React(action)
		if err != nil || !handled {
			return handled, ret, err
		}
		ret.(metav1.ListInterface).SetResourceVersion("1")
		return handled, ret, err
	})

	mock := &kubetest.MockKubectlCmd{
		DynamicClient: client,
		APIResources: []kube.APIResourceInfo{
			{
				GroupKind: schema.GroupKind{Kind: "Pod"}, GroupVersionResource: podGVR,
				Meta: metav1.APIResource{Namespaced: true},
			},
			{
				GroupKind: schema.GroupKind{Group: "apps", Kind: "ReplicaSet"}, GroupVersionResource: rsGVR,
				Meta: metav1.APIResource{Namespaced: true},
			},
			{
				GroupKind: schema.GroupKind{Group: "apps", Kind: "Deployment"}, GroupVersionResource: deploymentGVR,
				Meta: metav1.APIResource{Namespaced: true},
			},
		},
	}

	cc := cache.NewClusterCache(
		&rest.Config{Host: "https://test"},
		cache.SetKubectl(mock),
		cache.SetPopulateResourceInfoHandler(onPopulateResourceInfoHandler(appList)),
	)
	return cc, client, &discoveryClient
}

func createAndWait(
	t *testing.T,
	dyn dynamic.Interface,
	namespace string,
	obj *unstructured.Unstructured,
	gvr schema.GroupVersionResource,
	expected kube.ResourceKey,
	events <-chan kube.ResourceKey,
) {
	t.Helper()
	if _, err := dyn.Resource(gvr).
		Namespace(namespace).
		Create(context.Background(), obj, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create %s: %v", expected, err)
	}
	select {
	case got := <-events:
		if got != expected {
			t.Fatalf("expected cache event for %s, got %s", expected, got)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for cache event for %s", expected)
	}
}
