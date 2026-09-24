package cluster

import (
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache/mocks"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/kubernetes"
)

var (
	forbiddenGK = schema.GroupKind{Group: "eventing.keda.sh", Kind: "CloudEventSource"}
	deployGK    = schema.GroupKind{Group: "apps", Kind: "Deployment"}
)

func setupForbiddenResourceMockCache(
	apiResources []kube.APIResourceInfo,
	forbiddenGKs []schema.GroupKind,
) *mocks.ClusterCache {
	mockCache := &mocks.ClusterCache{}
	mockCache.On("GetAPIResources").Return(apiResources)

	for _, gk := range forbiddenGKs {
		mockCache.On("IsNamespaced", gk).Return(false, fmt.Errorf("not found"))
	}

	return mockCache
}

func addEmptyLiveObjectExpectations(mockCache *mocks.ClusterCache) {
	mockCache.On(
		"GetManagedLiveObjs",
		mock.AnythingOfType("[]*unstructured.Unstructured"),
		mock.AnythingOfType("func(*cache.Resource) bool"),
	).Return(map[kube.ResourceKey]*unstructured.Unstructured{}, nil).Once()
	mockCache.On(
		"IterateHierarchyV2",
		mock.AnythingOfType("[]kube.ResourceKey"),
		mock.AnythingOfType("func(*cache.Resource, map[kube.ResourceKey]*cache.Resource) bool"),
	).Return()
}

func newForbiddenResourceTestCluster(mockCache Cache) *Cluster {
	return &Cluster{
		ClusterId:            "cluster-id",
		logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		clusterCache:         mockCache,
		namespaceScopedMode:  true,
		targetNamespaces:     map[string]struct{}{"watched": {}},
		ApplicationInstances: NewApplicationInstanceList(),
	}
}

func newForbiddenDesiredResource(id DesiredResourceId, name string) DesiredResource {
	return NewDesiredResourceBuilder().
		WithId(id).
		WithName(name).
		WithNamespace("watched").
		WithKind("CloudEventSource").
		WithApiVersion("eventing.keda.sh/v1alpha1").
		WithManifest(
			kubernetes.NewUnstructuredBuilder().
				WithAPIVersion("eventing.keda.sh/v1alpha1").
				WithKind("CloudEventSource").
				WithName(name).
				WithNamespace("watched").
				Build(),
		).
		Build()
}

// TestGetMonitoredResources_ForbiddenResource_NoParentChild verifies that a forbidden
// desired resource is reported as unknown with a permission error message,
// rather than incorrectly reported as missing.
func TestGetMonitoredResources_ForbiddenResource_NoParentChild(t *testing.T) {
	mockCache := setupForbiddenResourceMockCache(
		[]kube.APIResourceInfo{
			{GroupKind: forbiddenGK, Meta: metav1.APIResource{Namespaced: true}},
		},
		[]schema.GroupKind{forbiddenGK},
	)
	addEmptyLiveObjectExpectations(mockCache)

	c := newForbiddenResourceTestCluster(mockCache)
	forbiddenDesired := newForbiddenDesiredResource("forbidden-id", "my-cloudeventsource")

	desiredResources := map[kube.ResourceKey]*DesiredResource{
		forbiddenDesired.ResourceKey(): &forbiddenDesired,
	}

	_, _, missing, unknown, err := c.getMonitoredResources(desiredResources, "salt", true)
	if !assert.NoError(t, err) {
		return
	}

	assert.Contains(t, unknown, forbiddenDesired.Id, "forbidden resource should be marked unknown")
	assert.NotContains(t, missing, forbiddenDesired.Id, "forbidden resource should not appear as missing")

	unknownResource := unknown[forbiddenDesired.Id]
	assert.Contains(t, unknownResource.Status.Message, "Insufficient permissions")
	assert.Contains(t, unknownResource.Status.Message, "CloudEventSource")
	assert.Contains(t, unknownResource.Status.Message, "eventing.keda.sh")

	mockCache.AssertExpectations(t)
}

// TestGetMonitoredResources_ForbiddenParentResource verifies that when a parent resource type
// is forbidden, it is reported as unknown with a permission error while a permitted sibling
// resource which is missing is still reported as such
func TestGetMonitoredResources_ForbiddenParentResource(t *testing.T) {
	mockCache := setupForbiddenResourceMockCache(
		[]kube.APIResourceInfo{
			{GroupKind: deployGK, Meta: metav1.APIResource{Namespaced: true}},
			{GroupKind: forbiddenGK, Meta: metav1.APIResource{Namespaced: true}},
		},
		[]schema.GroupKind{forbiddenGK},
	)
	mockCache.On("IsNamespaced", deployGK).Return(true, nil)
	addEmptyLiveObjectExpectations(mockCache)

	c := newForbiddenResourceTestCluster(mockCache)

	deployDesired := NewDesiredResourceBuilder().
		WithId("deploy-id").
		WithName("my-deployment").
		WithNamespace("watched").
		WithManifest(
			kubernetes.NewUnstructuredBuilder().
				WithAPIVersion("apps/v1").
				WithKind("Deployment").
				WithName("my-deployment").
				WithNamespace("watched").
				Build(),
		).
		Build()

	forbiddenDesired := newForbiddenDesiredResource("forbidden-parent-id", "my-cloudeventsource")

	desiredResources := map[kube.ResourceKey]*DesiredResource{
		deployDesired.ResourceKey():    &deployDesired,
		forbiddenDesired.ResourceKey(): &forbiddenDesired,
	}

	_, _, missing, unknown, err := c.getMonitoredResources(desiredResources, "salt", true)
	if !assert.NoError(t, err) {
		return
	}

	// Forbidden parent is reported as unknown with a permission error
	assert.Contains(t, unknown, forbiddenDesired.Id, "forbidden parent should be marked unknown")
	assert.NotContains(t, missing, forbiddenDesired.Id, "forbidden parent should not appear as missing")
	assert.Contains(t, unknown[forbiddenDesired.Id].Status.Message, "Insufficient permissions")

	// Permitted resource is correctly reported as missing (it's just not in the cluster yet)
	assert.Contains(t, missing, deployDesired.Id, "permitted resource should be in missing")
	assert.NotContains(t, unknown, deployDesired.Id, "permitted resource should not be unknown")

	mockCache.AssertExpectations(t)
}
