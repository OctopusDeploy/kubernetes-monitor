package cluster

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/kubernetes"
)

func TestOnPopulateResourceInfoHandler_CachesManifest_ForDesiredResource(t *testing.T) {
	obj := kubernetes.NewUnstructuredBuilder().
		WithAPIVersion("v1").
		WithKind("Pod").
		WithNamespace("default").
		WithName("test").
		WithSpec(map[string]interface{}{
			"containers": []interface{}{
				map[string]interface{}{
					"name":  "example-container",
					"image": "nginx",
				},
			},
		}).
		Build()

	desiredResource := NewDesiredResourceBuilder().
		WithKind(obj.GetKind()).
		WithNamespace(obj.GetNamespace()).
		WithName(obj.GetName()).
		WithApiVersion(obj.GetAPIVersion()).
		WithManifest(obj).
		Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		Build()

	applicationInstanceList := NewApplicationInstanceList()
	applicationInstanceList.UpsertApplicationInstance(&applicationInstance)

	onPopulateResourceInfoHandlerFunc := onPopulateResourceInfoHandler(applicationInstanceList)

	expectedInfo := ResourceInfo{
		ResourceKey: kube.GetResourceKey(obj),
		OwnerRefs:   []v1.OwnerReference{{}},
	}

	info, keep := onPopulateResourceInfoHandlerFunc(obj, true)

	if diff := cmp.Diff(expectedInfo, info); diff != "" {
		t.Error(diff)
	}

	if !keep {
		t.Error("Expected to cache object, but discarded")
	}
}

func TestOnPopulateResourceInfoHandler_CachesManifest_ForChildrenDesiredResource(t *testing.T) {
	parentObj := kubernetes.NewUnstructuredBuilder().
		WithUID(types.UID(uuid.New().String())).
		WithAPIVersion("v1").
		WithKind("Deployment").
		WithNamespace("default").
		WithName("test-deployment").
		Build()

	parentOwnerRef := v1.OwnerReference{
		APIVersion: parentObj.GetAPIVersion(),
		Kind:       parentObj.GetKind(),
		Name:       parentObj.GetName(),
		UID:        parentObj.GetUID(),
	}

	childObj := kubernetes.NewUnstructuredBuilder().
		WithAPIVersion("v1").
		WithKind("Pod").
		WithNamespace("default").
		WithName("test").
		WithSpec(map[string]interface{}{
			"containers": []interface{}{
				map[string]interface{}{
					"name":  "example-container",
					"image": "nginx",
				},
			},
		}).
		WithOwnerReference(parentOwnerRef).
		Build()

	desiredResource := NewDesiredResourceBuilder().
		WithKind(parentObj.GetKind()).
		WithNamespace(parentObj.GetNamespace()).
		WithName(parentObj.GetName()).
		WithApiVersion(parentObj.GetAPIVersion()).
		WithManifest(parentObj).
		Build()

	presentMonitoredResource := NewPresentMonitoredResourceBuilder().
		ForDesiredResource(desiredResource).
		WithUID(parentObj.GetUID()).
		Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&presentMonitoredResource}).
		Build()

	applicationInstanceList := NewApplicationInstanceList()
	applicationInstanceList.UpsertApplicationInstance(&applicationInstance)

	onPopulateResourceInfoHandlerFunc := onPopulateResourceInfoHandler(applicationInstanceList)

	expectedParentInfo := ResourceInfo{
		ResourceKey: kube.GetResourceKey(parentObj),
		OwnerRefs:   []v1.OwnerReference{{}},
	}

	info, keep := onPopulateResourceInfoHandlerFunc(parentObj, true)

	if diff := cmp.Diff(expectedParentInfo, info); diff != "" {
		t.Error(diff)
	}
	if !keep {
		t.Error("Expected to cache object, but discarded")
	}

	expectedChildInfo := ResourceInfo{
		ResourceKey: kube.GetResourceKey(childObj),
		OwnerRefs:   []v1.OwnerReference{parentOwnerRef},
	}

	info, keep = onPopulateResourceInfoHandlerFunc(childObj, false)

	if diff := cmp.Diff(expectedChildInfo, info); diff != "" {
		t.Error(diff)
	}
	if !keep {
		t.Error("Expected to cache child object, but discarded")
	}
}

func TestOnPopulateResourceInfoHandler_DoesNotCacheManifest_ForUndesiredResources(t *testing.T) {
	t.Skip("We are currently caching manifests for all resources intentionally")

	obj := kubernetes.NewUnstructuredBuilder().
		WithAPIVersion("v1").
		WithKind("Pod").
		WithNamespace("default").
		WithName("test").
		WithSpec(map[string]interface{}{
			"containers": []interface{}{
				map[string]interface{}{
					"name":  "example-container",
					"image": "nginx",
				},
			},
		}).
		Build()

	applicationInstance := NewApplicationInstanceBuilder().
		Build()

	applicationInstanceList := NewApplicationInstanceList()
	applicationInstanceList.UpsertApplicationInstance(&applicationInstance)

	onPopulateResourceInfoHandlerFunc := onPopulateResourceInfoHandler(applicationInstanceList)

	expectedInfo := ResourceInfo{
		ResourceKey: kube.GetResourceKey(obj),
		OwnerRefs:   []v1.OwnerReference{{}},
	}

	info, keep := onPopulateResourceInfoHandlerFunc(obj, true)

	if diff := cmp.Diff(expectedInfo, info); diff != "" {
		t.Error(diff)
	}
	if keep {
		t.Error("Expected object manifest not to be cached")
	}
}

func TestNewCache_NilClientset_SkipsPermissionFilter(t *testing.T) {
	applicationInstances := NewApplicationInstanceList()
	c, filter, err := NewCache(
		t.Context(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		&rest.Config{Host: "http://example.invalid"},
		nil,
		applicationInstances,
		nil,
		false,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("NewCache returned error: %v", err)
	}
	if c == nil {
		t.Fatal("NewCache returned nil cache")
	}
	if filter != nil {
		t.Error("NewCache should return nil ResourceFilter when clientset is nil")
	}
}
