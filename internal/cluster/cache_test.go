package cluster

import (
	"testing"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/kubernetes"
)

func newTestPod(name string, ownerRefs ...v1.OwnerReference) *unstructured.Unstructured {
	builder := kubernetes.NewUnstructuredBuilder().
		WithAPIVersion("v1").
		WithKind("Pod").
		WithNamespace("default").
		WithName(name).
		WithSpec(map[string]interface{}{
			"containers": []interface{}{
				map[string]interface{}{
					"name":  "example-container",
					"image": "nginx",
				},
			},
		})
	for _, ownerRef := range ownerRefs {
		builder = builder.WithOwnerReference(ownerRef)
	}
	return builder.Build()
}

func newTestResourceInterests(t *testing.T, keys ...kube.ResourceKey) *resourceInterests {
	t.Helper()
	interests := newResourceInterests(t.Context(), discardLogger())
	keySet := map[kube.ResourceKey]struct{}{}
	for _, key := range keys {
		keySet[key] = struct{}{}
	}
	if err := interests.set(t.Context(), testClusterId, keySet); err != nil {
		t.Fatalf("setting interests: %v", err)
	}
	return interests
}

func TestPopulateResourceInfo_CachesManifest_ForDesiredResource(t *testing.T) {
	obj := newTestPod("test")
	interests := newTestResourceInterests(t, kube.GetResourceKey(obj))

	expectedInfo := ResourceInfo{
		ResourceKey: kube.GetResourceKey(obj),
		OwnerRefs:   []v1.OwnerReference{{}},
	}

	info, keep := interests.populateResourceInfo(obj, true)

	if diff := cmp.Diff(expectedInfo, info); diff != "" {
		t.Error(diff)
	}

	if !keep {
		t.Error("Expected to cache object, but discarded")
	}
}

func TestPopulateResourceInfo_CachesManifest_ForChildrenOfTrackedResource(t *testing.T) {
	parentObj := kubernetes.NewUnstructuredBuilder().
		WithUID(types.UID(uuid.New().String())).
		WithAPIVersion("apps/v1").
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

	childObj := newTestPod("test", parentOwnerRef)

	interests := newTestResourceInterests(t, kube.GetResourceKey(parentObj))

	expectedParentInfo := ResourceInfo{
		ResourceKey: kube.GetResourceKey(parentObj),
		OwnerRefs:   []v1.OwnerReference{{}},
	}

	info, keep := interests.populateResourceInfo(parentObj, true)

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

	info, keep = interests.populateResourceInfo(childObj, false)

	if diff := cmp.Diff(expectedChildInfo, info); diff != "" {
		t.Error(diff)
	}
	if !keep {
		t.Error("Expected to cache child object, but discarded")
	}

	// The child inherits its owner's interest, so its own children are recognised during the same listing.
	grandchildObj := newTestPod("grandchild", v1.OwnerReference{
		APIVersion: childObj.GetAPIVersion(),
		Kind:       childObj.GetKind(),
		Name:       childObj.GetName(),
	})
	if _, keep := interests.populateResourceInfo(grandchildObj, false); !keep {
		t.Error("Expected to cache grandchild object, but discarded")
	}
}

func TestPopulateResourceInfo_DoesNotCacheManifest_ForUntrackedRootResource(t *testing.T) {
	obj := newTestPod("test")
	interests := newTestResourceInterests(t)

	expectedInfo := ResourceInfo{
		ResourceKey: kube.GetResourceKey(obj),
		OwnerRefs:   []v1.OwnerReference{{}},
	}

	info, keep := interests.populateResourceInfo(obj, true)

	if diff := cmp.Diff(expectedInfo, info); diff != "" {
		t.Error(diff)
	}
	if keep {
		t.Error("Expected object manifest not to be cached")
	}
}

func TestNewCache_NilClientset_SkipsPermissionFilter(t *testing.T) {
	c, filter, err := newCache(
		t.Context(),
		discardLogger(),
		&rest.Config{Host: "http://example.invalid"},
		nil,
		newResourceInterests(t.Context(), discardLogger()),
		nil,
		false,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("newCache returned error: %v", err)
	}
	if c == nil {
		t.Fatal("newCache returned nil cache")
	}
	if filter != nil {
		t.Error("newCache should return nil ResourceFilter when clientset is nil")
	}
}
