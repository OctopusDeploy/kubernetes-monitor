package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func makeNamespacedMap(typeMeta v1.TypeMeta, namespaced bool) map[v1.TypeMeta]bool {
	return map[v1.TypeMeta]bool{typeMeta: namespaced}
}

func podTypeMeta() v1.TypeMeta {
	return v1.TypeMeta{Kind: "Pod", APIVersion: "v1"}
}

func makeDesiredResource(name, manifestNs, assumedNs string) *DesiredResource {
	manifest := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]interface{}{
				"name": name,
			},
		},
	}
	if manifestNs != "" {
		manifest.SetNamespace(manifestNs)
	}

	return &DesiredResource{
		Id: "test-id",
		Details: ResourceDetails{
			Name:         name,
			Namespace:    "",
			ManifestType: podTypeMeta(),
		},
		AssumedNamespace:    assumedNs,
		IsNamespaceResolved: false,
		Manifest:            manifest,
	}
}

func TestResolveNamespace_SetsManifestNamespace_WhenMissing(t *testing.T) {
	dr := makeDesiredResource("counter", "", "app1")
	namespacedMap := makeNamespacedMap(podTypeMeta(), true)

	resolved := dr.ResolveNamespace(namespacedMap)

	assert.True(t, resolved)
	assert.True(t, dr.IsNamespaceResolved)
	assert.False(t, dr.IsClusterScoped)
	assert.Equal(t, "app1", dr.Details.Namespace)
	assert.Equal(t, "app1", dr.Manifest.GetNamespace())
}

func TestResolveNamespace_PreservesExistingManifestNamespace(t *testing.T) {
	dr := makeDesiredResource("counter", "explicit-ns", "default-ns")
	namespacedMap := makeNamespacedMap(podTypeMeta(), true)

	resolved := dr.ResolveNamespace(namespacedMap)

	assert.True(t, resolved)
	assert.Equal(t, "default-ns", dr.Details.Namespace)
	// Manifest keeps its original namespace — we don't overwrite
	assert.Equal(t, "explicit-ns", dr.Manifest.GetNamespace())
}

func TestResolveNamespace_DoesNotSetNamespace_WhenClusterScoped(t *testing.T) {
	dr := makeDesiredResource("my-node", "", "")
	namespacedMap := makeNamespacedMap(podTypeMeta(), false)

	resolved := dr.ResolveNamespace(namespacedMap)

	assert.True(t, resolved)
	assert.True(t, dr.IsClusterScoped)
	assert.Equal(t, "", dr.Details.Namespace)
	assert.Equal(t, "", dr.Manifest.GetNamespace())
}

func TestResolveNamespace_ReturnsFalse_WhenTypeNotInMap(t *testing.T) {
	dr := makeDesiredResource("counter", "", "app1")
	emptyMap := map[v1.TypeMeta]bool{}

	resolved := dr.ResolveNamespace(emptyMap)

	assert.False(t, resolved)
	assert.False(t, dr.IsNamespaceResolved)
	assert.Equal(t, "", dr.Manifest.GetNamespace())
}

func TestResourceKey_UsesManifestGVK(t *testing.T) {
	dr := makeDesiredResource("counter", "", "app1")
	dr.IsNamespaceResolved = true
	dr.Details.Namespace = "resolved-ns"

	key := dr.ResourceKey()

	assert.Equal(t, "Pod", key.Kind)
	assert.Equal(t, "", key.Group)
	assert.Equal(t, "resolved-ns", key.Namespace)
	assert.Equal(t, "counter", key.Name)
}

func TestResourceKey_FallsBackToDetailsManifestType_WhenManifestNil(t *testing.T) {
	dr := &DesiredResource{
		Id: "test-id",
		Details: ResourceDetails{
			Name:         "counter",
			Namespace:    "resolved-ns",
			ManifestType: v1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		},
		IsNamespaceResolved: true,
		Manifest:            nil,
	}

	key := dr.ResourceKey()

	assert.Equal(t, "Pod", key.Kind)
	assert.Equal(t, "", key.Group)
	assert.Equal(t, "resolved-ns", key.Namespace)
	assert.Equal(t, "counter", key.Name)
}

func TestResolveNamespace_Idempotent(t *testing.T) {
	dr := makeDesiredResource("counter", "", "app1")
	namespacedMap := makeNamespacedMap(podTypeMeta(), true)

	dr.ResolveNamespace(namespacedMap)
	dr.ResolveNamespace(namespacedMap)

	assert.Equal(t, "app1", dr.Details.Namespace)
	assert.Equal(t, "app1", dr.Manifest.GetNamespace())
}
