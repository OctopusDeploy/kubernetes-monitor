package cluster

import (
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	deploymentKey = kube.NewResourceKey("apps", "Deployment", "default", "web")
	replicaSetKey = kube.NewResourceKey("apps", "ReplicaSet", "default", "web-123")
	otherKey      = kube.NewResourceKey("apps", "Deployment", "default", "other")
)

func ownedBy(kind, apiVersion, name string) []metav1.OwnerReference {
	return []metav1.OwnerReference{{APIVersion: apiVersion, Kind: kind, Name: name}}
}

func testResource(key kube.ResourceKey, apiVersion string, ownerRefs []metav1.OwnerReference) *cache.Resource {
	return &cache.Resource{
		Ref: v1.ObjectReference{
			APIVersion: apiVersion,
			Kind:       key.Kind,
			Namespace:  key.Namespace,
			Name:       key.Name,
		},
		OwnerRefs: ownerRefs,
		Info:      ResourceInfo{ResourceKey: key, OwnerRefs: ownerRefs},
	}
}

func testUnstructured(
	key kube.ResourceKey,
	apiVersion string,
	ownerRefs []metav1.OwnerReference,
) *unstructured.Unstructured {
	un := &unstructured.Unstructured{}
	un.SetAPIVersion(apiVersion)
	un.SetKind(key.Kind)
	un.SetNamespace(key.Namespace)
	un.SetName(key.Name)
	un.SetOwnerReferences(ownerRefs)
	return un
}

func keys(resourceKeys ...kube.ResourceKey) map[kube.ResourceKey]struct{} {
	set := make(map[kube.ResourceKey]struct{}, len(resourceKeys))
	for _, key := range resourceKeys {
		set[key] = struct{}{}
	}
	return set
}

// subscribeTestTargets subscribes buffered channels, so routing never waits on a reader and its result can
// be checked as soon as route returns.
func subscribeTestTargets(
	t *testing.T,
	interests *resourceInterests,
	targets ...ClusterId,
) map[ClusterId]chan resourceChange {
	t.Helper()
	channels := map[ClusterId]chan resourceChange{}
	for _, target := range targets {
		changes := make(chan resourceChange, 10)
		require.NoError(t, interests.subscribe(t.Context(), target, changes))
		channels[target] = changes
	}
	return channels
}

func received(changes chan resourceChange) []resourceChange {
	var all []resourceChange
	for {
		select {
		case change := <-changes:
			all = append(all, change)
		default:
			return all
		}
	}
}

func indexSizes(t *testing.T, interests *resourceInterests) (targets, resources int) {
	t.Helper()
	type sizes struct{ targets, resources int }
	result, err := ask(t.Context(), interests.mailbox, func(index *interestIndex) sizes {
		total := 0
		for _, targetKeys := range index.byTarget {
			total += len(targetKeys)
		}
		return sizes{total, len(index.byResource)}
	})
	require.NoError(t, err)
	return result.targets, result.resources
}

func TestResourceInterests_Route(t *testing.T) {
	t.Run("Delivers a change only to targets interested in the resource", func(t *testing.T) {
		interests := newResourceInterests(t.Context(), discardLogger())
		channels := subscribeTestTargets(t, interests, "a", "b")
		require.NoError(t, interests.set(t.Context(), "a", keys(deploymentKey)))
		require.NoError(t, interests.set(t.Context(), "b", keys(otherKey)))

		interests.route(testResource(deploymentKey, "apps/v1", nil), nil, nil)

		assert.Len(t, received(channels["a"]), 1)
		assert.Empty(t, received(channels["b"]))
	})

	t.Run("Delivers a change to targets interested in the resource's owner", func(t *testing.T) {
		interests := newResourceInterests(t.Context(), discardLogger())
		channels := subscribeTestTargets(t, interests, "a", "b")
		require.NoError(t, interests.set(t.Context(), "a", keys(deploymentKey)))
		require.NoError(t, interests.set(t.Context(), "b", keys(otherKey)))

		interests.route(testResource(replicaSetKey, "apps/v1", ownedBy("Deployment", "apps/v1", "web")), nil, nil)

		assert.Len(t, received(channels["a"]), 1)
		assert.Empty(t, received(channels["b"]))
	})

	t.Run("Delivers a removal to targets interested in what was removed", func(t *testing.T) {
		interests := newResourceInterests(t.Context(), discardLogger())
		channels := subscribeTestTargets(t, interests, "a")
		require.NoError(t, interests.set(t.Context(), "a", keys(deploymentKey)))

		interests.route(nil, testResource(deploymentKey, "apps/v1", nil), nil)

		assert.Len(t, received(channels["a"]), 1)
	})

	t.Run("Delivers a shared resource to every interested target until one stops caring", func(t *testing.T) {
		interests := newResourceInterests(t.Context(), discardLogger())
		channels := subscribeTestTargets(t, interests, "a", "b")
		require.NoError(t, interests.set(t.Context(), "a", keys(deploymentKey)))
		require.NoError(t, interests.set(t.Context(), "b", keys(deploymentKey)))

		interests.route(testResource(deploymentKey, "apps/v1", nil), nil, nil)
		assert.Len(t, received(channels["a"]), 1)
		assert.Len(t, received(channels["b"]), 1)

		require.NoError(t, interests.set(t.Context(), "a", keys()))
		interests.route(testResource(deploymentKey, "apps/v1", nil), nil, nil)

		assert.Empty(t, received(channels["a"]))
		assert.Len(t, received(channels["b"]), 1, "one target dropping a resource mustn't stop another receiving it")
	})

	t.Run("Ignores changes nobody is interested in", func(t *testing.T) {
		interests := newResourceInterests(t.Context(), discardLogger())
		channels := subscribeTestTargets(t, interests, "a")

		interests.route(testResource(otherKey, "apps/v1", nil), nil, nil)

		assert.Empty(t, received(channels["a"]))
	})
}

func TestResourceInterests_PopulateResourceInfo(t *testing.T) {
	t.Run("Keeps the manifest of a resource a target is interested in", func(t *testing.T) {
		interests := newResourceInterests(t.Context(), discardLogger())
		require.NoError(t, interests.set(t.Context(), "a", keys(deploymentKey)))

		info, cacheManifest := interests.populateResourceInfo(testUnstructured(deploymentKey, "apps/v1", nil), true)

		assert.True(t, cacheManifest)
		assert.Equal(t, deploymentKey, info.(ResourceInfo).ResourceKey)
	})

	t.Run("Doesn't keep the manifest of a root resource nobody is interested in", func(t *testing.T) {
		interests := newResourceInterests(t.Context(), discardLogger())
		require.NoError(t, interests.set(t.Context(), "a", keys(deploymentKey)))

		_, cacheManifest := interests.populateResourceInfo(testUnstructured(otherKey, "apps/v1", nil), true)

		assert.False(t, cacheManifest)
	})

	t.Run("Keeps the manifest of a child, and routes the child's own children to the same target", func(t *testing.T) {
		interests := newResourceInterests(t.Context(), discardLogger())
		channels := subscribeTestTargets(t, interests, "a", "b")
		require.NoError(t, interests.set(t.Context(), "a", keys(deploymentKey)))

		replicaSet := testUnstructured(replicaSetKey, "apps/v1", ownedBy("Deployment", "apps/v1", "web"))
		_, cacheManifest := interests.populateResourceInfo(replicaSet, false)
		require.True(t, cacheManifest)

		podKey := kube.NewResourceKey("", "Pod", "default", "web-123-abc")
		interests.route(testResource(podKey, "v1", ownedBy("ReplicaSet", "apps/v1", "web-123")), nil, nil)

		assert.Len(t, received(channels["a"]), 1)
		assert.Empty(t, received(channels["b"]))
	})
}

// Interests a target stops publishing, including those inherited while the cache listed the cluster, mustn't
// linger and keep routing changes or keeping manifests for the life of the process.
func TestResourceInterests_DoNotAccumulate(t *testing.T) {
	t.Run("A resource the target stops publishing is no longer routed or cached", func(t *testing.T) {
		interests := newResourceInterests(t.Context(), discardLogger())
		channels := subscribeTestTargets(t, interests, "a")
		require.NoError(t, interests.set(t.Context(), "a", keys(deploymentKey)))

		require.NoError(t, interests.set(t.Context(), "a", keys(otherKey)))

		interests.route(testResource(deploymentKey, "apps/v1", nil), nil, nil)
		assert.Empty(t, received(channels["a"]))
		_, cacheManifest := interests.populateResourceInfo(testUnstructured(deploymentKey, "apps/v1", nil), true)
		assert.False(t, cacheManifest)
	})

	t.Run("An inherited child is dropped when the target republishes without it", func(t *testing.T) {
		interests := newResourceInterests(t.Context(), discardLogger())
		channels := subscribeTestTargets(t, interests, "a")
		require.NoError(t, interests.set(t.Context(), "a", keys(deploymentKey)))
		replicaSet := testUnstructured(replicaSetKey, "apps/v1", ownedBy("Deployment", "apps/v1", "web"))
		_, inherited := interests.populateResourceInfo(replicaSet, false)
		require.True(t, inherited)

		// The target stops caring about the deployment, then the cache resyncs and lists everything again.
		require.NoError(t, interests.set(t.Context(), "a", keys()))
		_, cacheManifest := interests.populateResourceInfo(replicaSet, false)

		assert.False(t, cacheManifest)
		podKey := kube.NewResourceKey("", "Pod", "default", "web-123-abc")
		interests.route(testResource(podKey, "v1", ownedBy("ReplicaSet", "apps/v1", "web-123")), nil, nil)
		assert.Empty(t, received(channels["a"]))

		targets, resources := indexSizes(t, interests)
		assert.Zero(t, targets)
		assert.Zero(t, resources)
	})

	t.Run("Republishing and relisting the same resources doesn't grow the index", func(t *testing.T) {
		interests := newResourceInterests(t.Context(), discardLogger())
		replicaSet := testUnstructured(replicaSetKey, "apps/v1", ownedBy("Deployment", "apps/v1", "web"))

		for range 100 {
			require.NoError(t, interests.set(t.Context(), "a", keys(deploymentKey, otherKey)))
			require.NoError(t, interests.set(t.Context(), "b", keys(deploymentKey)))
			interests.populateResourceInfo(replicaSet, false)
		}

		targets, resources := indexSizes(t, interests)
		assert.Equal(t, 5, targets, "a: deployment, other and the replica set; b: deployment and the replica set")
		assert.Equal(t, 3, resources)
	})
}
