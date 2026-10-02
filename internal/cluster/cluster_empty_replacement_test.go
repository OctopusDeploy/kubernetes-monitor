package cluster

import (
	"context"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache/mocks"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakediscovery "k8s.io/client-go/discovery/fake"
	testcore "k8s.io/client-go/testing"
)

// deploymentGroupKind is the type the default DesiredResource builder produces. A synced cache has to
// know it, or the resource is read as "the type's CRD is gone" and skipped rather than reported.
var deploymentGroupKind = schema.GroupKind{Group: "apps", Kind: "Deployment"}

type recordingUpdater struct {
	updates      []*ApplicationInstanceChanges
	replacements []*ApplicationInstanceChanges
}

func (u *recordingUpdater) Update(_ context.Context, update *ApplicationInstanceChanges) {
	u.updates = append(u.updates, update)
}

func (u *recordingUpdater) Replace(_ context.Context, replacement *ApplicationInstanceChanges) {
	u.replacements = append(u.replacements, replacement)
}

func newReplacementTestConnection() ClusterConnection {
	mockCache := &mocks.ClusterCache{}
	mockCache.On("GetClusterInfo").Return(cache.ClusterInfo{Server: "test-server"})
	mockCache.On("EnsureSynced").Return(nil)
	mockCache.On("GetAPIResources").Return([]kube.APIResourceInfo{{GroupKind: deploymentGroupKind}})
	mockCache.On("IsNamespaced", deploymentGroupKind).Return(true, nil)
	mockCache.On("GetManagedLiveObjs", mock.Anything, mock.Anything).
		Return(map[kube.ResourceKey]*unstructured.Unstructured{}, nil)
	mockCache.On("IterateHierarchyV2", mock.Anything, mock.Anything)

	discoveryClient := &MockCachedDiscoveryClient{&fakediscovery.FakeDiscovery{
		Fake: &testcore.Fake{Resources: []*metav1.APIResourceList{{
			GroupVersion: "apps/v1",
			APIResources: []metav1.APIResource{{Name: "deployments", Namespaced: true, Kind: "Deployment"}},
		}}},
	}}

	return ClusterConnection{Cache: mockCache, Discovery: discoveryClient}
}

func countApplicationInstanceUpdates(t *testing.T, clusterList *ClusterList) int {
	t.Helper()
	count := 0
	for range clusterList.ApplicationInstanceUpdates(t.Context()) {
		count++
	}

	return count
}

func TestApplicationInstanceUpdates(t *testing.T) {
	t.Run("Reports an empty application instance on every sweep", func(t *testing.T) {
		clusterList := NewClusterListFromConnection(
			t.Context(), discardLogger(), NoOpUpdater{}, newReplacementTestConnection())
		testCluster, err := clusterList.EnsureCluster(t.Context(), testClusterId)
		require.NoError(t, err)

		applicationInstance := NewApplicationInstanceBuilder().
			WithDesiredResources([]*DesiredResource{}).
			Build()
		seedApplicationInstances(t, testCluster, &applicationInstance)

		assert.Equal(t, 1, countApplicationInstanceUpdates(t, clusterList),
			"Server has to be told the application instance monitors nothing")

		_, stillListed := storedApplicationInstance(t, testCluster, applicationInstance.ApplicationInstanceId)
		assert.True(t, stillListed,
			"an application instance reported as empty stays listed so Server can refill it")

		assert.Equal(t, 1, countApplicationInstanceUpdates(t, clusterList),
			"the empty instance stays in the sweep and is reported again")
	})
}

func TestReplaceDesiredResources(t *testing.T) {
	t.Run("Empty list for an unknown application instance reports it as empty", func(t *testing.T) {
		updater := &recordingUpdater{}
		testCluster := newTestCluster(t, newTestSharedCluster(t, newReplacementTestConnection()), updater)

		err := testCluster.ReplaceDesiredResources(
			t.Context(), "never-seen", map[kube.ResourceKey]*DesiredResource{}, "fake-salt")

		require.NoError(t, err)
		assert.Len(t, updater.replacements, 1,
			"Server has to be told the application instance monitors nothing")

		stored, created := storedApplicationInstance(t, testCluster, "never-seen")
		require.True(t, created,
			"the application instance has to be tracked so later replacements are diffed against it")
		assert.Empty(t, stored.GetDesiredResources())
	})

	t.Run("Empty list for a known application instance still clears it", func(t *testing.T) {
		updater := &recordingUpdater{}
		existing := NewDesiredResourceBuilder().Build()
		applicationInstance := NewApplicationInstanceBuilder().
			WithDesiredResources([]*DesiredResource{&existing}).
			Build()
		testCluster := newTestCluster(
			t, newTestSharedCluster(t, newReplacementTestConnection()), updater, &applicationInstance)

		err := testCluster.ReplaceDesiredResources(
			t.Context(), applicationInstance.ApplicationInstanceId,
			map[kube.ResourceKey]*DesiredResource{}, "fake-salt")

		require.NoError(t, err)
		assert.Len(t, updater.replacements, 1,
			"an instance Server has already been told about has to be told it is now empty")

		stored, found := storedApplicationInstance(t, testCluster, applicationInstance.ApplicationInstanceId)
		require.True(t, found)
		assert.Empty(t, stored.GetDesiredResources())
	})
}
