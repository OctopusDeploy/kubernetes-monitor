package cluster

import (
	"context"
	"io"
	"log/slog"
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

func newEmptyReplacementTestCluster() *Cluster {
	return newReplacementTestCluster(nil, nil)
}

func newReplacementTestCluster(syncError error, updater MonitoredResourcesUpdater) *Cluster {
	mockCache := &mocks.ClusterCache{}
	mockCache.On("GetClusterInfo").Return(cache.ClusterInfo{Server: "test-server"})
	mockCache.On("EnsureSynced").Return(syncError)
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

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	return &Cluster{
		ClusterId:                    "cluster-id",
		logger:                       logger,
		mutex:                        NewMutexWithLogging(logger, "Cluster"),
		clusterCache:                 mockCache,
		cachedDiscoveryClient:        discoveryClient,
		ApplicationInstances:         NewApplicationInstanceList(),
		updateMonitoredResourcesFunc: updater,
	}
}

func countApplicationInstanceUpdates(c *Cluster) int {
	count := 0
	for range c.GetApplicationInstanceUpdates(context.TODO()) {
		count++
	}

	return count
}

func TestGetApplicationInstanceUpdates(t *testing.T) {
	t.Run("Reports an empty application instance on every sweep", func(t *testing.T) {
		testCluster := newEmptyReplacementTestCluster()

		applicationInstance := NewApplicationInstanceBuilder().
			WithDesiredResources([]*DesiredResource{}).
			Build()
		testCluster.ApplicationInstances.UpsertApplicationInstance(&applicationInstance)

		assert.Equal(t, 1, countApplicationInstanceUpdates(testCluster),
			"Server has to be told the application instance monitors nothing")

		_, stillListed := testCluster.ApplicationInstances.Get(applicationInstance.ApplicationInstanceId)
		assert.True(t, stillListed,
			"an application instance reported as empty stays listed so Server can refill it")

		assert.Equal(t, 1, countApplicationInstanceUpdates(testCluster),
			"the empty instance stays in the sweep and is reported again")
	})
}

func TestReplaceDesiredResources(t *testing.T) {
	t.Run("Empty list for an unknown application instance reports it as empty", func(t *testing.T) {
		updater := &recordingUpdater{}
		testCluster := newReplacementTestCluster(nil, updater)

		err := testCluster.ReplaceDesiredResources(
			context.TODO(), "never-seen", map[kube.ResourceKey]*DesiredResource{}, "fake-salt")

		require.NoError(t, err)
		assert.Len(t, updater.replacements, 1,
			"Server has to be told the application instance monitors nothing")

		stored, created := testCluster.ApplicationInstances.Get("never-seen")
		require.True(t, created,
			"the application instance has to be tracked so later replacements are diffed against it")
		assert.Empty(t, stored.GetDesiredResources())
	})

	t.Run("Empty list for a known application instance still clears it", func(t *testing.T) {
		updater := &recordingUpdater{}
		testCluster := newReplacementTestCluster(nil, updater)

		existing := NewDesiredResourceBuilder().Build()
		applicationInstance := NewApplicationInstanceBuilder().
			WithDesiredResources([]*DesiredResource{&existing}).
			Build()
		testCluster.ApplicationInstances.UpsertApplicationInstance(&applicationInstance)

		err := testCluster.ReplaceDesiredResources(
			context.TODO(), applicationInstance.ApplicationInstanceId,
			map[kube.ResourceKey]*DesiredResource{}, "fake-salt")

		require.NoError(t, err)
		assert.Len(t, updater.replacements, 1,
			"an instance Server has already been told about has to be told it is now empty")

		stored, found := testCluster.ApplicationInstances.Get(applicationInstance.ApplicationInstanceId)
		require.True(t, found)
		assert.Empty(t, stored.GetDesiredResources())
	})
}
