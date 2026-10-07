package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache/mocks"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	fakediscovery "k8s.io/client-go/discovery/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

type channelUpdater struct {
	updates      chan *ApplicationInstanceChanges
	replacements chan *ApplicationInstanceChanges
}

func newChannelUpdater() channelUpdater {
	return channelUpdater{
		updates:      make(chan *ApplicationInstanceChanges, 10),
		replacements: make(chan *ApplicationInstanceChanges, 10),
	}
}

func (u channelUpdater) Update(_ context.Context, update *ApplicationInstanceChanges) {
	u.updates <- update
}

func (u channelUpdater) Replace(_ context.Context, replacement *ApplicationInstanceChanges) {
	u.replacements <- replacement
}

func awaitChanges(t *testing.T, changes chan *ApplicationInstanceChanges) *ApplicationInstanceChanges {
	t.Helper()
	select {
	case c := <-changes:
		return c
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for changes to be sent")
		return nil
	}
}

func assertNoChanges(t *testing.T, changes chan *ApplicationInstanceChanges) {
	t.Helper()
	select {
	case c := <-changes:
		assert.Failf(t, "unexpected changes sent", "%+v", c)
	default:
	}
}

func waitForIdle(t *testing.T, c *Cluster) {
	t.Helper()
	require.NoError(t, do(t.Context(), c.mailbox, func(*clusterState) {}))
}

// newTargetsTestConnection is a cluster with no live objects, so a rescan reports every desired resource as
// missing.
func newTargetsTestConnection() ClusterConnection {
	deployments := schema.GroupKind{Group: "apps", Kind: "Deployment"}

	mockCache := &mocks.ClusterCache{}
	mockCache.On("GetClusterInfo").Return(cache.ClusterInfo{Server: "test-server"})
	mockCache.On("EnsureSynced").Return(nil)
	mockCache.On("GetAPIResources").Return([]kube.APIResourceInfo{{GroupKind: deployments}})
	mockCache.On("IsNamespaced", deployments).Return(true, nil)
	mockCache.On("GetManagedLiveObjs",
		mock.AnythingOfType("[]*unstructured.Unstructured"),
		mock.AnythingOfType("func(*cache.Resource) bool"),
	).Return(map[kube.ResourceKey]*unstructured.Unstructured{}, nil)
	mockCache.On("IterateHierarchyV2",
		mock.AnythingOfType("[]kube.ResourceKey"),
		mock.AnythingOfType("func(*cache.Resource, map[kube.ResourceKey]*cache.Resource) bool"),
	).Return()

	clientSet := k8sfake.NewClientset()
	return ClusterConnection{
		Cache:     mockCache,
		Discovery: &MockCachedDiscoveryClient{FakeDiscovery: clientSet.Discovery().(*fakediscovery.FakeDiscovery)},
		ClientSet: clientSet,
	}
}

func startTarget(
	t *testing.T, shared *sharedCluster, id ClusterId, updater MonitoredResourcesUpdater,
	applicationInstances ...*ApplicationInstance,
) *Cluster {
	t.Helper()
	c, err := newCluster(t.Context(), id, discardLogger(), shared, updater)
	require.NoError(t, err)
	seedApplicationInstances(t, c, applicationInstances...)
	return c
}

// monitoringDeployment is an application instance whose only desired resource is present in the cluster.
func monitoringDeployment(id string, desired DesiredResource) (*ApplicationInstance, *PresentMonitoredResource) {
	present := NewPresentMonitoredResourceBuilder().ForDesiredResource(desired).WithUID("deployment-uid").Build()
	applicationInstance := NewApplicationInstanceBuilder().
		WithApplicationInstanceId(id).
		WithHashSalt("salt").
		WithDesiredResources([]*DesiredResource{&desired}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&present}).
		Build()
	return &applicationInstance, &present
}

func updated(present *PresentMonitoredResource) resourceChange {
	return resourceChange{newRes: present.ToResource(true), oldRes: present.ToResource(false)}
}

func TestTargets_ShareResourceChanges(t *testing.T) {
	desired := NewDesiredResourceBuilder().WithName("web").WithNamespace("default").Build()
	shared := newTestSharedCluster(t, newTargetsTestConnection())

	firstApplicationInstance, present := monitoringDeployment("first-app", desired)
	firstUpdater := newChannelUpdater()
	first := startTarget(t, shared, "first-machine", firstUpdater, firstApplicationInstance)

	secondApplicationInstance, _ := monitoringDeployment("second-app", desired)
	secondUpdater := newChannelUpdater()
	startTarget(t, shared, "second-machine", secondUpdater, secondApplicationInstance)

	change := updated(present)
	shared.interests.route(change.newRes, change.oldRes, nil)

	firstUpdate := awaitChanges(t, firstUpdater.updates)
	assert.Equal(t, ClusterId("first-machine"), firstUpdate.ClusterId)
	assert.Equal(t, ApplicationInstanceId("first-app"), firstUpdate.ApplicationInstanceId)
	require.Len(t, firstUpdate.PresentMonitoredResources, 1)
	assert.Equal(t, desired.Id, firstUpdate.PresentMonitoredResources[0].DesiredResourceId)

	secondUpdate := awaitChanges(t, secondUpdater.updates)
	assert.Equal(t, ClusterId("second-machine"), secondUpdate.ClusterId)
	assert.Equal(t, ApplicationInstanceId("second-app"), secondUpdate.ApplicationInstanceId)

	t.Run("A target that stops monitoring the resource stops receiving its changes", func(t *testing.T) {
		require.NoError(t, first.ReplaceDesiredResources(t.Context(), "first-app", nil, "salt"))
		awaitChanges(t, firstUpdater.replacements)

		change := updated(present)
		shared.interests.route(change.newRes, change.oldRes, nil)

		awaitChanges(t, secondUpdater.updates)
		waitForIdle(t, first)
		assertNoChanges(t, firstUpdater.updates)
	})
}

// These run the change and the desired-state command through the target's mailbox in a fixed order, which
// pins down what happens when Octopus stops monitoring a resource while a change to it is in flight.
func TestTarget_OrdersResourceChangesAgainstDesiredState(t *testing.T) {
	desired := NewDesiredResourceBuilder().WithName("web").WithNamespace("default").Build()

	applyInOrder := func(t *testing.T, c *Cluster, change resourceChange) {
		t.Helper()
		require.NoError(t, do(t.Context(), c.mailbox, func(state *clusterState) {
			c.sendResourceChangeUpdates(t.Context(), state.applyResourceChange(t.Context(), change))
		}))
	}

	t.Run("A change that arrives after the resource stopped being desired sends nothing", func(t *testing.T) {
		shared := newTestSharedCluster(t, newTargetsTestConnection())
		applicationInstance, present := monitoringDeployment("app", desired)
		updater := newChannelUpdater()
		target := startTarget(t, shared, testClusterId, updater, applicationInstance)

		require.NoError(t, target.ReplaceDesiredResources(t.Context(), "app", nil, "salt"))
		applyInOrder(t, target, updated(present))

		replacement := awaitChanges(t, updater.replacements)
		assert.Zero(t, replacement.GetAllResourceCount())
		assertNoChanges(t, updater.updates)
	})

	t.Run("A change that arrives before the resource stopped being desired is sent, then replaced", func(t *testing.T) {
		shared := newTestSharedCluster(t, newTargetsTestConnection())
		applicationInstance, present := monitoringDeployment("app", desired)
		updater := newChannelUpdater()
		target := startTarget(t, shared, testClusterId, updater, applicationInstance)

		applyInOrder(t, target, updated(present))
		require.NoError(t, target.ReplaceDesiredResources(t.Context(), "app", nil, "salt"))

		update := awaitChanges(t, updater.updates)
		require.Len(t, update.PresentMonitoredResources, 1)
		assert.Equal(t, desired.Id, update.PresentMonitoredResources[0].DesiredResourceId)
		replacement := awaitChanges(t, updater.replacements)
		assert.Zero(t, replacement.GetAllResourceCount())
	})

	// The change queue folds a creation and deletion it hasn't delivered yet into a change with neither side.
	t.Run("A resource created and deleted before the target saw it sends nothing", func(t *testing.T) {
		shared := newTestSharedCluster(t, newTargetsTestConnection())
		missing := NewMissingMonitoredResource(testClusterId, &desired)
		applicationInstance := NewApplicationInstanceBuilder().
			WithApplicationInstanceId("app").
			WithDesiredResources([]*DesiredResource{&desired}).
			WithMissingMonitoredResources([]*MissingMonitoredResource{missing}).
			Build()
		updater := newChannelUpdater()
		target := startTarget(t, shared, testClusterId, updater, &applicationInstance)

		applyInOrder(t, target, resourceChange{})

		assertNoChanges(t, updater.updates)
	})
}
