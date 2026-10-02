package cluster

import (
	"errors"
	"fmt"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache/mocks"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/stretchr/testify/mock"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery/fake"

	k8sfake "k8s.io/client-go/kubernetes/fake"
)

var assertSyncErr = errors.New("cluster cache sync failed")

// newSweepTestClusterList builds targets that share one mock cluster cache, so tests can count how many
// times a single sweep syncs it.
func newSweepTestClusterList(t *testing.T, mockCache *mocks.ClusterCache, targetCount, instanceCount int) *ClusterList {
	t.Helper()

	clientset := k8sfake.NewSimpleClientset()
	clusterList := NewClusterListFromConnection(t.Context(), discardLogger(), NoOpUpdater{}, ClusterConnection{
		Cache:     mockCache,
		Discovery: &MockCachedDiscoveryClient{FakeDiscovery: clientset.Discovery().(*fake.FakeDiscovery)},
	})

	for target := range targetCount {
		c, err := clusterList.EnsureCluster(t.Context(), ClusterId(fmt.Sprintf("cluster-%d", target)))
		if err != nil {
			t.Fatalf("EnsureCluster: %v", err)
		}
		for i := range instanceCount {
			instance := NewApplicationInstanceBuilder().
				WithApplicationInstanceId(string(rune('a' + i))).
				WithHashSalt("salt").
				Build()
			seedApplicationInstances(t, c, &instance)
		}
	}

	return clusterList
}

func TestApplicationInstanceUpdates_SyncsOncePerSweepAcrossTargets(t *testing.T) {
	mockCache := &mocks.ClusterCache{}
	mockCache.On("EnsureSynced").Return(nil).Once()
	mockCache.On("GetClusterInfo").Return(cache.ClusterInfo{Server: "test-server"})
	mockCache.On("GetAPIResources").Return([]kube.APIResourceInfo{})
	mockCache.On(
		"GetManagedLiveObjs",
		mock.AnythingOfType("[]*unstructured.Unstructured"),
		mock.AnythingOfType("func(*cache.Resource) bool"),
	).Return(map[kube.ResourceKey]*unstructured.Unstructured{}, nil)
	mockCache.On(
		"IterateHierarchyV2",
		mock.AnythingOfType("[]kube.ResourceKey"),
		mock.AnythingOfType("func(*cache.Resource, map[kube.ResourceKey]*cache.Resource) bool"),
	).Return()

	const targetCount, instanceCount = 2, 3
	clusterList := newSweepTestClusterList(t, mockCache, targetCount, instanceCount)

	swept := 0
	for range clusterList.ApplicationInstanceUpdates(t.Context()) {
		swept++
	}

	if swept != targetCount*instanceCount {
		t.Errorf("expected %d application instances to be swept, got %d", targetCount*instanceCount, swept)
	}

	mockCache.AssertExpectations(t)
	mockCache.AssertNumberOfCalls(t, "EnsureSynced", 1)
}

func TestApplicationInstanceUpdates_SkipsSweepWhenSyncFails(t *testing.T) {
	mockCache := &mocks.ClusterCache{}
	mockCache.On("EnsureSynced").Return(assertSyncErr).Once()
	mockCache.On("GetClusterInfo").Return(cache.ClusterInfo{Server: "test-server"})

	clusterList := newSweepTestClusterList(t, mockCache, 2, 3)

	swept := 0
	for range clusterList.ApplicationInstanceUpdates(t.Context()) {
		swept++
	}

	if swept != 0 {
		t.Errorf("expected no application instances to be swept after a sync failure, got %d", swept)
	}

	mockCache.AssertNumberOfCalls(t, "EnsureSynced", 1)
	mockCache.AssertNotCalled(t, "GetAPIResources")
	mockCache.AssertNotCalled(t, "GetManagedLiveObjs", mock.Anything, mock.Anything)
	mockCache.AssertNotCalled(t, "IterateHierarchyV2", mock.Anything, mock.Anything)
}
