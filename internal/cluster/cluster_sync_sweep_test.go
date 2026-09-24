package cluster

import (
	"errors"
	"io"
	"log/slog"
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

// newSweepTestCluster builds a Cluster sharing one mock cluster cache
// this lets tests count how many times a single sweep syncs the cache
func newSweepTestCluster(t *testing.T, mockCache *mocks.ClusterCache, instanceCount int) *Cluster {
	t.Helper()

	appList := NewApplicationInstanceList()
	for i := range instanceCount {
		instance := NewApplicationInstanceBuilder().
			WithApplicationInstanceId(string(rune('a' + i))).
			WithHashSalt("salt").
			Build()
		appList.UpsertApplicationInstance(&instance)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	clientset := k8sfake.NewSimpleClientset()

	return &Cluster{
		ClusterId:             "cluster-id",
		ApplicationInstances:  appList,
		mutex:                 NewMutexWithLogging(logger, "Cluster"),
		logger:                logger,
		clusterCache:          mockCache,
		cachedDiscoveryClient: &MockCachedDiscoveryClient{FakeDiscovery: clientset.Discovery().(*fake.FakeDiscovery)},
	}
}

func TestGetApplicationInstanceUpdates_SyncsOncePerSweep(t *testing.T) {
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

	const instanceCount = 3
	c := newSweepTestCluster(t, mockCache, instanceCount)

	swept := 0
	for range c.GetApplicationInstanceUpdates(t.Context()) {
		swept++
	}

	if swept != instanceCount {
		t.Errorf("expected %d application instances to be swept, got %d", instanceCount, swept)
	}

	mockCache.AssertExpectations(t)
	mockCache.AssertNumberOfCalls(t, "EnsureSynced", 1)
}

func TestGetApplicationInstanceUpdates_SkipsSweepWhenSyncFails(t *testing.T) {
	mockCache := &mocks.ClusterCache{}
	mockCache.On("EnsureSynced").Return(assertSyncErr).Once()
	mockCache.On("GetClusterInfo").Return(cache.ClusterInfo{Server: "test-server"})

	c := newSweepTestCluster(t, mockCache, 3)

	swept := 0
	for range c.GetApplicationInstanceUpdates(t.Context()) {
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
