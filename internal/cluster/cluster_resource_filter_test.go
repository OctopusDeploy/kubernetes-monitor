package cluster

import (
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache/mocks"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
	"github.com/octopusdeploy/kubernetes-monitor/internal/kubernetes"
)

type spyResourceFilter struct {
	mu       sync.Mutex
	calls    []schema.GroupKind
	excluded map[schema.GroupKind]struct{}
}

func (s *spyResourceFilter) IsExcludedResource(group, kind, cluster string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	gk := schema.GroupKind{Group: group, Kind: kind}
	s.calls = append(s.calls, gk)
	_, excluded := s.excluded[gk]
	return excluded
}

func (s *spyResourceFilter) GetLabelSelector(group, kind, cluster string) string {
	return ""
}

func TestGetMonitoredResources_FiltersTargetObjectsViaResourceFilter(t *testing.T) {
	spy := &spyResourceFilter{
		excluded: map[schema.GroupKind]struct{}{
			{Group: "apps", Kind: "Deployment"}: {},
		},
	}

	mockCache := mocks.ClusterCache{}
	// Both types exist in the cluster; the Deployment is dropped by the resource filter,
	// not because its type is gone. GetAPIResources must list both so they are treated
	// as known (not as vanished CRDs).
	mockCache.On("GetAPIResources").Return([]kube.APIResourceInfo{
		{GroupKind: schema.GroupKind{Group: "", Kind: "Pod"}},
		{GroupKind: schema.GroupKind{Group: "apps", Kind: "Deployment"}},
	})
	mockCache.On("IsNamespaced", schema.GroupKind{Group: "", Kind: "Pod"}).Return(true, nil)
	mockCache.On(
		"GetManagedLiveObjs",
		mock.MatchedBy(func(targetObjs []*unstructured.Unstructured) bool {
			// Only the non-excluded Pod manifest should reach GetManagedLiveObjs;
			// the excluded apps/Deployment must be filtered out beforehand.
			if len(targetObjs) != 1 {
				return false
			}
			gvk := targetObjs[0].GroupVersionKind()
			return gvk.Group == "" && gvk.Kind == "Pod" && targetObjs[0].GetName() == "kept-pod"
		}),
		mock.AnythingOfType("func(*cache.Resource) bool"),
	).Return(map[kube.ResourceKey]*unstructured.Unstructured{}, nil).Once()
	mockCache.On(
		"IterateHierarchyV2",
		mock.AnythingOfType("[]kube.ResourceKey"),
		mock.AnythingOfType("func(*cache.Resource, map[kube.ResourceKey]*cache.Resource) bool"),
	).Return()

	c := &Cluster{
		ClusterId:            "cluster-id",
		logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		clusterCache:         &mockCache,
		resourceFilter:       spy,
		clusterServer:        "test-server",
		ApplicationInstances: NewApplicationInstanceList(),
	}

	keptPod := NewDesiredResourceBuilder().
		WithId("kept-pod-id").
		WithName("kept-pod").
		WithNamespace("default").
		WithApiVersion("v1").
		WithKind("Pod").
		WithManifest(
			kubernetes.NewUnstructuredBuilder().
				WithAPIVersion("v1").
				WithKind("Pod").
				WithName("kept-pod").
				WithNamespace("default").
				Build(),
		).
		Build()

	droppedDeployment := NewDesiredResourceBuilder().
		WithId("dropped-deploy-id").
		WithName("dropped-deploy").
		WithNamespace("default").
		WithApiVersion("apps/v1").
		WithKind("Deployment").
		WithManifest(
			kubernetes.NewUnstructuredBuilder().
				WithAPIVersion("apps/v1").
				WithKind("Deployment").
				WithName("dropped-deploy").
				WithNamespace("default").
				Build(),
		).
		Build()

	desiredResources := map[kube.ResourceKey]*DesiredResource{
		keptPod.ResourceKey():           &keptPod,
		droppedDeployment.ResourceKey(): &droppedDeployment,
	}

	_, _, _, _, err := c.getMonitoredResources(desiredResources, crypto.HashSalt("salt"), true)
	if !assert.NoError(t, err) {
		return
	}

	mockCache.AssertExpectations(t)

	assert.Contains(t, spy.calls, schema.GroupKind{Group: "apps", Kind: "Deployment"},
		"IsExcludedResource should have been invoked for the Deployment manifest")
	assert.Contains(t, spy.calls, schema.GroupKind{Group: "", Kind: "Pod"},
		"IsExcludedResource should have been invoked for the Pod manifest")
}
