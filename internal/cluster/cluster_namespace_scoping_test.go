package cluster

import (
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache/mocks"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
	"github.com/octopusdeploy/kubernetes-monitor/internal/kubernetes"
)

func TestGetMonitoredResources_SkipsClusterScopedInNamespaceScopedMode(t *testing.T) {
	mockCache := mocks.ClusterCache{}
	// Both types exist in the cluster; only their scope/namespace differs. GetAPIResources
	// must include them so they are treated as known (not as vanished CRDs).
	mockCache.On("GetAPIResources").Return([]kube.APIResourceInfo{
		{GroupKind: schema.GroupKind{Group: "apps", Kind: "Deployment"}, Meta: metav1.APIResource{Namespaced: true}},
		{
			GroupKind: schema.GroupKind{Group: "rbac.authorization.k8s.io", Kind: "ClusterRole"},
			Meta:      metav1.APIResource{Namespaced: false},
		},
	})
	mockCache.On("IsNamespaced", schema.GroupKind{Group: "apps", Kind: "Deployment"}).Return(true, nil)
	mockCache.On(
		"GetManagedLiveObjs",
		mock.MatchedBy(func(targetObjs []*unstructured.Unstructured) bool {
			// Only the namespaced in-scope resource should be passed — not the cluster-scoped one
			return len(targetObjs) == 1 &&
				targetObjs[0].GetName() == "in-scope-deploy" &&
				targetObjs[0].GetNamespace() == "watched"
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
		namespaceScopedMode:  true,
		targetNamespaces:     map[string]struct{}{"watched": {}},
		ApplicationInstances: NewApplicationInstanceList(),
	}

	namespacedDesired := NewDesiredResourceBuilder().
		WithId("namespaced-id").
		WithName("in-scope-deploy").
		WithNamespace("watched").
		WithManifest(
			kubernetes.NewUnstructuredBuilder().
				WithAPIVersion("apps/v1").
				WithKind("Deployment").
				WithName("in-scope-deploy").
				WithNamespace("watched").
				Build(),
		).
		Build()

	clusterScopedDesired := NewDesiredResourceBuilder().
		WithId("cluster-scoped-id").
		WithName("my-clusterrole").
		WithKind("ClusterRole").
		WithApiVersion("rbac.authorization.k8s.io/v1").
		WithClusterScoped().
		WithManifest(
			kubernetes.NewUnstructuredBuilder().
				WithAPIVersion("rbac.authorization.k8s.io/v1").
				WithKind("ClusterRole").
				WithName("my-clusterrole").
				Build(),
		).
		Build()

	desiredResources := map[kube.ResourceKey]*DesiredResource{
		namespacedDesired.ResourceKey():    &namespacedDesired,
		clusterScopedDesired.ResourceKey(): &clusterScopedDesired,
	}

	_, _, missing, unknown, err := c.getMonitoredResources(desiredResources, crypto.HashSalt("salt"), true)
	if !assert.NoError(t, err) {
		return
	}

	assert.Contains(t, unknown, clusterScopedDesired.Id, "cluster-scoped resource should be marked unknown")
	assert.NotContains(t, unknown, namespacedDesired.Id, "namespaced resource should not be unknown")
	assert.Contains(t, missing, namespacedDesired.Id, "namespaced resource should be in missing")
	mockCache.AssertExpectations(t)
}

func TestGetMonitoredResources_MarksForbiddenResourcesAsUnknown(t *testing.T) {
	forbiddenGK := schema.GroupKind{Group: "eventing.keda.sh", Kind: "CloudEventSource"}

	mockCache := mocks.ClusterCache{}
	// GetAPIResources returns all discovered APIs including the forbidden one
	mockCache.On("GetAPIResources").Return([]kube.APIResourceInfo{
		{
			GroupKind:            schema.GroupKind{Group: "apps", Kind: "Deployment"},
			Meta:                 metav1.APIResource{Namespaced: true},
			GroupVersionResource: schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
		},
		{
			GroupKind: forbiddenGK,
			Meta:      metav1.APIResource{Namespaced: true},
			GroupVersionResource: schema.GroupVersionResource{
				Group:    "eventing.keda.sh",
				Version:  "v1alpha1",
				Resource: "cloudeventsources",
			},
		},
	})
	// IsNamespaced succeeds for Deployment (tracked by cache) but fails for the forbidden GK
	mockCache.On("IsNamespaced", schema.GroupKind{Group: "apps", Kind: "Deployment"}).Return(true, nil)
	mockCache.On("IsNamespaced", forbiddenGK).Return(false, fmt.Errorf("not found"))
	mockCache.On(
		"GetManagedLiveObjs",
		mock.AnythingOfType("[]*unstructured.Unstructured"),
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
		namespaceScopedMode:  true,
		targetNamespaces:     map[string]struct{}{"watched": {}},
		ApplicationInstances: NewApplicationInstanceList(),
	}

	permittedDesired := NewDesiredResourceBuilder().
		WithId("permitted-id").
		WithName("my-deploy").
		WithNamespace("watched").
		WithManifest(
			kubernetes.NewUnstructuredBuilder().
				WithAPIVersion("apps/v1").
				WithKind("Deployment").
				WithName("my-deploy").
				WithNamespace("watched").
				Build(),
		).
		Build()

	forbiddenDesired := NewDesiredResourceBuilder().
		WithId("forbidden-id").
		WithName("my-cloudeventsource").
		WithNamespace("watched").
		WithKind("CloudEventSource").
		WithApiVersion("eventing.keda.sh/v1alpha1").
		WithManifest(
			kubernetes.NewUnstructuredBuilder().
				WithAPIVersion("eventing.keda.sh/v1alpha1").
				WithKind("CloudEventSource").
				WithName("my-cloudeventsource").
				WithNamespace("watched").
				Build(),
		).
		Build()

	desiredResources := map[kube.ResourceKey]*DesiredResource{
		permittedDesired.ResourceKey(): &permittedDesired,
		forbiddenDesired.ResourceKey(): &forbiddenDesired,
	}

	_, _, missing, unknown, err := c.getMonitoredResources(desiredResources, crypto.HashSalt("salt"), true)
	if !assert.NoError(t, err) {
		return
	}

	assert.Contains(t, unknown, forbiddenDesired.Id, "forbidden resource should be marked unknown")
	assert.NotContains(t, missing, forbiddenDesired.Id, "forbidden resource should not be in missing")
	assert.Contains(
		t,
		missing,
		permittedDesired.Id,
		"permitted resource should be in missing (not found in live cluster)",
	)
	assert.NotContains(t, unknown, permittedDesired.Id, "permitted resource should not be unknown")
	mockCache.AssertExpectations(t)
}

func TestGetMonitoredResources_SkipsOutOfScopeNamespacesBeforeGetManagedLiveObjs(t *testing.T) {
	tests := []struct {
		name                string
		namespaceScopedMode bool
	}{
		{
			name:                "namespace scoped mode",
			namespaceScopedMode: true,
		},
		{
			name:                "cluster-scoped resources enabled",
			namespaceScopedMode: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCache := mocks.ClusterCache{}
			// The Deployment type exists in the cluster (only the out-of-scope resource's
			// namespace is unmanaged), so GetAPIResources must include it — otherwise it
			// would be treated as a vanished CRD.
			mockCache.On("GetAPIResources").Return([]kube.APIResourceInfo{
				{
					GroupKind: schema.GroupKind{Group: "apps", Kind: "Deployment"},
					Meta:      metav1.APIResource{Namespaced: true},
				},
			})
			mockCache.On("IsNamespaced", schema.GroupKind{Group: "apps", Kind: "Deployment"}).Return(true, nil)
			mockCache.On(
				"GetManagedLiveObjs",
				mock.MatchedBy(func(targetObjs []*unstructured.Unstructured) bool {
					return len(targetObjs) == 1 &&
						targetObjs[0].GetName() == "in-scope" &&
						targetObjs[0].GetNamespace() == "watched"
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
				namespaceScopedMode:  tt.namespaceScopedMode,
				targetNamespaces:     map[string]struct{}{"watched": {}},
				ApplicationInstances: NewApplicationInstanceList(),
			}

			inScopeDesired := NewDesiredResourceBuilder().
				WithId("in-scope-id").
				WithName("in-scope").
				WithNamespace("watched").
				WithManifest(
					kubernetes.NewUnstructuredBuilder().
						WithAPIVersion("apps/v1").
						WithKind("Deployment").
						WithName("in-scope").
						WithNamespace("watched").
						Build(),
				).
				Build()

			outOfScopeDesired := NewDesiredResourceBuilder().
				WithId("out-of-scope-id").
				WithName("out-of-scope").
				WithNamespace("watched").
				WithManifest(
					kubernetes.NewUnstructuredBuilder().
						WithAPIVersion("apps/v1").
						WithKind("Deployment").
						WithName("out-of-scope").
						WithNamespace("unmanaged").
						Build(),
				).
				Build()

			desiredResources := map[kube.ResourceKey]*DesiredResource{
				inScopeDesired.ResourceKey():    &inScopeDesired,
				outOfScopeDesired.ResourceKey(): &outOfScopeDesired,
			}

			_, _, missing, unknown, err := c.getMonitoredResources(desiredResources, crypto.HashSalt("salt"), true)
			if !assert.NoError(t, err) {
				return
			}

			assert.Contains(t, unknown, outOfScopeDesired.Id, "out-of-scope resource should be marked unknown")
			assert.NotContains(t, unknown, inScopeDesired.Id, "in-scope resource should not be unknown")
			assert.Contains(
				t,
				missing,
				inScopeDesired.Id,
				"in-scope resource should be in missing (not found in live cluster)",
			)
			mockCache.AssertExpectations(t)
		})
	}
}
