package cluster

import (
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

	"github.com/octopusdeploy/kubernetes-monitor/internal/kubernetes"
)

var (
	// A custom type whose CRD has been deleted: absent from GetAPIResources().
	goneGK = schema.GroupKind{Group: "widgets.example.com", Kind: "Widget"}
	// A built-in type that is still present in the cluster.
	presentDeployGK = schema.GroupKind{Group: "apps", Kind: "Deployment"}
)

func newCrdTestCluster(mockCache Cache) *Cluster {
	return &Cluster{
		ClusterId:            "cluster-id",
		logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		clusterCache:         mockCache,
		ApplicationInstances: NewApplicationInstanceList(),
	}
}

// mockCacheWithAPIResources returns a cache whose discovered API resource list is
// exactly apiResources. GetManagedLiveObjs asserts (via MatchedBy) that no object of
// a vanished GVK is ever passed to it — the code must exclude those from targetObjects.
func mockCacheWithAPIResources(apiResources []kube.APIResourceInfo, forbiddenKinds []string) *mocks.ClusterCache {
	m := &mocks.ClusterCache{}
	m.On("GetAPIResources").Return(apiResources)
	m.On(
		"GetManagedLiveObjs",
		mock.MatchedBy(func(objs []*unstructured.Unstructured) bool {
			for _, o := range objs {
				for _, k := range forbiddenKinds {
					if o.GetKind() == k {
						return false // a vanished-GVK object leaked into the live lookup
					}
				}
			}
			return true
		}),
		mock.AnythingOfType("func(*cache.Resource) bool"),
	).Return(map[kube.ResourceKey]*unstructured.Unstructured{}, nil).Once()
	m.On(
		"IterateHierarchyV2",
		mock.AnythingOfType("[]kube.ResourceKey"),
		mock.AnythingOfType("func(*cache.Resource, map[kube.ResourceKey]*cache.Resource) bool"),
	).Return()
	return m
}

func newWidgetDesired(id DesiredResourceId, name string, resolved bool) DesiredResource {
	b := NewDesiredResourceBuilder().
		WithId(id).
		WithName(name).
		WithNamespace("default").
		WithKind("Widget").
		WithApiVersion("widgets.example.com/v1").
		WithManifest(
			kubernetes.NewUnstructuredBuilder().
				WithAPIVersion("widgets.example.com/v1").
				WithKind("Widget").
				WithName(name).
				WithNamespace("default").
				Build(),
		)
	if !resolved {
		b = b.WithUnresolvedNamespace()
	}
	return b.Build()
}

// Path A: CRD gone AND namespace never resolved (e.g. after an agent restart).
// Must be reported Missing, not stranded Unknown.
func TestGetMonitoredResources_GvkGone_Unresolved_ReportedMissing(t *testing.T) {
	m := mockCacheWithAPIResources(
		[]kube.APIResourceInfo{{GroupKind: presentDeployGK, Meta: metav1.APIResource{Namespaced: true}}},
		[]string{"Widget"},
	)
	c := newCrdTestCluster(m)
	widget := newWidgetDesired("widget-id", "my-widget", false)
	desired := map[kube.ResourceKey]*DesiredResource{widget.ResourceKey(): &widget}

	_, _, missing, unknown, err := c.getMonitoredResources(desired, "salt", true)
	assert.NoError(t, err)
	assert.Contains(t, missing, widget.Id, "vanished-GVK resource should be Missing")
	assert.NotContains(t, unknown, widget.Id, "vanished-GVK resource should not be Unknown")
	m.AssertExpectations(t)
}

// Path B: CRD gone but the resource was resolved before deletion. Must be reported
// Missing AND must not be passed to GetManagedLiveObjs (which would hard-error and
// abort the whole snapshot). The MatchedBy in the mock enforces the exclusion.
func TestGetMonitoredResources_GvkGone_Resolved_ReportedMissing_NotSentToLiveLookup(t *testing.T) {
	m := mockCacheWithAPIResources(
		[]kube.APIResourceInfo{{GroupKind: presentDeployGK, Meta: metav1.APIResource{Namespaced: true}}},
		[]string{"Widget"},
	)
	c := newCrdTestCluster(m)
	widget := newWidgetDesired("widget-id", "my-widget", true)
	desired := map[kube.ResourceKey]*DesiredResource{widget.ResourceKey(): &widget}

	_, _, missing, unknown, err := c.getMonitoredResources(desired, "salt", true)
	assert.NoError(t, err)
	assert.Contains(t, missing, widget.Id, "vanished-GVK resolved resource should be Missing")
	assert.NotContains(t, unknown, widget.Id)
	m.AssertExpectations(t)
}

// Safety: the GVK is absent from discovery but discovery only partially succeeded this
// cycle (discoveryHealthy == false), so the absence is not conclusive — the type may still
// exist behind a group whose discovery transiently failed. Must stay Unknown, never Missing.
func TestGetMonitoredResources_GvkGone_DiscoveryUnhealthy_StaysUnknown(t *testing.T) {
	m := mockCacheWithAPIResources(
		[]kube.APIResourceInfo{{GroupKind: presentDeployGK, Meta: metav1.APIResource{Namespaced: true}}},
		[]string{"Widget"},
	)
	c := newCrdTestCluster(m)
	widget := newWidgetDesired("widget-id", "my-widget", true)
	desired := map[kube.ResourceKey]*DesiredResource{widget.ResourceKey(): &widget}

	_, _, missing, unknown, err := c.getMonitoredResources(desired, "salt", false)
	assert.NoError(t, err)
	assert.Contains(t, unknown, widget.Id, "absent GVK under unhealthy discovery should stay Unknown")
	assert.NotContains(t, missing, widget.Id, "absent GVK under unhealthy discovery must not be Missing")
	m.AssertExpectations(t)
}

// Safety: the GVK is still known to the cluster but the namespace hasn't resolved yet
// (transient / brand-new). Must stay Unknown — never flipped to Missing.
func TestGetMonitoredResources_GvkKnown_Unresolved_StaysUnknown(t *testing.T) {
	m := mockCacheWithAPIResources(
		[]kube.APIResourceInfo{{GroupKind: goneGK, Meta: metav1.APIResource{Namespaced: true}}},
		nil, // Widget's GVK IS in the API here, so it may be sent to the live lookup
	)
	c := newCrdTestCluster(m)
	widget := newWidgetDesired("widget-id", "my-widget", false)
	desired := map[kube.ResourceKey]*DesiredResource{widget.ResourceKey(): &widget}

	_, _, missing, unknown, err := c.getMonitoredResources(desired, "salt", true)
	assert.NoError(t, err)
	assert.Contains(t, unknown, widget.Id, "known-GVK but unresolved namespace should stay Unknown")
	assert.NotContains(t, missing, widget.Id)
	m.AssertExpectations(t)
}
