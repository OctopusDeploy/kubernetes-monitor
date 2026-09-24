package cluster

import (
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	corev1 "k8s.io/api/core/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
)

func TestNewPresentMonitoredResource_SyncStatus(t *testing.T) {
	tests := []struct {
		name       string
		isOrphan   bool
		drifted    bool
		wantStatus SyncStatusCode
		wantPatch  bool
	}{
		{name: "matching manifest is InSync", wantStatus: SyncStatusInSync},
		{name: "drifted manifest is OutOfSync", drifted: true, wantStatus: SyncStatusOutOfSync, wantPatch: true},
		{name: "orphan is Orphaned", isOrphan: true, wantStatus: SyncStatusOrphaned},
		// A drifted orphan is what proves the orphan check short-circuits the diff, rather than the diff
		// happening to come back clean
		{name: "drifted orphan is still Orphaned", isOrphan: true, drifted: true, wantStatus: SyncStatusOrphaned},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			syncStatus := presentSyncStatus(t, test.isOrphan, test.drifted)

			assert.Equal(t, test.wantStatus, syncStatus.Status)
			if test.wantPatch {
				assert.NotEmpty(t, syncStatus.JsonPatch)
			} else {
				assert.Empty(t, syncStatus.JsonPatch, "only a drifted non-orphan carries a diff")
			}
		})
	}
}

func TestNewMissingMonitoredResource_OrphanReportsOrphaned(t *testing.T) {
	desiredResource := NewDesiredResourceBuilder().WithOrphan().Build()

	resource := NewMissingMonitoredResource("cluster-id", &desiredResource)

	assert.Equal(t, SyncStatusOrphaned, resource.SyncStatus.Status)
}

func TestNewUnknownMonitoredResource_OrphanReportsOrphanedAndKeepsReason(t *testing.T) {
	desiredResource := NewDesiredResourceBuilder().WithOrphan().Build()

	resource := NewUnknownMonitoredResource("cluster-id", &desiredResource, "some reason")

	assert.Equal(t, SyncStatusOrphaned, resource.SyncStatus.Status)
	assert.Equal(t, "some reason", resource.SyncStatus.Message, "the reason a resource is unknown still holds")
}

// An orphan with no manifest at all must still report Orphaned rather than Unknown
func TestGetSyncStatus_OrphanWithoutManifestReportsOrphaned(t *testing.T) {
	desiredResource := NewDesiredResourceBuilder().WithNamespace("default").WithOrphan().Build()
	desiredResource.Manifest = nil

	syncStatus := getSyncStatus(nil, &desiredResource)

	assert.Equal(t, SyncStatusOrphaned, syncStatus.Status)
}

func liveResourceFor(desiredResource *DesiredResource) (*cache.Resource, *unstructured.Unstructured) {
	return &cache.Resource{
		Ref: corev1.ObjectReference{
			APIVersion: desiredResource.Details.ManifestType.APIVersion,
			Kind:       desiredResource.Details.ManifestType.Kind,
			Name:       desiredResource.Details.Name,
			Namespace:  desiredResource.AssumedNamespace,
			UID:        types.UID("uid-" + desiredResource.Details.Name),
		},
		Info: ResourceInfo{
			ResourceKey: desiredResource.ResourceKey(),
		},
		ResourceVersion: "1",
	}, desiredResource.Manifest.DeepCopy()
}

func presentSyncStatus(t *testing.T, isOrphan bool, drifted bool) *SyncStatus {
	t.Helper()

	builder := NewDesiredResourceBuilder().WithNamespace("default")
	if isOrphan {
		builder = builder.WithOrphan()
	}
	desiredResource := builder.Build()

	liveResource, liveManifest := liveResourceFor(&desiredResource)
	if drifted {
		unstructured.RemoveNestedField(liveManifest.Object, "spec")
	}

	resource, err := NewPresentMonitoredResource(
		"cluster-id",
		liveResource,
		liveManifest,
		&desiredResource,
		crypto.HashSalt("salt"),
	)
	if err != nil {
		t.Fatalf("NewPresentMonitoredResource: %v", err)
	}

	return resource.SyncStatus
}
