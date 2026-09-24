package protos

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	engineHealth "github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

const testJsonPatch = `[{"op":"replace","path":"/spec/replicas","value":2}]`

func TestToReplaceMonitoredResourceRequest(t *testing.T) {
	t.Run("Empty changes still produces request", func(t *testing.T) {
		// An empty replacement is how Server learns an application instance monitors nothing
		request, errs := ToReplaceMonitoredResourceRequest(cluster.ApplicationInstanceChanges{
			ApplicationInstanceId: "application-instance",
			ClusterId:             "cluster",
		})

		assert.Empty(t, errs)
		assert.NotNil(t, request)
		assert.Equal(t, "application-instance", request.ApplicationInstanceId.Value)
		assert.Empty(t, request.PresentMonitoredResources)
		assert.Empty(t, request.ChildMonitoredResources)
		assert.Empty(t, request.MissingMonitoredResources)
		assert.Empty(t, request.UnknownMonitoredResources)
	})

	t.Run("Partial failure produces partial result and errors", func(t *testing.T) {
		convertible := presentWithoutManifest()
		convertible.Manifest = (*cluster.SanitizedManifest)(&unstructured.Unstructured{
			Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment"},
		})
		failing := presentWithoutManifest()

		request, errs := ToReplaceMonitoredResourceRequest(cluster.ApplicationInstanceChanges{
			ApplicationInstanceId:     "application-instance",
			ClusterId:                 "cluster",
			PresentMonitoredResources: []*cluster.PresentMonitoredResource{convertible, failing},
		})

		assert.NotEmpty(t, errs)
		assert.NotNil(t, request)
		assert.Len(t, request.PresentMonitoredResources, 1)
	})
}

func TestToUpdateMonitoredResourceRequest(t *testing.T) {
	t.Run("Empty changes produces nothing", func(t *testing.T) {
		request, deleteRequest, errs := ToUpdateMonitoredResourceRequest(
			context.TODO(),
			cluster.ApplicationInstanceChanges{
				ApplicationInstanceId: "application-instance",
				ClusterId:             "cluster",
			},
		)

		assert.Empty(t, errs)
		assert.Nil(t, request)
		assert.Nil(t, deleteRequest)
	})
}

func TestToPresentMonitoredResource_NilManifest_ReturnsError(t *testing.T) {
	in := presentWithoutManifest()

	out, err := toPresentMonitoredResource(in)

	assert.Nil(t, out)
	assert.ErrorContains(t, err, "Manifest is nil")
}

func TestToChildMonitoredResource_NilManifest_ReturnsError(t *testing.T) {
	in := childWithoutManifest()

	out, err := toChildMonitoredResource(in)

	assert.Nil(t, out)
	assert.ErrorContains(t, err, "Manifest is nil")
}

func TestToChildMonitoredResource_NonNilManifest_Succeeds(t *testing.T) {
	in := childWithoutManifest()
	in.Manifest = sanitizedManifest("ReplicaSet")

	out, err := toChildMonitoredResource(in)

	assert.NoError(t, err)
	assert.NotNil(t, out)
}

// The deprecated status code cannot be orphaned
func TestToSyncStatus(t *testing.T) {
	tests := []struct {
		status         cluster.SyncStatusCode
		wantDeprecated SyncStatusCode
		wantCurrent    SyncStatusCode
	}{
		{cluster.SyncStatusUnknown, SyncStatusCode_SYNC_STATUS_CODE_UNKNOWN, SyncStatusCode_SYNC_STATUS_CODE_UNKNOWN},
		{cluster.SyncStatusInSync, SyncStatusCode_SYNC_STATUS_CODE_INSYNC, SyncStatusCode_SYNC_STATUS_CODE_INSYNC},
		{
			cluster.SyncStatusOutOfSync,
			SyncStatusCode_SYNC_STATUS_CODE_OUTOFSYNC,
			SyncStatusCode_SYNC_STATUS_CODE_OUTOFSYNC,
		},
		{
			cluster.SyncStatusNotApplicable,
			SyncStatusCode_SYNC_STATUS_CODE_NOTAPPLICABLE,
			SyncStatusCode_SYNC_STATUS_CODE_NOTAPPLICABLE,
		},
		{
			cluster.SyncStatusOrphaned,
			SyncStatusCode_SYNC_STATUS_CODE_NOTAPPLICABLE,
			SyncStatusCode_SYNC_STATUS_CODE_ORPHANED,
		},
	}

	for _, test := range tests {
		t.Run(string(test.status), func(t *testing.T) {
			out := ToSyncStatus(cluster.SyncStatus{Status: test.status})

			assert.Equal(t, test.wantDeprecated, out.GetSyncStatusCode())
			assert.Equal(t, test.wantCurrent, out.GetSyncStatusCodeV2())
		})
	}
}

func TestToSyncStatus_CopiesMessageAndPatchThrough(t *testing.T) {
	out := ToSyncStatus(cluster.SyncStatus{
		Status:    cluster.SyncStatusOrphaned,
		Message:   "some reason",
		JsonPatch: testJsonPatch,
	})

	assert.Equal(t, "some reason", out.GetMessage())
	assert.Equal(t, testJsonPatch, out.GetJsonPatch())
}

func TestToPresentMonitoredResource_CarriesSyncStatus(t *testing.T) {
	in := presentWithoutManifest()
	in.Manifest = sanitizedManifest("Deployment")
	in.SyncStatus = &cluster.SyncStatus{Status: cluster.SyncStatusOrphaned}

	out, err := toPresentMonitoredResource(in)

	assert.NoError(t, err)
	assert.Equal(t, SyncStatusCode_SYNC_STATUS_CODE_ORPHANED, out.GetResourceSyncStatus().GetSyncStatusCodeV2())
}

func TestToMissingResource_CarriesSyncStatus(t *testing.T) {
	in := missingResource()
	in.SyncStatus = &cluster.SyncStatus{Status: cluster.SyncStatusOrphaned}

	out := toMissingResource(in)

	assert.Equal(t, SyncStatusCode_SYNC_STATUS_CODE_ORPHANED, out.GetResourceSyncStatus().GetSyncStatusCodeV2())
}

func TestToUnknownResource_CarriesSyncStatus(t *testing.T) {
	in := unknownResource()
	in.SyncStatus = &cluster.SyncStatus{Status: cluster.SyncStatusOrphaned}

	out := toUnknownResource(in)

	assert.Equal(t, SyncStatusCode_SYNC_STATUS_CODE_ORPHANED, out.GetResourceSyncStatus().GetSyncStatusCodeV2())
}

func sanitizedManifest(kind string) *cluster.SanitizedManifest {
	return (*cluster.SanitizedManifest)(&unstructured.Unstructured{
		Object: map[string]any{"apiVersion": "apps/v1", "kind": kind},
	})
}

func presentWithoutManifest() *cluster.PresentMonitoredResource {
	return &cluster.PresentMonitoredResource{
		ClusterId:         "cluster",
		ResourceId:        "uid-1",
		DesiredResourceId: "desired-1",
		Status:            &engineHealth.HealthStatus{Status: engineHealth.HealthStatusHealthy},
		SyncStatus:        &cluster.SyncStatus{Status: cluster.SyncStatusInSync},
		GroupVersionKind:  &schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		Name:              "dep",
		Namespace:         "ns",
		ResourceVersion:   "1",
		Manifest:          nil,
	}
}

func childWithoutManifest() *cluster.ChildMonitoredResource {
	return &cluster.ChildMonitoredResource{
		ClusterId:        "cluster",
		ResourceId:       "uid-1",
		OwnerId:          "owner-1",
		RootOwnerId:      "root-1",
		Status:           &engineHealth.HealthStatus{Status: engineHealth.HealthStatusHealthy},
		GroupVersionKind: &schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "ReplicaSet"},
		Name:             "rs",
		Namespace:        "ns",
		ResourceVersion:  "1",
		Manifest:         nil,
	}
}

func missingResource() *cluster.MissingMonitoredResource {
	return &cluster.MissingMonitoredResource{
		ClusterId:         "cluster",
		DesiredResourceId: "desired-1",
		Status:            &engineHealth.HealthStatus{Status: engineHealth.HealthStatusMissing},
		SyncStatus:        &cluster.SyncStatus{Status: cluster.SyncStatusOutOfSync},
		GroupVersionKind:  &schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		Name:              "dep",
		Namespace:         "ns",
	}
}

func unknownResource() *cluster.UnknownMonitoredResource {
	return &cluster.UnknownMonitoredResource{
		ClusterId:         "cluster",
		DesiredResourceId: "desired-1",
		Status:            &engineHealth.HealthStatus{Status: engineHealth.HealthStatusUnknown},
		SyncStatus:        &cluster.SyncStatus{Status: cluster.SyncStatusUnknown},
	}
}
