package cluster

import (
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"k8s.io/apimachinery/pkg/runtime/schema"

	engineHealth "github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
)

type MissingMonitoredResource struct {
	ClusterId
	DesiredResourceId DesiredResourceId
	Status            *engineHealth.HealthStatus
	SyncStatus        *SyncStatus
	GroupVersionKind  *schema.GroupVersionKind
	Name              string
	Namespace         string
}

func (r *MissingMonitoredResource) ResourceKey() kube.ResourceKey {
	return kube.ResourceKey{
		Group:     r.GroupVersionKind.Group,
		Kind:      r.GroupVersionKind.Kind,
		Namespace: r.Namespace,
		Name:      r.Name,
	}
}

func NewMissingMonitoredResource(clusterId ClusterId, desiredResource *DesiredResource) *MissingMonitoredResource {
	gvk := desiredResource.Details.ManifestType.GroupVersionKind()
	syncStatus := NewSyncStatus(desiredResource, SyncStatusOutOfSync, "", "")

	return &MissingMonitoredResource{
		ClusterId:         clusterId,
		DesiredResourceId: desiredResource.Id,
		Status:            &engineHealth.HealthStatus{Status: engineHealth.HealthStatusMissing},
		SyncStatus:        &syncStatus,
		GroupVersionKind:  &gvk,
		Name:              desiredResource.Details.Name,
		Namespace:         desiredResource.Details.Namespace,
	}
}
