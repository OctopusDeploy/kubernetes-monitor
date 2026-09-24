package cluster

import (
	engineHealth "github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
)

type UnknownMonitoredResource struct {
	ClusterId
	DesiredResourceId DesiredResourceId
	Status            *engineHealth.HealthStatus
	SyncStatus        *SyncStatus
}

func NewUnknownMonitoredResource(
	clusterId ClusterId,
	desiredResource *DesiredResource,
	message string,
) *UnknownMonitoredResource {
	syncStatus := NewSyncStatus(desiredResource, SyncStatusUnknown, message, "")

	return &UnknownMonitoredResource{
		ClusterId:         clusterId,
		DesiredResourceId: desiredResource.Id,
		Status:            &engineHealth.HealthStatus{Status: engineHealth.HealthStatusUnknown, Message: message},
		SyncStatus:        &syncStatus,
	}
}
