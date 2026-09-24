package cluster

import (
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/types"

	engineHealth "github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
)

type UnknownMonitoredResourceBuilder struct {
	clusterId         ClusterId
	desiredResourceId DesiredResourceId
	status            *engineHealth.HealthStatus
	syncStatus        *SyncStatus
}

func NewUnknownMonitoredResourceBuilder() *UnknownMonitoredResourceBuilder {
	return &UnknownMonitoredResourceBuilder{
		clusterId:         ClusterId("cluster-id"),
		desiredResourceId: DesiredResourceId(types.UID(uuid.NewString())),
		status:            &engineHealth.HealthStatus{Status: "Missing", Message: ""},
		syncStatus:        &SyncStatus{Status: "OutOfSync", Message: ""},
	}
}

func (b *UnknownMonitoredResourceBuilder) WithDesiredResourceId(
	value DesiredResourceId,
) *UnknownMonitoredResourceBuilder {
	b.desiredResourceId = value
	return b
}

func (b *UnknownMonitoredResourceBuilder) Build() UnknownMonitoredResource {
	return UnknownMonitoredResource{
		ClusterId:         b.clusterId,
		DesiredResourceId: b.desiredResourceId,
		Status:            b.status,
		SyncStatus:        b.syncStatus,
	}
}
