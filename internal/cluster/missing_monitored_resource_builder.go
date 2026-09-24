package cluster

import (
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	engineHealth "github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
)

type MissingMonitoredResourceBuilder struct {
	clusterId         ClusterId
	resourceId        types.UID
	desiredResourceId DesiredResourceId
	status            *engineHealth.HealthStatus
	manifest          *SanitizedManifest
	syncStatus        *SyncStatus
	groupVersionKind  *schema.GroupVersionKind
	name              string
	namespace         string
	resourceVersion   string
}

func NewMissingMonitoredResourceBuilder() *MissingMonitoredResourceBuilder {
	return &MissingMonitoredResourceBuilder{
		clusterId:         ClusterId("cluster-id"),
		resourceId:        types.UID(uuid.NewString()),
		desiredResourceId: DesiredResourceId(types.UID(uuid.NewString())),
		status:            &engineHealth.HealthStatus{Status: "Missing", Message: ""},
		manifest:          nil,
		syncStatus:        &SyncStatus{Status: "OutOfSync", Message: ""},
		groupVersionKind:  &schema.GroupVersionKind{},
		name:              "present-resource",
		namespace:         "present-resource-ns",
		resourceVersion:   "1",
	}
}

func (b *MissingMonitoredResourceBuilder) WithDesiredResourceId(
	value DesiredResourceId,
) *MissingMonitoredResourceBuilder {
	b.desiredResourceId = value
	return b
}

func (b *MissingMonitoredResourceBuilder) WithName(value string) *MissingMonitoredResourceBuilder {
	b.name = value
	return b
}

func (b *MissingMonitoredResourceBuilder) WithNamespace(value string) *MissingMonitoredResourceBuilder {
	b.namespace = value
	return b
}

func (b *MissingMonitoredResourceBuilder) WithGroup(value string) *MissingMonitoredResourceBuilder {
	b.groupVersionKind.Group = value
	return b
}

func (b *MissingMonitoredResourceBuilder) WithKind(value string) *MissingMonitoredResourceBuilder {
	b.groupVersionKind.Kind = value
	return b
}

func (b *MissingMonitoredResourceBuilder) WithVersion(value string) *MissingMonitoredResourceBuilder {
	b.groupVersionKind.Version = value
	return b
}

func (b *MissingMonitoredResourceBuilder) Build() MissingMonitoredResource {
	return MissingMonitoredResource{
		ClusterId:         b.clusterId,
		DesiredResourceId: b.desiredResourceId,
		Status:            b.status,
		SyncStatus:        b.syncStatus,
		GroupVersionKind:  b.groupVersionKind,
		Name:              b.name,
		Namespace:         b.namespace,
	}
}
