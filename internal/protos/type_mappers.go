package protos

import (
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/types"

	engineHealth "github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

func (v *ApplicationInstanceId) FromProto() cluster.ApplicationInstanceId {
	return cluster.ApplicationInstanceId(v.GetValue())
}

func ToApplicationInstanceId(v cluster.ApplicationInstanceId) *ApplicationInstanceId {
	return &ApplicationInstanceId{Value: string(v)}
}

func (v *ClusterId) FromProto() cluster.ClusterId {
	return cluster.ClusterId(v.GetValue())
}

func ToClusterId(v cluster.ClusterId) *ClusterId {
	return &ClusterId{Value: string(v)}
}

func (v *Version) FromProto() cluster.Version {
	return cluster.Version(v.GetValue())
}

// FromProtoOrNil maps an optional Version, which is absent on commands that carry no deployment version.
func (v *Version) FromProtoOrNil() *cluster.Version {
	if v == nil {
		return nil
	}

	version := v.FromProto()

	return &version
}

func ToUUID(v types.UID) *UUID {
	return &UUID{Value: string(v)}
}

func ToUUIDs(v []types.UID) []*UUID {
	output := make([]*UUID, 0)
	for _, val := range v {
		output = append(output, &UUID{Value: string(val)})
	}
	return output
}

func ToResourceKeys(v []kube.ResourceKey) []*ResourceKey {
	output := make([]*ResourceKey, 0)
	for _, val := range v {
		output = append(output, ToResourceKey(val))
	}
	return output
}

func ToResourceKey(val kube.ResourceKey) *ResourceKey {
	return &ResourceKey{
		Name:                val.Name,
		KubernetesNamespace: val.Namespace,
		Group:               val.Group,
		Kind:                val.Kind,
	}
}

func (v *DesiredResourceId) FromProto() cluster.DesiredResourceId {
	return cluster.DesiredResourceId(types.UID(v.GetValue()))
}

func ToDesiredResourceIds(v []cluster.DesiredResourceId) []*DesiredResourceId {
	output := make([]*DesiredResourceId, 0)
	for _, val := range v {
		output = append(output, ToDesiredResourceId(val))
	}
	return output
}

func ToDesiredResourceId(v cluster.DesiredResourceId) *DesiredResourceId {
	return &DesiredResourceId{Value: string(v)}
}

func ToResourceStatus(resourceStatus *engineHealth.HealthStatus) *ResourceStatus {
	if resourceStatus == nil {
		return &ResourceStatus{
			ResourceStatusCode: ResourceStatusCode_RESOURCE_STATUS_CODE_NOTAPPLICABLE,
			Message:            "Status not applicable",
		}
	}

	var statusCode ResourceStatusCode
	switch resourceStatus.Status {
	case engineHealth.HealthStatusUnknown:
		statusCode = ResourceStatusCode_RESOURCE_STATUS_CODE_UNKNOWN
	case engineHealth.HealthStatusDegraded:
		statusCode = ResourceStatusCode_RESOURCE_STATUS_CODE_DEGRADED
	case engineHealth.HealthStatusMissing:
		statusCode = ResourceStatusCode_RESOURCE_STATUS_CODE_MISSING
	case engineHealth.HealthStatusSuspended:
		statusCode = ResourceStatusCode_RESOURCE_STATUS_CODE_SUSPENDED
	case engineHealth.HealthStatusProgressing:
		statusCode = ResourceStatusCode_RESOURCE_STATUS_CODE_PROGRESSING
	case engineHealth.HealthStatusHealthy:
		statusCode = ResourceStatusCode_RESOURCE_STATUS_CODE_HEALTHY
	}
	return &ResourceStatus{
		ResourceStatusCode: statusCode,
		Message:            resourceStatus.Message,
	}
}

func ToSyncStatus(syncStatus cluster.SyncStatus) *SyncStatus {
	var syncStatusCode SyncStatusCode
	switch syncStatus.Status {
	case cluster.SyncStatusUnknown:
		syncStatusCode = SyncStatusCode_SYNC_STATUS_CODE_UNKNOWN
	case cluster.SyncStatusInSync:
		syncStatusCode = SyncStatusCode_SYNC_STATUS_CODE_INSYNC
	case cluster.SyncStatusOutOfSync:
		syncStatusCode = SyncStatusCode_SYNC_STATUS_CODE_OUTOFSYNC
	case cluster.SyncStatusNotApplicable:
		syncStatusCode = SyncStatusCode_SYNC_STATUS_CODE_NOTAPPLICABLE
	case cluster.SyncStatusOrphaned:
		syncStatusCode = SyncStatusCode_SYNC_STATUS_CODE_ORPHANED
	default:
		panic("unknown sync status: " + string(syncStatusCode))
	}

	return &SyncStatus{
		SyncStatusCode:   MapToV1SyncStatusCode(syncStatusCode),
		SyncStatusCodeV2: syncStatusCode,
		Message:          syncStatus.Message,
		JsonPatch:        syncStatus.JsonPatch,
	}
}

func MapToV1SyncStatusCode(syncStatusCode SyncStatusCode) SyncStatusCode {
	if syncStatusCode == SyncStatusCode_SYNC_STATUS_CODE_ORPHANED {
		return SyncStatusCode_SYNC_STATUS_CODE_NOTAPPLICABLE
	}
	return syncStatusCode
}

func ToLogLine(logLine cluster.LogLine) *LogLine {
	return &LogLine{
		Message:   logLine.Message,
		Timestamp: timestamppb.New(logLine.Timestamp),
	}
}

func ToLogLines(logLines []cluster.LogLine) []*LogLine {
	output := make([]*LogLine, 0)
	for _, val := range logLines {
		output = append(output, ToLogLine(val))
	}
	return output
}

func ToEvent(event cluster.Event) *Event {
	parsedEvent := &Event{
		FirstObservedTime:   timestamppb.New(event.FirstObservedTime),
		LastObservedTime:    timestamppb.New(event.LastObservedTime),
		Count:               event.Count,
		Action:              event.Action,
		Reason:              event.Reason,
		Note:                event.Note,
		ReportingController: event.ReportingController,
		ReportingInstance:   event.ReportingInstance,
		Type:                event.Type,
		Manifest:            &YamlManifest{Value: event.Manifest},
	}

	return parsedEvent
}

func ToEvents(events []cluster.Event) []*Event {
	output := make([]*Event, 0)
	for _, val := range events {
		output = append(output, ToEvent(val))
	}
	return output
}
