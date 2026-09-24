package cluster

type SyncStatus struct {
	Status    SyncStatusCode
	Message   string
	JsonPatch string
}

type SyncStatusCode string

const (
	// Assessment failed and actual sync status is unknown
	SyncStatusUnknown SyncStatusCode = "Unknown"
	// The resource isn't explicit created via manifests, so sync status is not applicable eg. Pod created by a Deployment
	SyncStatusNotApplicable SyncStatusCode = "NotApplicable"
	// The resource is equivalent to what was deployed
	SyncStatusInSync SyncStatusCode = "InSync"
	// The resource differs from what was deployed
	SyncStatusOutOfSync SyncStatusCode = "OutOfSync"
	// Octopus no longer expects the resource, so there is nothing left to compare it against
	SyncStatusOrphaned SyncStatusCode = "Orphaned"
)

func NewSyncStatus(
	desiredResource *DesiredResource,
	status SyncStatusCode,
	message string,
	jsonPatch string,
) SyncStatus {
	if desiredResource.IsOrphaned() {
		return SyncStatus{Status: SyncStatusOrphaned, Message: message}
	}

	return SyncStatus{
		Status:    status,
		Message:   message,
		JsonPatch: jsonPatch,
	}
}
