package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/diff"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/mattbaird/jsonpatch"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
)

const maxLogSizeBytes = 10 * 1024 * 1024 // 10MiB

const name = "github.com/octopusdeploy/kubernetes-monitor/internal/cluster"

var tracer = otel.Tracer(name)

// Cluster is one Octopus machine rather than a Kubernetes cluster: many machines can monitor the same cluster
// through a sharedCluster. A single goroutine owns its application instances, so commands, sweeps and
// resource changes can't interleave.
type Cluster struct {
	ClusterId ClusterId

	logger  *slog.Logger
	shared  *sharedCluster
	updater MonitoredResourcesUpdater
	mailbox mailbox[ApplicationInstanceList]
}

// ManifestResolver returns the unstructured manifest for a cache.Resource.
// Use the cluster cache's copy when it's there, otherwise fall back to a get.
type ManifestResolver interface {
	ResolveManifest(ctx context.Context, res *cache.Resource) (*unstructured.Unstructured, error)
}

type MonitoredResourcesUpdater interface {
	Update(ctx context.Context, update *ApplicationInstanceChanges)
	Replace(ctx context.Context, replacement *ApplicationInstanceChanges)
}
type NoOpUpdater struct{}

func (_ NoOpUpdater) Update(_ context.Context, _ *ApplicationInstanceChanges) {}

func (_ NoOpUpdater) Replace(_ context.Context, _ *ApplicationInstanceChanges) {}

type ClusterId string

type LogLine struct {
	Timestamp time.Time
	Message   string
}

type Event struct {
	FirstObservedTime   time.Time
	LastObservedTime    time.Time
	Count               int32
	Action              string
	Reason              string
	Note                string
	ReportingController string
	ReportingInstance   string
	Type                string
	Manifest            string
}

func newCluster(
	ctx context.Context,
	id ClusterId,
	logger *slog.Logger,
	shared *sharedCluster,
	updater MonitoredResourcesUpdater,
) (*Cluster, error) {
	c := &Cluster{
		ClusterId: id,
		logger:    logger.With(slog.Any("clusterId", id)),
		shared:    shared,
		updater:   updater,
		mailbox:   newMailbox[ApplicationInstanceList](ctx.Done()),
	}

	changes := newChangeQueue(ctx)
	if err := shared.interests.subscribe(ctx, id, changes.in); err != nil {
		return nil, err
	}
	go c.run(ctx, changes.out)

	c.logger.Info("Monitoring resource changes for cluster")
	return c, nil
}

func (c *Cluster) run(ctx context.Context, changes <-chan resourceChange) {
	applicationInstances := NewApplicationInstanceList()
	for {
		select {
		case <-ctx.Done():
			return
		case op := <-c.mailbox.ops:
			op(applicationInstances)
		case change := <-changes:
			c.applyResourceChange(ctx, applicationInstances, change)
		}
	}
}

// DeleteDesiredResourcesExceptForVersion removes all desired resources that were applied with a different
// version that what is provided
func (c *Cluster) DeleteDesiredResourcesExceptForVersion(
	ctx context.Context, applicationInstanceId ApplicationInstanceId, versionToKeep Version,
) error {
	return do(ctx, c.mailbox, func(applicationInstances *ApplicationInstanceList) {
		if applicationInstance, ok := applicationInstances.Get(applicationInstanceId); ok {
			applicationInstance.DeleteDesiredResourcesExceptForVersion(versionToKeep)
			c.publishInterests(ctx, applicationInstances)
		}
	})
}

// DeleteDesiredResources removes only the listed resource IDs from the in-memory desired resource map
// for the given application instance, leaving all others untouched
func (c *Cluster) DeleteDesiredResources(
	ctx context.Context, applicationInstanceId ApplicationInstanceId, resourceIds []DesiredResourceId,
) error {
	return do(ctx, c.mailbox, func(applicationInstances *ApplicationInstanceList) {
		if applicationInstance, ok := applicationInstances.Get(applicationInstanceId); ok {
			applicationInstance.DeleteDesiredResources(resourceIds)
			c.publishInterests(ctx, applicationInstances)
		}
	})
}

// MergeDesiredResources upserts the provided desired resources into the matching application instance,
// sending the updated monitored resources back to server in the process
func (c *Cluster) MergeDesiredResources(
	ctx context.Context, applicationInstanceId ApplicationInstanceId,
	desiredResources map[kube.ResourceKey]*DesiredResource,
	hashSalt crypto.HashSalt,
) error {
	ctx, span := tracer.Start(ctx, "Cluster.MergeDesiredResources")
	defer span.End()

	// An empty delta says nothing, so there is nothing to do.
	if len(desiredResources) == 0 {
		return nil
	}

	var update *ApplicationInstanceChanges
	err := call(ctx, c.mailbox, func(applicationInstances *ApplicationInstanceList) (err error) {
		update, err = c.mergeDesiredResources(ctx, applicationInstances, applicationInstanceId, desiredResources, hashSalt)
		return err
	})
	if err != nil {
		return err
	}

	c.logApplicationInstanceChanges("Sending resource updates due to updated desired resources", update)
	c.updater.Update(ctx, update)
	return nil
}

// ReplaceDesiredResources takes the entire desired state for the application instance rather than a
// delta. Desired resources absent from the list are dropped, and the resulting monitored resources are
// sent to Server as a complete replacement instead of an update.
func (c *Cluster) ReplaceDesiredResources(
	ctx context.Context, applicationInstanceId ApplicationInstanceId,
	desiredResources map[kube.ResourceKey]*DesiredResource,
	hashSalt crypto.HashSalt,
) error {
	ctx, span := tracer.Start(ctx, "Cluster.ReplaceDesiredResources")
	defer span.End()

	var replacement *ApplicationInstanceChanges
	err := call(ctx, c.mailbox, func(applicationInstances *ApplicationInstanceList) (err error) {
		replacement, err = c.replaceDesiredResources(
			ctx, applicationInstances, applicationInstanceId, desiredResources, hashSalt)
		return err
	})
	if err != nil {
		return err
	}

	c.logApplicationInstanceChanges("Sending resource replacement due to replaced desired resources", replacement)
	c.updater.Replace(ctx, replacement)
	return nil
}

// applicationInstanceUpdates expects the caller to have synced the shared cluster, once for every target.
func (c *Cluster) applicationInstanceUpdates(ctx context.Context) ([]*ApplicationInstanceChanges, error) {
	return ask(ctx, c.mailbox, func(applicationInstances *ApplicationInstanceList) []*ApplicationInstanceChanges {
		var updates []*ApplicationInstanceChanges
		for applicationInstanceId, applicationInstance := range applicationInstances.All() {
			ctx, span := tracer.Start(ctx, "Cluster.applicationInstanceUpdates")
			span.SetAttributes(attribute.String("applicationInstanceId", string(applicationInstanceId)))
			span.SetAttributes(attribute.String("clusterId", string(c.ClusterId)))

			if err := applicationInstance.ReplaceMonitoredResourcesFromCluster(ctx, c.ClusterId, c.shared); err != nil {
				c.logger.
					With(slog.Any("error", err)).
					With(slog.String("applicationInstanceId", string(applicationInstanceId))).
					Error("Failed to get monitored resources")
			} else {
				updates = append(updates, applicationInstance.MonitoredResourceChanges(c.ClusterId))
			}
			span.End()
		}

		// Republishing every sweep drops interests the index inherited while listing that this target never
		// went on to monitor, so they can't accumulate.
		c.publishInterests(ctx, applicationInstances)
		return updates
	})
}

func (c *Cluster) GetContainerLogs(
	namespace string,
	pod string,
	container string,
	previous bool,
	ctx context.Context,
) ([]LogLine, error) {
	return c.shared.containerLogs(namespace, pod, container, previous, ctx)
}

func (c *Cluster) GetEvents(
	namespace string,
	name string,
	kind string,
	ctx context.Context,
) ([]Event, error) {
	return c.shared.events(namespace, name, kind, ctx)
}

func (c *Cluster) logApplicationInstanceChanges(message string, changes *ApplicationInstanceChanges) {
	c.logger.Info(message,
		slog.Int("presentMonitoredResources", len(changes.PresentMonitoredResources)),
		slog.Int("childMonitoredResources", len(changes.ChildMonitoredResources)),
		slog.Int("missingMonitoredResources", len(changes.MissingMonitoredResources)),
		slog.Int("unknownMonitoredResources", len(changes.UnknownMonitoredResources)),
		slog.Any("key", changes.ApplicationInstanceId),
	)
}

func (c *Cluster) mergeDesiredResources(
	ctx context.Context, applicationInstances *ApplicationInstanceList,
	applicationInstanceId ApplicationInstanceId,
	desiredResources map[kube.ResourceKey]*DesiredResource,
	hashSalt crypto.HashSalt,
) (*ApplicationInstanceChanges, error) {
	ctx, span := tracer.Start(ctx, "Cluster.mergeDesiredResources")
	defer span.End()
	c.setDesiredResourceAttributes(span, applicationInstanceId, desiredResources)

	desiredResources = c.resolveNamespaces(ctx, desiredResources)
	desiredResourceIds := make([]DesiredResourceId, 0, len(desiredResources))
	for _, desiredResource := range desiredResources {
		desiredResourceIds = append(desiredResourceIds, desiredResource.Id)
	}

	applicationInstance := applicationInstances.getOrCreateApplicationInstance(applicationInstanceId, hashSalt)
	applicationInstance.MergeDesiredResources(ctx, c.ClusterId, desiredResources)
	if err := c.rescan(ctx, applicationInstances, applicationInstance); err != nil {
		return nil, err
	}

	// Resolve the UIDs of any desired resources
	// Only necessary for desired resources that are present
	isIncoming := func(id DesiredResourceId) bool { return slices.Contains(desiredResourceIds, id) }

	var desiredAndPresentResources []*PresentMonitoredResource
	desiredAndPresentResourceUids := make([]types.UID, 0, len(applicationInstance.presentMonitoredResources))
	for id, presentResource := range applicationInstance.presentMonitoredResources {
		if isIncoming(id) {
			desiredAndPresentResources = append(desiredAndPresentResources, presentResource)
			desiredAndPresentResourceUids = append(desiredAndPresentResourceUids, presentResource.ResourceId)
		}
	}

	var childResources []*ChildMonitoredResource
	for _, childResource := range applicationInstance.childMonitoredResources {
		if slices.Contains(desiredAndPresentResourceUids, childResource.RootOwnerId) {
			childResources = append(childResources, childResource)
		}
	}

	var missingResources []*MissingMonitoredResource
	for id, missingResource := range applicationInstance.missingMonitoredResources {
		if isIncoming(id) {
			missingResources = append(missingResources, missingResource)
		}
	}

	var unknownResources []*UnknownMonitoredResource
	for id, unknownResource := range applicationInstance.unknownMonitoredResources {
		if isIncoming(id) {
			unknownResources = append(unknownResources, unknownResource)
		}
	}

	return &ApplicationInstanceChanges{
		ApplicationInstanceId:     applicationInstance.ApplicationInstanceId,
		ClusterId:                 c.ClusterId,
		PresentMonitoredResources: desiredAndPresentResources,
		ChildMonitoredResources:   childResources,
		MissingMonitoredResources: missingResources,
		UnknownMonitoredResources: unknownResources,
	}, nil
}

func (c *Cluster) replaceDesiredResources(
	ctx context.Context, applicationInstances *ApplicationInstanceList,
	applicationInstanceId ApplicationInstanceId,
	desiredResources map[kube.ResourceKey]*DesiredResource,
	hashSalt crypto.HashSalt,
) (*ApplicationInstanceChanges, error) {
	ctx, span := tracer.Start(ctx, "Cluster.replaceDesiredResources")
	defer span.End()
	c.setDesiredResourceAttributes(span, applicationInstanceId, desiredResources)

	desiredResources = c.resolveNamespaces(ctx, desiredResources)

	applicationInstance := applicationInstances.getOrCreateApplicationInstance(applicationInstanceId, hashSalt)
	applicationInstance.ReplaceDesiredResources(ctx, c.ClusterId, desiredResources)
	if err := c.rescan(ctx, applicationInstances, applicationInstance); err != nil {
		return nil, err
	}

	return applicationInstance.MonitoredResourceChanges(c.ClusterId), nil
}

// rescan publishes the target's interests before syncing so that, on the cluster's first sync, the cache
// keeps the manifests of the resources just desired.
func (c *Cluster) rescan(
	ctx context.Context, applicationInstances *ApplicationInstanceList, applicationInstance *ApplicationInstance,
) error {
	applicationInstances.UpsertApplicationInstance(applicationInstance)
	c.publishInterests(ctx, applicationInstances)

	if err := c.shared.sync(); err != nil {
		c.logFailedRescan(applicationInstance, err)
		return err
	}

	if err := applicationInstance.ReplaceMonitoredResourcesFromCluster(ctx, c.ClusterId, c.shared); err != nil {
		c.logFailedRescan(applicationInstance, err)
		return err
	}

	c.publishInterests(ctx, applicationInstances)
	return nil
}

func (c *Cluster) logFailedRescan(applicationInstance *ApplicationInstance, err error) {
	c.logger.Error(
		"Failed to get monitored resources",
		slog.Any("error", err),
		slog.String("applicationInstanceId", string(applicationInstance.ApplicationInstanceId)),
	)
}

func (c *Cluster) resolveNamespaces(
	ctx context.Context, desiredResources map[kube.ResourceKey]*DesiredResource,
) map[kube.ResourceKey]*DesiredResource {
	namespacedMap, _ := c.shared.namespaceMapWithHealth(ctx)

	resolved := make(map[kube.ResourceKey]*DesiredResource, len(desiredResources))
	for _, desiredResource := range desiredResources {
		desiredResource.ResolveNamespace(namespacedMap)
		// Try one more time to resolve the namespace after an invalidation
		// TODO: We should not do this in the refactor, and instead only do this occasionally
		if !desiredResource.IsNamespaceResolved {
			c.shared.invalidateDiscovery()
			namespacedMap, _ = c.shared.namespaceMapWithHealth(ctx)
			desiredResource.ResolveNamespace(namespacedMap)
		}
		resolved[desiredResource.ResourceKey()] = desiredResource
	}

	return resolved
}

func (c *Cluster) publishInterests(ctx context.Context, applicationInstances *ApplicationInstanceList) {
	if err := c.shared.interests.set(ctx, c.ClusterId, applicationInstances.resourceKeysOfInterest()); err != nil {
		c.logger.Warn("Failed to publish the resources this cluster monitors", slog.Any("error", err))
	}
}

// applyResourceChange runs on the target's goroutine, outside the cache's lock, so it's free to fetch
// manifests from the API server.
func (c *Cluster) applyResourceChange(
	ctx context.Context, applicationInstances *ApplicationInstanceList, change resourceChange,
) {
	ctx, span := tracer.Start(ctx, "Cluster.applyResourceChange")
	defer span.End()

	span.SetAttributes(attribute.String("clusterId", string(c.ClusterId)))
	span.SetAttributes(resourceTraceAttributes(change.newRes)...)

	updates, err := applicationInstances.GetChangesForUpdatedResource(
		ctx,
		c.ClusterId,
		c.shared,
		change.newRes,
		change.oldRes,
	)
	if err != nil {
		c.logger.Error(err.Error())
	}

	if len(updates) > 0 {
		c.publishInterests(ctx, applicationInstances)
	}

	for _, applicationInstanceUpdate := range updates {
		c.logApplicationInstanceChanges(
			"Sending resource updates due to updated monitored resource",
			applicationInstanceUpdate,
		)

		c.updater.Update(ctx, applicationInstanceUpdate)
	}
}

func (c *Cluster) setDesiredResourceAttributes(
	span trace.Span, applicationInstanceId ApplicationInstanceId,
	desiredResources map[kube.ResourceKey]*DesiredResource,
) {
	span.SetAttributes(attribute.String("applicationInstanceId", string(applicationInstanceId)))
	span.SetAttributes(attribute.Int("desiredResourceCount", len(desiredResources)))
	span.SetAttributes(attribute.String("clusterId", string(c.ClusterId)))
}

// GetRootParentResource recursively searches for the top level ownerId for the child resource provided.
func getRootParentResource(ownerId types.UID, childResources map[kube.ResourceKey]*ChildMonitoredResource) types.UID {
	for _, possibleParentResource := range childResources {
		if ownerId == possibleParentResource.ResourceId {
			return getRootParentResource(possibleParentResource.OwnerId, childResources)
		}
	}

	return ownerId
}

// gvkIsKnownToCluster reports whether the desired resource's type is still registered
// in the cluster's discovered API resources. GetAPIResources() is only populated by a
// successful cache sync, and callers of getMonitoredResources bail out when the sync fails,
// so a GroupKind being absent is authoritative — the type genuinely no longer exists (e.g.
// its CRD was deleted) rather than reflecting a transient discovery failure.
func gvkIsKnownToCluster(apiResourceGKs map[schema.GroupKind]struct{}, desiredResource *DesiredResource) bool {
	gk := desiredResource.Details.ManifestType.GroupVersionKind().GroupKind()
	_, known := apiResourceGKs[gk]
	return known
}

func getSyncStatus(manifest *SanitizedManifest, desiredResource *DesiredResource) *SyncStatus {
	// Orphan-ness replaces sync state, so there's nothing to be gained by diffing against a manifest
	// Octopus has stopped expecting.
	if desiredResource.IsOrphaned() {
		syncStatus := NewSyncStatus(desiredResource, SyncStatusOrphaned, "", "")
		return &syncStatus
	}

	if manifest == nil {
		return &SyncStatus{
			Status:  SyncStatusUnknown,
			Message: "Missing resource manifest",
		}
	}

	if desiredResource.Manifest == nil {
		return &SyncStatus{
			Status:  SyncStatusUnknown,
			Message: "Missing desired resource manifest",
		}
	}

	result, err := diff.Diff(
		context.TODO(), desiredResource.Manifest, (*unstructured.Unstructured)(manifest), []diff.Option{}...)

	if err != nil {
		return &SyncStatus{
			Status:  SyncStatusUnknown,
			Message: err.Error(),
		}
	} else {
		if result.Modified {
			jsonPatch, _ := jsonpatch.CreatePatch(result.NormalizedLive, result.PredictedLive)
			patch, marshallErr := json.Marshal(jsonPatch)
			if marshallErr != nil {
				return &SyncStatus{
					Status:  SyncStatusUnknown,
					Message: marshallErr.Error(),
				}
			}
			return &SyncStatus{
				Status:    SyncStatusOutOfSync,
				JsonPatch: string(patch),
			}
		} else {
			return &SyncStatus{
				Status: SyncStatusInSync,
			}
		}
	}
}

func resourceTraceAttributes(res *cache.Resource) []attribute.KeyValue {
	if res == nil {
		return []attribute.KeyValue{
			attribute.String("resourceName", "unknown value"),
		}
	}
	return []attribute.KeyValue{
		attribute.String("resourceName", res.Ref.Name),
		attribute.String("resourceKind", res.Ref.Kind),
		attribute.String("resourceAPIVersion", res.Ref.APIVersion),
		attribute.String("resourceNamespace", res.Ref.Namespace),
		attribute.String("resourceUID", string(res.Ref.UID)),
	}
}

func parseLogs(ctx context.Context, stream io.Reader) ([]LogLine, error) {
	_, span := tracer.Start(ctx, "Cluster.parseLogs")
	defer span.End()

	cb := NewCircularBuffer(maxLogSizeBytes)
	readBuf := make([]byte, 1024) // 1KB buffer
	// Read the stream into the ring buffer
	for {
		n, err := stream.Read(readBuf)
		if n > 0 {
			cb.Write(readBuf[:n])
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	finalBytes := cb.Bytes()

	lines := bytes.Split(finalBytes, []byte("\n"))
	span.SetAttributes(attribute.Int("logLineCount", len(lines)))
	span.SetAttributes(attribute.Int("logLineSize", len(finalBytes)))

	logLines := make([]LogLine, 0)

	span.AddEvent("splitting log lines")
	for i, l := range lines {
		// Skip empty lines
		if len(l) == 0 {
			continue
		}
		line := string(l)
		ts, message, err := splitLogLine(line)
		// First line may be malformed, so we ignore it
		if i == 0 && err != nil {
			continue
		} else if err != nil {
			return logLines, err
		}
		logLines = append(logLines, LogLine{
			Timestamp: ts,
			Message:   message,
		})
	}
	return logLines, nil
}

func splitLogLine(line string) (time.Time, string, error) {
	split := strings.SplitN(line, " ", 2)
	if len(split) != 2 {
		return time.Time{}, line, errors.New("malformed log line")
	}
	ts, err := time.Parse(time.RFC3339Nano, split[0])
	return ts, split[1], err
}
