package cluster

import (
	"context"
	"log/slog"
	"slices"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
)

// clusterState is a target's mutable state and every operation that changes it. Only the target's goroutine
// holds it, through the Cluster's mailbox, so it isn't safe for concurrent use. Operations return the changes
// to send rather than sending them, so the Cluster does all the talking to Octopus.
type clusterState struct {
	clusterId            ClusterId
	logger               *slog.Logger
	shared               *sharedCluster
	applicationInstances map[ApplicationInstanceId]*ApplicationInstance
}

func newClusterState(clusterId ClusterId, logger *slog.Logger, shared *sharedCluster) *clusterState {
	return &clusterState{
		clusterId:            clusterId,
		logger:               logger,
		shared:               shared,
		applicationInstances: map[ApplicationInstanceId]*ApplicationInstance{},
	}
}

func (s *clusterState) upsertApplicationInstance(applicationInstance *ApplicationInstance) {
	s.applicationInstances[applicationInstance.ApplicationInstanceId] = applicationInstance
}

func (s *clusterState) getApplicationInstance(
	applicationInstanceId ApplicationInstanceId,
) (*ApplicationInstance, bool) {
	applicationInstance, ok := s.applicationInstances[applicationInstanceId]
	return applicationInstance, ok
}

func (s *clusterState) getOrCreateApplicationInstance(
	applicationInstanceId ApplicationInstanceId, hashSalt crypto.HashSalt,
) *ApplicationInstance {
	applicationInstance, found := s.getApplicationInstance(applicationInstanceId)
	if !found {
		applicationInstance = NewApplicationInstance(applicationInstanceId, hashSalt)
	}

	return applicationInstance
}

func (s *clusterState) deleteDesiredResourcesExceptForVersion(
	ctx context.Context, applicationInstanceId ApplicationInstanceId, versionToKeep Version,
) {
	if applicationInstance, ok := s.getApplicationInstance(applicationInstanceId); ok {
		applicationInstance.DeleteDesiredResourcesExceptForVersion(versionToKeep)
		s.publishInterests(ctx)
	}
}

func (s *clusterState) deleteDesiredResources(
	ctx context.Context, applicationInstanceId ApplicationInstanceId, resourceIds []DesiredResourceId,
) {
	if applicationInstance, ok := s.getApplicationInstance(applicationInstanceId); ok {
		applicationInstance.DeleteDesiredResources(resourceIds)
		s.publishInterests(ctx)
	}
}

func (s *clusterState) mergeDesiredResources(
	ctx context.Context,
	applicationInstanceId ApplicationInstanceId,
	desiredResources map[kube.ResourceKey]*DesiredResource,
	hashSalt crypto.HashSalt,
) (*ApplicationInstanceChanges, error) {
	ctx, span := tracer.Start(ctx, "Cluster.mergeDesiredResources")
	defer span.End()
	s.setDesiredResourceAttributes(span, applicationInstanceId, desiredResources)

	desiredResources = s.resolveNamespaces(ctx, desiredResources)
	desiredResourceIds := make([]DesiredResourceId, 0, len(desiredResources))
	for _, desiredResource := range desiredResources {
		desiredResourceIds = append(desiredResourceIds, desiredResource.Id)
	}

	applicationInstance := s.getOrCreateApplicationInstance(applicationInstanceId, hashSalt)
	applicationInstance.MergeDesiredResources(ctx, s.clusterId, desiredResources)
	if err := s.rescan(ctx, applicationInstance); err != nil {
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
		ClusterId:                 s.clusterId,
		PresentMonitoredResources: desiredAndPresentResources,
		ChildMonitoredResources:   childResources,
		MissingMonitoredResources: missingResources,
		UnknownMonitoredResources: unknownResources,
	}, nil
}

func (s *clusterState) replaceDesiredResources(
	ctx context.Context,
	applicationInstanceId ApplicationInstanceId,
	desiredResources map[kube.ResourceKey]*DesiredResource,
	hashSalt crypto.HashSalt,
) (*ApplicationInstanceChanges, error) {
	ctx, span := tracer.Start(ctx, "Cluster.replaceDesiredResources")
	defer span.End()
	s.setDesiredResourceAttributes(span, applicationInstanceId, desiredResources)

	desiredResources = s.resolveNamespaces(ctx, desiredResources)

	applicationInstance := s.getOrCreateApplicationInstance(applicationInstanceId, hashSalt)
	applicationInstance.ReplaceDesiredResources(ctx, s.clusterId, desiredResources)
	if err := s.rescan(ctx, applicationInstance); err != nil {
		return nil, err
	}

	return applicationInstance.MonitoredResourceChanges(s.clusterId), nil
}

// monitoredResourceUpdates expects the caller to have synced the shared cluster, once for every target.
func (s *clusterState) monitoredResourceUpdates(ctx context.Context) []*ApplicationInstanceChanges {
	var updates []*ApplicationInstanceChanges
	for applicationInstanceId, applicationInstance := range s.applicationInstances {
		ctx, span := tracer.Start(ctx, "Cluster.applicationInstanceUpdates")
		span.SetAttributes(attribute.String("applicationInstanceId", string(applicationInstanceId)))
		span.SetAttributes(attribute.String("clusterId", string(s.clusterId)))

		if err := applicationInstance.ReplaceMonitoredResourcesFromCluster(ctx, s.clusterId, s.shared); err != nil {
			s.logger.
				With(slog.Any("error", err)).
				With(slog.String("applicationInstanceId", string(applicationInstanceId))).
				Error("Failed to get monitored resources")
		} else {
			updates = append(updates, applicationInstance.MonitoredResourceChanges(s.clusterId))
		}
		span.End()
	}

	// Republishing every sweep drops interests the index inherited while listing that this target never
	// went on to monitor, so they can't accumulate.
	s.publishInterests(ctx)
	return updates
}

// applyResourceChange runs on the target's goroutine, outside the cache's lock, so it's free to fetch
// manifests from the API server.
func (s *clusterState) applyResourceChange(ctx context.Context, change resourceChange) []*ApplicationInstanceChanges {
	ctx, span := tracer.Start(ctx, "Cluster.applyResourceChange")
	defer span.End()

	span.SetAttributes(attribute.String("clusterId", string(s.clusterId)))
	span.SetAttributes(resourceTraceAttributes(change.newRes)...)

	updates, err := s.changesForUpdatedResource(ctx, change.newRes, change.oldRes)
	if err != nil {
		s.logger.Error(err.Error())
	}

	if len(updates) > 0 {
		s.publishInterests(ctx)
	}
	return updates
}

func (s *clusterState) changesForUpdatedResource(
	ctx context.Context, newRes *cache.Resource, oldRes *cache.Resource,
) ([]*ApplicationInstanceChanges, error) {
	ctx, span := tracer.Start(ctx, "Cluster.changesForUpdatedResource")
	defer span.End()
	span.SetAttributes(attribute.String("clusterId", string(s.clusterId)))
	var oldKey kube.ResourceKey
	var newKey kube.ResourceKey
	if oldRes != nil && oldRes.Resource != nil {
		oldKey = oldRes.ResourceKey()
	}
	if newRes != nil && newRes.Resource != nil {
		newKey = newRes.ResourceKey()
	}
	span.SetAttributes(attribute.String("oldRes", oldKey.String()))
	span.SetAttributes(attribute.String("newRes", newKey.String()))

	var updateApplicationInstanceRequests []*ApplicationInstanceChanges
	var errs error
	for applicationInstanceId, applicationInstance := range s.applicationInstances {
		instanceCtx, instanceSpan := tracer.Start(ctx, "Cluster.changesForUpdatedResource")
		instanceSpan.SetAttributes(attribute.String("applicationInstanceId", string(applicationInstanceId)))

		updateApplicationInstanceRequest, err := applicationInstance.GetChangesForUpdatedResource(
			instanceCtx,
			s.clusterId,
			s.shared,
			newRes,
			oldRes,
		)
		instanceSpan.End()
		if err != nil {
			errs = multierr.Append(errs, err)
			continue
		}

		if updateApplicationInstanceRequest != nil {
			updateApplicationInstanceRequests = append(
				updateApplicationInstanceRequests,
				updateApplicationInstanceRequest,
			)
		}
	}

	return updateApplicationInstanceRequests, errs
}

// rescan publishes the target's interests before syncing so that, on the cluster's first sync, the cache
// keeps the manifests of the resources just desired.
func (s *clusterState) rescan(ctx context.Context, applicationInstance *ApplicationInstance) error {
	s.upsertApplicationInstance(applicationInstance)
	s.publishInterests(ctx)

	if err := s.shared.sync(); err != nil {
		s.logFailedRescan(applicationInstance, err)
		return err
	}

	if err := applicationInstance.ReplaceMonitoredResourcesFromCluster(ctx, s.clusterId, s.shared); err != nil {
		s.logFailedRescan(applicationInstance, err)
		return err
	}

	s.publishInterests(ctx)
	return nil
}

func (s *clusterState) logFailedRescan(applicationInstance *ApplicationInstance, err error) {
	s.logger.Error(
		"Failed to get monitored resources",
		slog.Any("error", err),
		slog.String("applicationInstanceId", string(applicationInstance.ApplicationInstanceId)),
	)
}

func (s *clusterState) resolveNamespaces(
	ctx context.Context, desiredResources map[kube.ResourceKey]*DesiredResource,
) map[kube.ResourceKey]*DesiredResource {
	namespacedMap, _ := s.shared.namespaceMapWithHealth(ctx)

	resolved := make(map[kube.ResourceKey]*DesiredResource, len(desiredResources))
	for _, desiredResource := range desiredResources {
		desiredResource.ResolveNamespace(namespacedMap)
		// Try one more time to resolve the namespace after an invalidation
		// TODO: We should not do this in the refactor, and instead only do this occasionally
		if !desiredResource.IsNamespaceResolved {
			s.shared.invalidateDiscovery()
			namespacedMap, _ = s.shared.namespaceMapWithHealth(ctx)
			desiredResource.ResolveNamespace(namespacedMap)
		}
		resolved[desiredResource.ResourceKey()] = desiredResource
	}

	return resolved
}

func (s *clusterState) publishInterests(ctx context.Context) {
	if err := s.shared.interests.set(ctx, s.clusterId, s.resourceKeysOfInterest()); err != nil {
		s.logger.Warn("Failed to publish the resources this cluster monitors", slog.Any("error", err))
	}
}

func (s *clusterState) resourceKeysOfInterest() map[kube.ResourceKey]struct{} {
	keys := map[kube.ResourceKey]struct{}{}
	for _, applicationInstance := range s.applicationInstances {
		for key := range applicationInstance.resourceKeysOfInterest() {
			keys[key] = struct{}{}
		}
	}
	return keys
}

func (s *clusterState) setDesiredResourceAttributes(
	span trace.Span, applicationInstanceId ApplicationInstanceId,
	desiredResources map[kube.ResourceKey]*DesiredResource,
) {
	span.SetAttributes(attribute.String("applicationInstanceId", string(applicationInstanceId)))
	span.SetAttributes(attribute.Int("desiredResourceCount", len(desiredResources)))
	span.SetAttributes(attribute.String("clusterId", string(s.clusterId)))
}

func ownerRefResourceKey(ownerRef v1.OwnerReference, namespace string) (kube.ResourceKey, error) {
	gv, err := schema.ParseGroupVersion(ownerRef.APIVersion)
	return kube.NewResourceKey(gv.Group, ownerRef.Kind, namespace, ownerRef.Name), err
}
