package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"maps"
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
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/yaml"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
)

const maxLogSizeBytes = 10 * 1024 * 1024 // 10MiB

const name = "github.com/octopusdeploy/kubernetes-monitor/internal/cluster"

var tracer = otel.Tracer(name)

type Cluster struct {
	ClusterId            ClusterId
	ApplicationInstances *ApplicationInstanceList

	mutex  MutexWithLogging
	logger *slog.Logger

	clusterCache                 Cache
	resourceFilter               kube.ResourceFilter
	clusterServer                string
	cacheInvalidationRequestedAt time.Time
	namespaceScopedMode          bool // true when targetNamespaces is set and clusterScopedResources is false
	targetNamespaces             map[string]struct{}

	cachedDiscoveryClient discovery.CachedDiscoveryInterface
	clientSet             kubernetes.Interface
	dynamicClient         dynamic.Interface

	updateMonitoredResourcesFunc MonitoredResourcesUpdater
	onResourceUpdatedUnsubscribe cache.Unsubscribe
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

func NewCluster(
	id ClusterId,
	applicationInstances *ApplicationInstanceList,
	logger *slog.Logger,
	clusterCache Cache,
	resourceFilter kube.ResourceFilter,
	cachedDiscoveryClient discovery.CachedDiscoveryInterface,
	clientSet kubernetes.Interface,
	dynamicClient dynamic.Interface,
	monitoredResourcesUpdater MonitoredResourcesUpdater,
	namespaceScopedMode bool,
	targetNamespaces []string,
) *Cluster {
	targetNamespacesMap := make(map[string]struct{}, len(targetNamespaces))
	for _, namespace := range targetNamespaces {
		targetNamespacesMap[namespace] = struct{}{}
	}

	c := &Cluster{
		ClusterId:                    id,
		ApplicationInstances:         applicationInstances,
		mutex:                        NewMutexWithLogging(logger, "Cluster"),
		logger:                       logger,
		clusterCache:                 clusterCache,
		resourceFilter:               resourceFilter,
		clusterServer:                clusterCache.GetClusterInfo().Server,
		cacheInvalidationRequestedAt: time.Now().UTC(),
		namespaceScopedMode:          namespaceScopedMode,
		targetNamespaces:             targetNamespacesMap,
		cachedDiscoveryClient:        cachedDiscoveryClient,
		clientSet:                    clientSet,
		dynamicClient:                dynamicClient,
		updateMonitoredResourcesFunc: monitoredResourcesUpdater,
	}

	c.RegisterOnResourceUpdatedEventHandler()

	return c
}

// ResolveManifest returns the manifest stored on the cache.Resource or
// fetches it live.
func (c *Cluster) ResolveManifest(ctx context.Context, res *cache.Resource) (*unstructured.Unstructured, error) {
	if res == nil {
		return nil, nil
	}
	if res.Resource != nil {
		return res.Resource, nil
	}
	if c.dynamicClient == nil {
		return nil, nil
	}
	gvr, namespaced, err := c.lookupGVR(res.Ref.GroupVersionKind())
	if err != nil {
		return nil, err
	}
	resClient := c.dynamicClient.Resource(gvr)
	if namespaced {
		return resClient.Namespace(res.Ref.Namespace).Get(ctx, res.Ref.Name, v1.GetOptions{})
	}
	return resClient.Get(ctx, res.Ref.Name, v1.GetOptions{})
}

// lookupGVR resolves a GVK to its plural GroupVersionResource via the cached
// discovery client, and reports whether the resource is namespaced.
func (c *Cluster) lookupGVR(gvk schema.GroupVersionKind) (schema.GroupVersionResource, bool, error) {
	if c.cachedDiscoveryClient == nil {
		return schema.GroupVersionResource{}, false, errors.New("no cached discovery client configured")
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(c.cachedDiscoveryClient)
	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return schema.GroupVersionResource{}, false, fmt.Errorf("rest mapping for %s: %w", gvk, err)
	}
	return mapping.Resource, mapping.Scope.Name() == meta.RESTScopeNameNamespace, nil
}

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

// RequestCacheRefresh indicates that we should invalidate it the cluster cache next time we attempt to sync
func (c *Cluster) RequestCacheRefresh() {
	c.cacheInvalidationRequestedAt = time.Now().UTC()
}

// Sync ensures that the cluster cache is synced with an optional invalidation
func (c *Cluster) Sync() error {
	clusterInfo := c.clusterCache.GetClusterInfo()

	if clusterInfo.LastCacheSyncTime != nil &&
		clusterInfo.LastCacheSyncTime.Before(c.cacheInvalidationRequestedAt) {

		c.logger.Info("Invalidating cluster",
			slog.Any("key", c.ClusterId),
			slog.Any("cacheInvalidationRequestedAt", c.cacheInvalidationRequestedAt),
			slog.Any("LastCacheSyncTime", clusterInfo.LastCacheSyncTime))

		c.clusterCache.Invalidate()
	}

	c.logger.Info("Syncing cluster",
		slog.Any("key", c.ClusterId),
		slog.Any("LastCacheSyncTime", clusterInfo.LastCacheSyncTime))

	return c.clusterCache.EnsureSynced()
}

// DeleteDesiredResourcesExceptForVersion removes all desired resources that were applied with a different
// version that what is provided
func (c *Cluster) DeleteDesiredResourcesExceptForVersion(
	applicationInstanceId ApplicationInstanceId, versionToKeep Version,
) {
	c.mutex.Lock("DeleteDesiredResourcesExceptForVersion")
	defer c.mutex.Unlock("DeleteDesiredResourcesExceptForVersion")

	if applicationInstance, ok := c.ApplicationInstances.applicationInstances.Get(applicationInstanceId); ok {
		applicationInstance.DeleteDesiredResourcesExceptForVersion(versionToKeep)
	}
}

// DeleteDesiredResources removes only the listed resource IDs from the in-memory desired resource map
// for the given application instance, leaving all others untouched
func (c *Cluster) DeleteDesiredResources(
	applicationInstanceId ApplicationInstanceId, resourceIds []DesiredResourceId,
) {
	c.mutex.Lock("DeleteDesiredResources")
	defer c.mutex.Unlock("DeleteDesiredResources")

	if applicationInstance, ok := c.ApplicationInstances.applicationInstances.Get(applicationInstanceId); ok {
		applicationInstance.DeleteDesiredResources(resourceIds)
	}
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

	applicationInstanceUpdate, err := c.buildDesiredResourcesUpdate(
		ctx,
		applicationInstanceId,
		desiredResources,
		hashSalt,
	)
	if err != nil {
		return err
	}

	c.logApplicationInstanceChanges(
		"Sending resource updates due to updated desired resources",
		applicationInstanceUpdate,
	)

	c.updateMonitoredResourcesFunc.Update(ctx, applicationInstanceUpdate)
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

	applicationInstanceReplacement, err := c.buildDesiredResourcesReplacement(
		ctx,
		applicationInstanceId,
		desiredResources,
		hashSalt,
	)
	if err != nil {
		return err
	}

	c.logApplicationInstanceChanges(
		"Sending resource replacement due to replaced desired resources",
		applicationInstanceReplacement,
	)

	c.updateMonitoredResourcesFunc.Replace(ctx, applicationInstanceReplacement)

	return nil
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

// buildDesiredResourcesUpdate merges the desired resources into the application instance and builds
// the update to send.
//
// The cluster mutex is released before this returns, so that the caller sends outside the lock.
func (c *Cluster) buildDesiredResourcesUpdate(
	ctx context.Context, applicationInstanceId ApplicationInstanceId,
	desiredResources map[kube.ResourceKey]*DesiredResource,
	hashSalt crypto.HashSalt,
) (*ApplicationInstanceChanges, error) {
	ctx, span := tracer.Start(ctx, "Cluster.buildDesiredResourcesUpdate")
	defer span.End()

	lockAttributes := trace.WithAttributes(attribute.String("applicationInstanceId", string(applicationInstanceId)))
	span.AddEvent("Locking cluster to apply desired resources", lockAttributes)
	c.mutex.Lock("buildDesiredResourcesUpdate")
	defer c.mutex.Unlock("buildDesiredResourcesUpdate")
	span.AddEvent("Locked cluster to apply desired resources", lockAttributes)

	desiredResourceIds := c.resolveAndTrackDesiredResources(ctx, desiredResources)

	applicationInstance, err := c.ApplicationInstances.MergeDesiredResources(
		ctx,
		c,
		applicationInstanceId,
		hashSalt,
		desiredResources,
	)
	if err != nil {
		c.logger.Error(
			"Failed to get monitored resources",
			slog.Any("error", err),
			slog.String("applicationInstanceId", string(applicationInstanceId)),
		)

		return nil, err
	}

	// Resolve the UIDs of any desired resources
	// Only necessary for desired resources that are present
	desiredAndPresentResources := applicationInstance.presentMonitoredResources.Filter(
		func(k DesiredResourceId, _ *PresentMonitoredResource) bool {
			return slices.Contains(desiredResourceIds, k)
		}).GetAll()

	desiredAndPresentResourcesUids := make([]types.UID, len(desiredAndPresentResources))
	for _, presentResource := range desiredAndPresentResources {
		desiredAndPresentResourcesUids = append(desiredAndPresentResourcesUids, presentResource.ResourceId)
	}

	return &ApplicationInstanceChanges{
		ApplicationInstanceId:     applicationInstance.ApplicationInstanceId,
		ClusterId:                 c.ClusterId,
		PresentMonitoredResources: desiredAndPresentResources,
		ChildMonitoredResources: applicationInstance.childMonitoredResources.Filter(
			func(_ kube.ResourceKey, r *ChildMonitoredResource) bool {
				return slices.Contains(desiredAndPresentResourcesUids, r.RootOwnerId)
			}).GetAll(),
		MissingMonitoredResources: applicationInstance.missingMonitoredResources.Filter(
			func(k DesiredResourceId, _ *MissingMonitoredResource) bool {
				return slices.Contains(desiredResourceIds, k)
			}).GetAll(),
		UnknownMonitoredResources: applicationInstance.unknownMonitoredResources.Filter(
			func(k DesiredResourceId, _ *UnknownMonitoredResource) bool {
				return slices.Contains(desiredResourceIds, k)
			}).GetAll(),
	}, nil
}

// buildDesiredResourcesReplacement replaces the desired resources on the application instance and
// builds the complete state to send.
//
// Releases the cluster mutex before returning, so that the caller sends outside the lock.
func (c *Cluster) buildDesiredResourcesReplacement(
	ctx context.Context, applicationInstanceId ApplicationInstanceId,
	desiredResources map[kube.ResourceKey]*DesiredResource,
	hashSalt crypto.HashSalt,
) (*ApplicationInstanceChanges, error) {
	ctx, span := tracer.Start(ctx, "Cluster.buildDesiredResourcesReplacement")
	defer span.End()

	lockAttributes := trace.WithAttributes(attribute.String("applicationInstanceId", string(applicationInstanceId)))
	span.AddEvent("Locking cluster to apply desired resources", lockAttributes)
	c.mutex.Lock("buildDesiredResourcesReplacement")
	defer c.mutex.Unlock("buildDesiredResourcesReplacement")
	span.AddEvent("Locked cluster to apply desired resources", lockAttributes)

	c.resolveAndTrackDesiredResources(ctx, desiredResources)

	applicationInstance, err := c.ApplicationInstances.ReplaceDesiredResources(
		ctx,
		c,
		applicationInstanceId,
		hashSalt,
		desiredResources,
	)
	if err != nil {
		c.logger.Error(
			"Failed to get monitored resources",
			slog.Any("error", err),
			slog.String("applicationInstanceId", string(applicationInstanceId)),
		)

		return nil, err
	}

	return applicationInstance.MonitoredResourceChanges(c.ClusterId), nil
}

// resolveAndTrackDesiredResources resolves the namespace of each desired resource, re-keying the map as
// namespaces become known, and returns their ids. Callers must hold the cluster mutex.
func (c *Cluster) resolveAndTrackDesiredResources(
	ctx context.Context, desiredResources map[kube.ResourceKey]*DesiredResource,
) []DesiredResourceId {
	namespacedMap, _ := c.GetNamespaceMapWithHealth(ctx)
	desiredResourceIds := make([]DesiredResourceId, 0, len(desiredResources))

	for _, desiredResource := range desiredResources {
		desiredResource.ResolveNamespace(namespacedMap)
		// Try one more time to resolve the namespace after an invalidation
		// TODO: We should not do this in the refactor, and instead only do this occasionally
		if !desiredResource.IsNamespaceResolved {
			c.InvalidateDiscoveryClient()
			namespacedMap, _ := c.GetNamespaceMapWithHealth(ctx)
			desiredResource.ResolveNamespace(namespacedMap)
		}
		resourceKey := desiredResource.ResourceKey()
		desiredResources[resourceKey] = desiredResource
		desiredResourceIds = append(desiredResourceIds, desiredResource.Id)

		c.ApplicationInstances.AddTrackedResourceKey(desiredResource.ResourceKey())
	}

	return desiredResourceIds
}

// GetApplicationInstanceUpdates does a full scan to find all MonitoredResources and updates Server with
// a complete replacement update
func (c *Cluster) GetApplicationInstanceUpdates(ctx context.Context) iter.Seq[*ApplicationInstanceChanges] {
	return func(yield func(*ApplicationInstanceChanges) bool) {
		// Sync once for the whole sweep rather than once per application instance.
		if err := c.Sync(); err != nil {
			c.logger.
				With(slog.Any("error", err)).
				With(slog.Any("clusterId", c.ClusterId)).
				Error("Skipping monitored resource sweep because the cluster cache failed to sync")
			return
		}

		c.ApplicationInstances.Iterate(func(
			applicationInstanceId ApplicationInstanceId, applicationInstance *ApplicationInstance,
		) bool {
			ctx, span := tracer.Start(ctx, "Cluster.GetApplicationInstanceUpdates")
			defer span.End()
			span.SetAttributes(attribute.String("applicationInstanceId", string(applicationInstanceId)))
			span.SetAttributes(attribute.String("clusterId", string(c.ClusterId)))

			updateWatcherRequest, err := c.buildApplicationInstanceUpdate(ctx, applicationInstance)
			if err != nil {
				c.logger.
					With(slog.Any("error", err)).
					With(slog.String("applicationInstanceId", string(applicationInstance.ApplicationInstanceId))).
					Error("Failed to get monitored resources")

				return true
			}

			if !yield(updateWatcherRequest) {
				return false
			}

			return true
		})
	}
}

// buildApplicationInstanceUpdate rescans the application instance against the cluster and builds its
// complete monitored resource state.
func (c *Cluster) buildApplicationInstanceUpdate(
	ctx context.Context, applicationInstance *ApplicationInstance,
) (*ApplicationInstanceChanges, error) {
	c.mutex.Lock("buildApplicationInstanceUpdate")
	defer c.mutex.Unlock("buildApplicationInstanceUpdate")

	err := applicationInstance.ReplaceMonitoredResourcesFromCluster(ctx, c)
	if err != nil {
		return nil, err
	}

	c.ApplicationInstances.UpsertApplicationInstance(applicationInstance)

	return applicationInstance.MonitoredResourceChanges(c.ClusterId), nil
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
// successful cache sync, and callers of getMonitoredResources bail out when Sync() fails,
// so a GroupKind being absent is authoritative — the type genuinely no longer exists (e.g.
// its CRD was deleted) rather than reflecting a transient discovery failure.
func gvkIsKnownToCluster(apiResourceGKs map[schema.GroupKind]struct{}, desiredResource *DesiredResource) bool {
	gk := desiredResource.Details.ManifestType.GroupVersionKind().GroupKind()
	_, known := apiResourceGKs[gk]
	return known
}

func (c *Cluster) getMonitoredResources(
	desiredResources map[kube.ResourceKey]*DesiredResource,
	hashSalt crypto.HashSalt,
	discoveryHealthy bool,
) (map[DesiredResourceId]*PresentMonitoredResource, map[kube.ResourceKey]*ChildMonitoredResource, map[DesiredResourceId]*MissingMonitoredResource, map[DesiredResourceId]*UnknownMonitoredResource, error) {
	presentMonitoredResources := map[DesiredResourceId]*PresentMonitoredResource{}
	childMonitoredResources := map[kube.ResourceKey]*ChildMonitoredResource{}
	missingMonitoredResources := map[DesiredResourceId]*MissingMonitoredResource{}
	unknownMonitoredResources := map[DesiredResourceId]*UnknownMonitoredResource{}

	// GetAPIResources is a full list of GVKs, even if we skip them since we don't have permissions
	apiResourceGKs := make(map[schema.GroupKind]struct{})
	for _, apiRes := range c.clusterCache.GetAPIResources() {
		apiResourceGKs[apiRes.GroupKind] = struct{}{}
	}

	for _, desiredResource := range desiredResources {
		// If the GVK doesn't exist and the discovery returned a full result, then don't mark as unknown.
		// Let the usual "Missing" resource detection run below
		if !gvkIsKnownToCluster(apiResourceGKs, desiredResource) {
			if !discoveryHealthy {
				unknownMonitoredResources[desiredResource.Id] = NewUnknownMonitoredResource(
					c.ClusterId,
					desiredResource,
					"",
				)
			}
			continue
		}

		// If we can't determine the namespace, we mark it as unknown until the cluster has more information to give us
		// We do not do any further processing for desired resources that are unknown
		if !desiredResource.IsNamespaceResolved {
			unknownMonitoredResources[desiredResource.Id] = NewUnknownMonitoredResource(
				c.ClusterId,
				desiredResource,
				"",
			)
			continue
		}

		// In namespace-scoped mode, cluster-scoped resources can't be managed by gitops-engine —
		// GetManagedLiveObjs rejects them with a hard error that blocks all resources.
		// Mark them as unknown rather than letting one cluster-scoped resource poison the entire batch.
		if c.namespaceScopedMode && desiredResource.IsClusterScoped {
			c.logger.Info("Skipping cluster-scoped resource in namespace-scoped mode",
				slog.String("kind", desiredResource.Details.ManifestType.Kind),
				slog.String("name", desiredResource.Details.Name),
				slog.String("desiredResourceId", string(desiredResource.Id)),
			)
			unknownMonitoredResources[desiredResource.Id] = NewUnknownMonitoredResource(
				c.ClusterId,
				desiredResource,
				"Cluster-scoped resources can't be monitored in namespace-scoped mode",
			)
			continue
		}

		if len(c.targetNamespaces) > 0 && !desiredResource.IsClusterScoped {
			// Prefer the manifest's namespace over Details.Namespace because the manifest
			// is what Kubernetes (and gitops-engine) actually uses. Details.Namespace comes
			// from the server's AssumedNamespace, which is only a fallback for manifests
			// that don't specify one.
			resourceNamespace := desiredResource.Details.Namespace
			if desiredResource.Manifest != nil && desiredResource.Manifest.GetNamespace() != "" {
				resourceNamespace = desiredResource.Manifest.GetNamespace()
			}

			if _, namespaceIsManaged := c.targetNamespaces[resourceNamespace]; !namespaceIsManaged {
				c.logger.Info("Skipping out-of-scope namespaced resource",
					slog.String("kind", desiredResource.Details.ManifestType.Kind),
					slog.String("name", desiredResource.Details.Name),
					slog.String("namespace", resourceNamespace),
					slog.String("desiredResourceId", string(desiredResource.Id)),
				)
				unknownMonitoredResources[desiredResource.Id] = NewUnknownMonitoredResource(
					c.ClusterId,
					desiredResource,
					"Resource namespace \""+resourceNamespace+"\" is not in the monitored namespace list",
				)
				continue
			}
		}

		if c.resourceFilter != nil {
			gvk := desiredResource.Details.ManifestType.GroupVersionKind()
			if c.resourceFilter.IsExcludedResource(gvk.Group, gvk.Kind, c.clusterServer) {
				unknownMonitoredResources[desiredResource.Id] = NewUnknownMonitoredResource(
					c.ClusterId,
					desiredResource,
					fmt.Sprintf(
						"Resource %s is not in the monitored 'apiGroups' and 'resources' list",
						gvk.String(),
					),
				)
				continue
			}
		}
	}

	targetObjectIterator := maps.Values(desiredResources)
	var targetObjects []*unstructured.Unstructured
	for i := range targetObjectIterator {
		// Skip resources already marked as unknown (unresolved namespace or cluster-scoped in namespace mode)
		if _, isUnknown := unknownMonitoredResources[i.Id]; isUnknown {
			continue
		}
		// Never send a vanished-GVK resource to GetManagedLiveObjs — it can no longer be
		// mapped, which makes GetManagedLiveObjs hard-error and abort the entire
		// application-instance snapshot. It is handled by the missing-detection loop.
		if !gvkIsKnownToCluster(apiResourceGKs, i) {
			continue
		}
		if i.Manifest == nil {
			c.logger.Warn("Skipping desired resource with nil manifest",
				slog.String("desiredResourceId", string(i.Id)),
				slog.String("kind", i.Details.ManifestType.Kind),
				slog.String("name", i.Details.Name))
			continue
		}
		if c.resourceFilter != nil {
			gvk := i.Manifest.GroupVersionKind()
			if c.resourceFilter.IsExcludedResource(gvk.Group, gvk.Kind, c.clusterServer) {
				c.logger.Debug("Skipping desired resource excluded by ResourcesFilter",
					slog.String("desiredResourceId", string(i.Id)),
					slog.String("group", gvk.Group),
					slog.String("kind", gvk.Kind),
					slog.String("name", i.Details.Name))
				continue
			}
		}
		targetObjects = append(targetObjects, i.Manifest)
	}

	resourceIsDesired := func(resource *cache.Resource) bool {
		_, found := desiredResources[resource.Info.(ResourceInfo).ResourceKey]
		return found
	}

	// Get the live objects directly related to the desired resources
	// Only live objects that we have previously cached the manifest for will be returned from this function
	managedLiveObjs, err := c.clusterCache.GetManagedLiveObjs(targetObjects, resourceIsDesired)
	c.logger.With(slog.Any("managedLiveObjCount", len(managedLiveObjs))).
		With(slog.Any("targetObjCount", len(targetObjects))).
		Debug("Got managedLiveObjs")

	if err != nil {
		return nil, nil, nil, nil, err
	}

	liveObjs := slices.Collect(maps.Values(managedLiveObjs))
	managedLiveObjsKeys := make([]kube.ResourceKey, len(managedLiveObjs))
	for i, liveObj := range liveObjs {
		managedLiveObjsKeys[i] = kube.GetResourceKey(liveObj)
	}

	// Go through all managedLiveObjs and their children resources (recursively) to discover parent/child relationships
	// The callback runs under the cluster cache's read lock, so it only collects: ResolveManifest can hit the API
	// server, and holding the lock across network calls stalls watch events and deadlocks nested cache reads
	type discoveredResource struct {
		resource *cache.Resource
		ownerId  types.UID
		isOwned  bool
	}
	var discovered []discoveredResource
	c.clusterCache.IterateHierarchyV2(
		managedLiveObjsKeys,
		func(resource *cache.Resource, namespaceResources map[kube.ResourceKey]*cache.Resource) bool {
			ownerId, err := c.getFirstOwnerId(resource, namespaceResources)
			discovered = append(discovered, discoveredResource{
				resource: resource,
				ownerId:  ownerId,
				isOwned:  err == nil,
			})
			return true
		},
	)

	// Keep traversal order: getRootParentResource needs parents recorded before their children
	for _, d := range discovered {
		manifest, manifestErr := c.ResolveManifest(context.TODO(), d.resource)
		if manifestErr != nil {
			key := d.resource.ResourceKey()
			c.logger.Warn("Failed to resolve manifest for cached resource",
				slog.String("resource", key.String()),
				slog.Any("error", manifestErr))
		}

		desiredResource, found := desiredResources[d.resource.Info.(ResourceInfo).ResourceKey]
		if !d.isOwned && found {
			presentResource, err := NewPresentMonitoredResource(
				c.ClusterId,
				d.resource,
				manifest,
				desiredResource,
				hashSalt,
			)
			if err != nil {
				continue
			}
			presentMonitoredResources[presentResource.DesiredResourceId] = presentResource
		} else {
			rootOwnerId := getRootParentResource(d.ownerId, childMonitoredResources)
			childResource, err := NewChildMonitoredResource(
				c.ClusterId,
				d.resource,
				manifest,
				d.ownerId,
				rootOwnerId,
				hashSalt,
			)
			if err != nil {
				continue
			}
			childMonitoredResources[childResource.ResourceKey()] = childResource
		}
	}

	// Any desired resource that isn't accounted for in the present or unknown collections is considered missing
	accountedForMonitoredResources := map[DesiredResourceId]bool{}
	for _, presentMonitoredResource := range presentMonitoredResources {
		accountedForMonitoredResources[presentMonitoredResource.DesiredResourceId] = true
	}

	for _, unknownMonitoredResource := range unknownMonitoredResources {
		accountedForMonitoredResources[unknownMonitoredResource.DesiredResourceId] = true
	}

	for _, desiredResource := range maps.All(desiredResources) {
		_, found := accountedForMonitoredResources[desiredResource.Id]
		if !found {
			// Use Details.ManifestType to stay safe when Manifest is nil — both carry the same GVK.
			gk := desiredResource.Details.ManifestType.GroupVersionKind().GroupKind()

			// Check if this resource type was discovered in the API but excluded from cache tracking.
			// This happens when SetRespectRBAC is enabled and the service account lacks permission —
			// the GroupKind remains in GetAPIResources() but is removed from IsNamespaced().
			if _, existsInAPI := apiResourceGKs[gk]; existsInAPI {
				if _, isNamespacedErr := c.clusterCache.IsNamespaced(gk); isNamespacedErr != nil {
					c.logger.Warn("Resource type is not accessible, likely due to insufficient permissions",
						slog.String("group", gk.Group),
						slog.String("kind", gk.Kind),
						slog.String("name", desiredResource.Details.Name),
						slog.String("desiredResourceId", string(desiredResource.Id)),
					)
					unknownMonitoredResources[desiredResource.Id] = NewUnknownMonitoredResource(
						c.ClusterId,
						desiredResource,
						fmt.Sprintf(
							"Insufficient permissions to list or watch resources of type %s in API group %s",
							gk.Kind,
							gk.Group,
						),
					)
					continue
				}
			}

			missingMonitoredResources[desiredResource.Id] = NewMissingMonitoredResource(c.ClusterId, desiredResource)
		}
	}

	return presentMonitoredResources, childMonitoredResources, missingMonitoredResources, unknownMonitoredResources, nil
}

func (c *Cluster) getFirstOwnerId(
	res *cache.Resource, namespaceResources map[kube.ResourceKey]*cache.Resource,
) (types.UID, error) {
	ownerReferences := res.OwnerRefs

	if len(ownerReferences) == 0 {
		return "", errors.New("Resource has no owner")
	}

	if ownerReferences[0].UID != "" {
		return ownerReferences[0].UID, nil
	}

	// Some resources do not provide an ownerRef and we populate it manually (see getOwnerReferences)
	// We cannot get the ID then, so we try to get it here instead
	// Use the callback's namespace map, not clusterCache.FindResources: the caller already holds the cache's read
	// lock, and re-acquiring it deadlocks
	var owners []*cache.Resource
	for _, r := range namespaceResources {
		if r.Ref.Name == ownerReferences[0].Name &&
			r.Ref.Kind == ownerReferences[0].Kind &&
			r.Ref.APIVersion == ownerReferences[0].APIVersion {
			owners = append(owners, r)
		}
	}

	if len(owners) == 0 {
		return "", errors.New("Resource has an owner but no UID could be found")
	}

	// Be lenient if there are multiple owners
	// This is unlikely but can happen, but it's not a scenario we support in the UI yet
	if len(owners) > 1 {
		c.logger.Info("Found multiple owners for resource, only recording the first owner")
	}

	return owners[0].Ref.UID, nil
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

func (c *Cluster) getResourceTraceProperties(res *cache.Resource) []attribute.KeyValue {
	// This is a last resort recovery to ensure we don't ever bubble up a panic because of tracing
	defer func() {
		if err := recover(); err != nil {
			c.logger.Error("could not get resource trace properties", slog.Any("error", err))
		}
	}()

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

// RegisterOnResourceUpdatedEventHandler registers an event handler that will send incremental updates when resources
// change in the gitops engine cluster cache
func (c *Cluster) RegisterOnResourceUpdatedEventHandler() {
	// Don't re-register when we are already handling events for this cluster
	if c.onResourceUpdatedUnsubscribe != nil {
		return
	}

	c.onResourceUpdatedUnsubscribe = c.clusterCache.OnResourceUpdated(func(
		newRes, oldRes *cache.Resource, namespaceResources map[kube.ResourceKey]*cache.Resource,
	) {
		ctx, span := tracer.Start(context.TODO(), "Cluster.RegisterOnResourceUpdatedEventHandler")
		defer span.End()

		span.SetAttributes(attribute.String("clusterId", string(c.ClusterId)))
		span.SetAttributes(c.getResourceTraceProperties(newRes)...)

		// We don't need to lock here because the clusterCache has an internal lock that prevents concurrent handler calls
		// This prevents us locking at different levels accidentally, and causing deadlocks due to processing order
		updates, err := c.ApplicationInstances.GetChangesForUpdatedResource(
			ctx,
			c.ClusterId,
			c,
			newRes,
			oldRes,
		)
		if err != nil {
			c.logger.Error(err.Error())
		}

		for _, applicationInstanceUpdate := range updates {
			c.logApplicationInstanceChanges(
				"Sending resource updates due to updated monitored resource",
				applicationInstanceUpdate,
			)

			c.updateMonitoredResourcesFunc.Update(ctx, applicationInstanceUpdate)
		}
	})

	c.logger.With(slog.Any("clusterId", c.ClusterId)).Info("Event handler registered")
}

func (c *Cluster) InvalidateDiscoveryClient() {
	c.cachedDiscoveryClient.Invalidate()
}

// GetNamespaceMapWithHealth returns the namespaced-resource map from cluster discovery,
// along with whether discovery fully succeeded this call. When discovery fails — fully
// OR partially (a single group's discovery endpoint erroring returns ErrGroupDiscoveryFailed
// and silently drops that group's types) — the map falls back to the built-in Kubernetes
// types and healthy is false. Callers must treat a type's absence as "the CRD was deleted"
// only when healthy is true; otherwise the absence may just be a transient discovery blip.
func (c *Cluster) GetNamespaceMapWithHealth(ctx context.Context) (map[v1.TypeMeta]bool, bool) {
	ctx, span := tracer.Start(ctx, "Cluster.GetNamespaceMap")
	defer span.End()
	namespacedMap, err := c.listApiResources(ctx)
	if namespacedMap == nil || err != nil {
		// TODO: Document this behaviour as it may appear unexpected to users if they have CRDs that are not found for a time
		c.logger.With(slog.Any("error", err)).Warn("Failed to fetch API resources, using defaults")
		return getDefaultApiResources(), false
	}

	return namespacedMap, true
}

func (c *Cluster) listApiResources(ctx context.Context) (map[v1.TypeMeta]bool, error) {
	_, span := tracer.Start(ctx, "Cluster.listApiResources")
	defer span.End()

	resourceLists, err := c.cachedDiscoveryClient.ServerPreferredResources()
	if err != nil {
		return nil, err
	}

	if resourceLists == nil {
		return nil, nil
	}

	namespaced := map[v1.TypeMeta]bool{}
	span.SetAttributes(attribute.Int("resourceListCount", len(resourceLists)))

	span.AddEvent("processing fetched API resources")
	for _, resourceList := range resourceLists {
		span.SetAttributes(
			attribute.Int(fmt.Sprintf("%s-resourceListCount", resourceList.Kind), len(resourceList.APIResources)),
		)
		for _, resource := range resourceList.APIResources {
			namespaced[v1.TypeMeta{
				APIVersion: resourceList.GroupVersion,
				Kind:       resource.Kind,
			}] = resource.Namespaced
		}
	}

	return namespaced, nil
}

func (c *Cluster) GetContainerLogs(
	namespace string,
	pod string,
	container string,
	previous bool,
	ctx context.Context,
) ([]LogLine, error) {
	ctx, span := tracer.Start(ctx, "Cluster.GetContainerLogs")
	defer span.End()

	// Assume a minimum of 32 bytes per line (this accounts for just timestamps)
	tailLines := int64(maxLogSizeBytes / 32)
	req := c.clientSet.CoreV1().Pods(namespace).GetLogs(pod, &corev1.PodLogOptions{
		Container:  container,
		Previous:   previous,
		TailLines:  &tailLines,
		Timestamps: true,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return nil, err
	}

	defer func(stream io.ReadCloser) {
		deferErr := stream.Close()
		if deferErr != nil {
			c.logger.Error("Failed to close podLogs", slog.Any("error", deferErr))
		}
	}(stream)

	return parseLogs(ctx, stream)
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

func (c *Cluster) GetEvents(
	namespace string,
	name string,
	kind string,
	ctx context.Context,
) ([]Event, error) {
	ctx, span := tracer.Start(ctx, "Cluster.GetEvents")
	defer span.End()

	listOptions := v1.ListOptions{}
	listOptions.FieldSelector = fields.AndSelectors(
		fields.OneTermEqualSelector("involvedObject.kind", kind),
		fields.OneTermEqualSelector("involvedObject.name", name)).String()

	span.SetAttributes(attribute.String("namespace", namespace))
	span.SetAttributes(attribute.String("kind", kind))
	span.SetAttributes(attribute.String("name", name))

	eventList, err := c.clientSet.CoreV1().Events(namespace).List(ctx, listOptions)
	if err != nil {
		return nil, err
	}

	span.SetAttributes(attribute.Int("eventCount", len(eventList.Items)))

	events := make([]Event, 0)

	span.AddEvent("processing fetched events")
	for _, item := range eventList.Items {
		// The Kubernetes API returns event objects without a GroupVersionKind which results in an invalid manifest when marshalling
		// Borrowed from Kubectl https://github.com/kubernetes/kubectl/blob/5366de04e168bcbc11f5e340d131a9ca8b7d0df4/pkg/cmd/events/events.go#L265-L270
		if item.GetObjectKind().GroupVersionKind().Empty() {
			item.SetGroupVersionKind(schema.GroupVersionKind{
				Version: "v1",
				Kind:    "Event",
			})
		}

		rawManifest, marshalError := yaml.Marshal(item)
		if marshalError != nil {
			c.logger.Error(
				"Failed to marshal event",
				slog.Any("error", marshalError),
				slog.String("eventName", item.Name),
			)
			continue
		}

		// When dealing with some events that will never have a series
		// the event timestamps are stored in a different field
		var firstTime, lastTime time.Time
		if item.FirstTimestamp.IsZero() {
			firstTime = item.EventTime.Time
		} else {
			firstTime = item.FirstTimestamp.Time
		}
		if item.LastTimestamp.IsZero() {
			lastTime = item.EventTime.Time
		} else {
			lastTime = item.LastTimestamp.Time
		}

		// If the event has no count, it still must have at least one occurrence
		var count int32
		if item.Count == 0 {
			count = 1
		} else {
			count = item.Count
		}

		event := Event{
			FirstObservedTime:   firstTime,
			LastObservedTime:    lastTime,
			Count:               count,
			Action:              item.Action,
			Reason:              item.Reason,
			Note:                item.Message,
			ReportingController: item.ReportingController,
			ReportingInstance:   item.ReportingInstance,
			Type:                item.Type,
			Manifest:            string(rawManifest[:]),
		}

		events = append(events, event)
	}

	return events, nil
}
