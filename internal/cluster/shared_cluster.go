package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"go.opentelemetry.io/otel/attribute"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/yaml"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
)

const permissionFilterRefreshInterval = 1 * time.Minute

// sharedCluster exists because Octopus can send many machine IDs for the same cluster, so they all share one
// cache, client set and discovery state. Nothing here changes after construction, and the cache and clients
// synchronise themselves, so targets use it concurrently without coordinating.
type sharedCluster struct {
	logger *slog.Logger

	clusterCache        Cache
	resourceFilter      kube.ResourceFilter
	clusterServer       string
	namespaceScopedMode bool // true when targetNamespaces is set and clusterScopedResources is false
	targetNamespaces    map[string]struct{}

	cachedDiscoveryClient discovery.CachedDiscoveryInterface
	clientSet             kubernetes.Interface
	dynamicClient         dynamic.Interface

	interests *resourceInterests
}

// ClusterConnection is what connecting to the Kubernetes cluster builds.
type ClusterConnection struct {
	Cache                  Cache
	ResourceFilter         kube.ResourceFilter
	Discovery              discovery.CachedDiscoveryInterface
	ClientSet              kubernetes.Interface
	DynamicClient          dynamic.Interface
	TargetNamespaces       []string
	ClusterScopedResources bool
}

func (c ClusterConnection) sharedCluster(logger *slog.Logger, interests *resourceInterests) *sharedCluster {
	targetNamespaces := make(map[string]struct{}, len(c.TargetNamespaces))
	for _, namespace := range c.TargetNamespaces {
		targetNamespaces[namespace] = struct{}{}
	}

	return &sharedCluster{
		logger:                logger,
		clusterCache:          c.Cache,
		resourceFilter:        c.ResourceFilter,
		clusterServer:         c.Cache.GetClusterInfo().Server,
		namespaceScopedMode:   len(c.TargetNamespaces) > 0 && !c.ClusterScopedResources,
		targetNamespaces:      targetNamespaces,
		cachedDiscoveryClient: c.Discovery,
		clientSet:             c.ClientSet,
		dynamicClient:         c.DynamicClient,
		interests:             interests,
	}
}

func newSharedCluster(
	ctx context.Context,
	logger *slog.Logger,
	config *rest.Config,
	targetNamespaces []string,
	clusterScopedResources bool,
) (*sharedCluster, error) {
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	interests := newResourceInterests(ctx, logger)
	clusterCache, resourceFilter, err := newCache(
		ctx,
		logger,
		config,
		clientSet,
		interests,
		targetNamespaces,
		clusterScopedResources,
		permissionFilterRefreshInterval,
	)
	if err != nil {
		return nil, err
	}

	// Invalidating stops the cache's watches, which otherwise run for as long as the process does.
	context.AfterFunc(ctx, func() { clusterCache.Invalidate() })

	connection := ClusterConnection{
		Cache:                  clusterCache,
		ResourceFilter:         resourceFilter,
		Discovery:              memory.NewMemCacheClient(clientSet.Discovery()),
		ClientSet:              clientSet,
		DynamicClient:          dynamicClient,
		TargetNamespaces:       targetNamespaces,
		ClusterScopedResources: clusterScopedResources,
	}
	return connection.sharedCluster(logger, interests), nil
}

// sync is shared by every target waiting on the first sync, because the cache serialises concurrent callers.
func (c *sharedCluster) sync() error {
	c.logger.Info("Syncing cluster",
		slog.Any("LastCacheSyncTime", c.clusterCache.GetClusterInfo().LastCacheSyncTime))

	return c.clusterCache.EnsureSynced()
}

func (c *sharedCluster) invalidateDiscovery() {
	c.cachedDiscoveryClient.Invalidate()
}

// ResolveManifest returns the manifest stored on the cache.Resource or
// fetches it live.
func (c *sharedCluster) ResolveManifest(ctx context.Context, res *cache.Resource) (*unstructured.Unstructured, error) {
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
func (c *sharedCluster) lookupGVR(gvk schema.GroupVersionKind) (schema.GroupVersionResource, bool, error) {
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

// namespaceMapWithHealth returns the namespaced-resource map from cluster discovery,
// along with whether discovery fully succeeded this call. When discovery fails — fully
// OR partially (a single group's discovery endpoint erroring returns ErrGroupDiscoveryFailed
// and silently drops that group's types) — the map falls back to the built-in Kubernetes
// types and healthy is false. Callers must treat a type's absence as "the CRD was deleted"
// only when healthy is true; otherwise the absence may just be a transient discovery blip.
func (c *sharedCluster) namespaceMapWithHealth(ctx context.Context) (map[v1.TypeMeta]bool, bool) {
	ctx, span := tracer.Start(ctx, "sharedCluster.namespaceMapWithHealth")
	defer span.End()
	namespacedMap, err := c.listApiResources(ctx)
	if namespacedMap == nil || err != nil {
		// TODO: Document this behaviour as it may appear unexpected to users if they have CRDs that are not found for a time
		c.logger.With(slog.Any("error", err)).Warn("Failed to fetch API resources, using defaults")
		return getDefaultApiResources(), false
	}

	return namespacedMap, true
}

func (c *sharedCluster) listApiResources(ctx context.Context) (map[v1.TypeMeta]bool, error) {
	_, span := tracer.Start(ctx, "sharedCluster.listApiResources")
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

func (c *sharedCluster) getMonitoredResources(
	clusterId ClusterId,
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
					clusterId,
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
				clusterId,
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
				clusterId,
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
					clusterId,
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
					clusterId,
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
				clusterId,
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
				clusterId,
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
						clusterId,
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

			missingMonitoredResources[desiredResource.Id] = NewMissingMonitoredResource(clusterId, desiredResource)
		}
	}

	return presentMonitoredResources, childMonitoredResources, missingMonitoredResources, unknownMonitoredResources, nil
}

func (c *sharedCluster) getFirstOwnerId(
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

func (c *sharedCluster) containerLogs(
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

func (c *sharedCluster) events(
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
