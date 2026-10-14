package cluster

import (
	"context"
	"iter"
	"maps"
	"slices"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"go.opentelemetry.io/otel/attribute"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
)

type (
	ApplicationInstanceId string
	Version               string
)

// ApplicationInstance is owned by its target's goroutine, so it isn't safe for concurrent use.
type ApplicationInstance struct {
	ApplicationInstanceId     ApplicationInstanceId
	hashSalt                  crypto.HashSalt
	desiredResources          map[kube.ResourceKey]*DesiredResource
	presentMonitoredResources map[DesiredResourceId]*PresentMonitoredResource
	childMonitoredResources   map[kube.ResourceKey]*ChildMonitoredResource
	missingMonitoredResources map[DesiredResourceId]*MissingMonitoredResource
	unknownMonitoredResources map[DesiredResourceId]*UnknownMonitoredResource

	// Resource keys that are in presentMonitoredResources and childMonitoredResources
	trackedResourceKeys map[kube.ResourceKey]struct{}
}

// NewApplicationInstance safely constructs ApplicationInstances
func NewApplicationInstance(id ApplicationInstanceId, hashSalt crypto.HashSalt) *ApplicationInstance {
	return &ApplicationInstance{
		ApplicationInstanceId:     id,
		hashSalt:                  hashSalt,
		desiredResources:          map[kube.ResourceKey]*DesiredResource{},
		presentMonitoredResources: map[DesiredResourceId]*PresentMonitoredResource{},
		childMonitoredResources:   map[kube.ResourceKey]*ChildMonitoredResource{},
		missingMonitoredResources: map[DesiredResourceId]*MissingMonitoredResource{},
		unknownMonitoredResources: map[DesiredResourceId]*UnknownMonitoredResource{},
		trackedResourceKeys:       map[kube.ResourceKey]struct{}{},
	}
}

// MergeDesiredResources upserts DesiredResources into the internal application instance state
// Conservatively set the status to unknown first, it should get overwritten later anyway, but just in case...
func (a *ApplicationInstance) MergeDesiredResources(
	ctx context.Context, clusterId ClusterId, desiredResources map[kube.ResourceKey]*DesiredResource,
) {
	_, span := tracer.Start(ctx, "ApplicationInstance.MergeDesiredResources")
	defer span.End()
	span.SetAttributes(attribute.String("applicationInstanceId", string(a.ApplicationInstanceId)))

	// Desired resources should be additively updated unless we get an explicit command to remove some
	for key, desiredResource := range desiredResources {
		a.unknownMonitoredResources[desiredResource.Id] = NewUnknownMonitoredResource(clusterId, desiredResource, "")
		a.desiredResources[key] = desiredResource
	}
}

// ReplaceDesiredResources swaps the entire DesiredResource set for this application instance.
func (a *ApplicationInstance) ReplaceDesiredResources(
	ctx context.Context, clusterId ClusterId, desiredResources map[kube.ResourceKey]*DesiredResource,
) {
	_, span := tracer.Start(ctx, "ApplicationInstance.ReplaceDesiredResources")
	defer span.End()
	span.SetAttributes(attribute.String("applicationInstanceId", string(a.ApplicationInstanceId)))

	a.desiredResources = maps.Clone(desiredResources)

	// Every monitored resource describes the set being replaced, so none of them carry over.
	// Set everything to unknown until a rescan resolves it
	unknownMonitoredResources := make(map[DesiredResourceId]*UnknownMonitoredResource, len(desiredResources))
	for _, desiredResource := range desiredResources {
		unknownMonitoredResources[desiredResource.Id] = NewUnknownMonitoredResource(clusterId, desiredResource, "")
	}

	a.replaceMonitoredResources(nil, nil, nil, unknownMonitoredResources)
}

// DeleteDesiredResourcesExceptForVersion removes desired resources in place that are not related to the
// provided version
func (a *ApplicationInstance) DeleteDesiredResourcesExceptForVersion(versionToKeep Version) {
	maps.DeleteFunc(a.desiredResources, func(_ kube.ResourceKey, resource *DesiredResource) bool {
		return resource.Version != nil && *resource.Version != versionToKeep
	})
}

// DeleteDesiredResources removes only the listed desired resource IDs from the in-memory desired
// resource map, leaving all others untouched
func (a *ApplicationInstance) DeleteDesiredResources(resourceIds []DesiredResourceId) {
	maps.DeleteFunc(a.desiredResources, func(_ kube.ResourceKey, resource *DesiredResource) bool {
		return slices.Contains(resourceIds, resource.Id)
	})
}

func (a *ApplicationInstance) GetParentResourceId(resourceInfo ResourceInfo) types.UID {
	for _, ownerRef := range resourceInfo.OwnerRefs {
		// Most of the time the owner ref will have an ID, so return the first one we find
		if ownerRef.UID != "" {
			return ownerRef.UID
		}

		ownerResourceKey, err := ownerRefResourceKey(ownerRef, resourceInfo.ResourceKey.Namespace)
		if err != nil {
			return ""
		}

		// Sometimes the owner is not correctly referenced, so we've filled in the resource information instead
		// In this case, we check if we know about the owner based off the resource key and grab the ID from there
		if desiredResource, found := a.desiredResources[ownerResourceKey]; found {
			if resource, found := a.presentMonitoredResources[desiredResource.Id]; found {
				return resource.ResourceId
			}
		}

		if resource, found := a.childMonitoredResources[ownerResourceKey]; found {
			return resource.ResourceId
		}
	}

	return ""
}

// GetRootParentResource recursively searches for the top level ownerId for the child resource provided.
func (a *ApplicationInstance) GetRootParentResource(ownerId types.UID) types.UID {
	return getRootParentResource(ownerId, a.childMonitoredResources)
}

// GetChangesForUpdatedResource generates a changeset for a single resource change event and updates
// the internal state of the ApplicationInstance to match
func (a *ApplicationInstance) GetChangesForUpdatedResource(
	ctx context.Context, clusterId ClusterId, resolver ManifestResolver,
	newRes *cache.Resource, oldRes *cache.Resource,
) (*ApplicationInstanceChanges, error) {
	ctx, span := tracer.Start(ctx, "ApplicationInstance.GetChangesForUpdatedResource")
	defer span.End()
	change := a.calculateResourceChange(ctx, newRes, oldRes)

	if change == OnResourceUpdatedNoChange {
		span.AddEvent("No change detected")
		return nil, nil
	}

	updatedPresentMonitoredResources := map[DesiredResourceId]*PresentMonitoredResource{}
	updatedMissingMonitoredResources := map[DesiredResourceId]*MissingMonitoredResource{}
	updatedChildMonitoredResources := map[kube.ResourceKey]*ChildMonitoredResource{}
	deleteChildMonitoredResourcesMap := map[kube.ResourceKey]types.UID{}
	var err error

	var newManifest *unstructured.Unstructured
	if newRes != nil && resolver != nil {
		newManifest, _ = resolver.ResolveManifest(ctx, newRes)
	}

	// Build change set + update internal ApplicationInstance state
	switch change {
	// Move missing resource to present resource
	case OnResourceUpdatedDesiredResourceFound:
		span.AddEvent("OnResourceUpdatedDesiredResourceFound")
		desiredResource := a.desiredResources[newRes.Info.(ResourceInfo).ResourceKey]
		updatedPresentMonitoredResources[desiredResource.Id], err = NewPresentMonitoredResource(
			clusterId,
			newRes,
			newManifest,
			desiredResource,
			a.hashSalt,
		)

		a.upsertPresentMonitoredResource(updatedPresentMonitoredResources[desiredResource.Id])
		a.deleteMissingMonitoredResource(desiredResource.Id)

		// Update existing present resource
	case OnResourceUpdatedDesiredResourceUpdated:
		span.AddEvent("OnResourceUpdatedDesiredResourceUpdated")
		desiredResource := a.desiredResources[newRes.Info.(ResourceInfo).ResourceKey]
		updatedPresentMonitoredResources[desiredResource.Id], err = NewPresentMonitoredResource(
			clusterId,
			newRes,
			newManifest,
			desiredResource,
			a.hashSalt,
		)

		a.upsertPresentMonitoredResource(updatedPresentMonitoredResources[desiredResource.Id])

	// Move present resource to missing resource
	case OnResourceUpdatedDesiredResourceRemoved:
		span.AddEvent("OnResourceUpdatedDesiredResourceRemoved")
		desiredResource := a.desiredResources[oldRes.Info.(ResourceInfo).ResourceKey]
		updatedMissingMonitoredResources[desiredResource.Id] = NewMissingMonitoredResource(clusterId, desiredResource)

		a.deletePresentMonitoredResource(desiredResource.Id)
		a.upsertMissingMonitoredResource(updatedMissingMonitoredResources[desiredResource.Id])

		// Set child resource (same as updating a child resource)
	case OnResourceUpdatedChildResourceFound:
		span.AddEvent("OnResourceUpdatedChildResourceFound")
		fallthrough

		// Update child resource
	case OnResourceUpdatedChildResourceUpdated:
		span.AddEvent("OnResourceUpdatedChildResourceUpdated")
		ownerId := a.GetParentResourceId(newRes.Info.(ResourceInfo))
		rootOwnerId := a.GetRootParentResource(ownerId)
		childResource, childErr := NewChildMonitoredResource(
			clusterId,
			newRes,
			newManifest,
			ownerId,
			rootOwnerId,
			a.hashSalt,
		)
		updatedChildMonitoredResources[newRes.Info.(ResourceInfo).ResourceKey], err = childResource, childErr

		a.upsertChildMonitoredResource(updatedChildMonitoredResources[newRes.Info.(ResourceInfo).ResourceKey])

		// Remove child resource
	case OnResourceUpdatedChildResourceRemoved:
		span.AddEvent("OnResourceUpdatedChildResourceRemoved")
		deleteChildMonitoredResourcesMap[oldRes.Info.(ResourceInfo).ResourceKey] = oldRes.Ref.UID

		a.deleteChildMonitoredResource(oldRes.Info.(ResourceInfo).ResourceKey)
	}

	if err != nil {
		return nil, err
	}

	// Changes to send to server
	// Server will handle any transitions between missing + present, but deleted children must be explicitly deleted
	return &ApplicationInstanceChanges{
		ApplicationInstanceId:     a.ApplicationInstanceId,
		ClusterId:                 clusterId,
		PresentMonitoredResources: slices.Collect(maps.Values(updatedPresentMonitoredResources)),
		ChildMonitoredResources:   slices.Collect(maps.Values(updatedChildMonitoredResources)),
		MissingMonitoredResources: slices.Collect(maps.Values(updatedMissingMonitoredResources)),
		DeletedChildResourceKeys:  slices.Collect(maps.Keys(deleteChildMonitoredResourcesMap)),
	}, nil
}

// ReplaceMonitoredResourcesFromCluster associates the DesiredResources from this ApplicationInstance and
// queries for related MonitoredResources in the provided cluster
// Results will be updated in the internal state of the ApplicationInstance
func (a *ApplicationInstance) ReplaceMonitoredResourcesFromCluster(
	ctx context.Context, clusterId ClusterId, cluster *sharedCluster,
) error {
	// Try to periodically resolve any namespaces that we weren't able to previously.
	// discoveryHealthy gates whether a resource whose type is absent from discovery may be
	// reported Missing (its CRD was deleted) versus kept Unknown (discovery only partially
	// succeeded, so absence isn't conclusive).
	namespacedMap, discoveryHealthy := cluster.namespaceMapWithHealth(ctx)
	a.resolveNamespaces(namespacedMap)

	present, child, missing, unknown, err := cluster.getMonitoredResources(
		clusterId,
		a.desiredResources,
		a.hashSalt,
		discoveryHealthy,
	)
	if err != nil {
		return err
	}

	a.replaceMonitoredResources(present, child, missing, unknown)
	return nil
}

func (a *ApplicationInstance) replaceMonitoredResources(
	presentMonitoredResources map[DesiredResourceId]*PresentMonitoredResource,
	childMonitoredResources map[kube.ResourceKey]*ChildMonitoredResource,
	missingMonitoredResources map[DesiredResourceId]*MissingMonitoredResource,
	unknownMonitoredResources map[DesiredResourceId]*UnknownMonitoredResource,
) {
	a.presentMonitoredResources = nonNil(presentMonitoredResources)
	a.childMonitoredResources = nonNil(childMonitoredResources)
	a.missingMonitoredResources = nonNil(missingMonitoredResources)
	a.unknownMonitoredResources = nonNil(unknownMonitoredResources)

	a.trackedResourceKeys = map[kube.ResourceKey]struct{}{}
	for _, presentMonitoredResource := range a.presentMonitoredResources {
		a.trackedResourceKeys[presentMonitoredResource.ResourceKey()] = struct{}{}
	}
	for _, childMonitoredResource := range a.childMonitoredResources {
		a.trackedResourceKeys[childMonitoredResource.ResourceKey()] = struct{}{}
	}
}

// MonitoredResourceChanges snapshots every monitored resource the application instance holds as the
// complete state to send for it.
func (a *ApplicationInstance) MonitoredResourceChanges(clusterId ClusterId) *ApplicationInstanceChanges {
	return &ApplicationInstanceChanges{
		ApplicationInstanceId:     a.ApplicationInstanceId,
		ClusterId:                 clusterId,
		PresentMonitoredResources: slices.Collect(maps.Values(a.presentMonitoredResources)),
		ChildMonitoredResources:   slices.Collect(maps.Values(a.childMonitoredResources)),
		MissingMonitoredResources: slices.Collect(maps.Values(a.missingMonitoredResources)),
		UnknownMonitoredResources: slices.Collect(maps.Values(a.unknownMonitoredResources)),
	}
}

// ResolveNamespaces loops over DesiredResources to find any do not have a resolved namespace
// and tries to resolve them
func (a *ApplicationInstance) ResolveNamespaces(namespacedMap map[v1.TypeMeta]bool) bool {
	return a.resolveNamespaces(namespacedMap)
}

func (a *ApplicationInstance) resolveNamespaces(namespacedMap map[v1.TypeMeta]bool) bool {
	allResourcesFound := true

	// Resolving a namespace can change a resource's key, so rebuild the map rather than editing it in place.
	resolved := make(map[kube.ResourceKey]*DesiredResource, len(a.desiredResources))
	for key, desiredResource := range a.desiredResources {
		if !desiredResource.ResolveNamespace(namespacedMap) {
			allResourcesFound = false
			resolved[key] = desiredResource
			continue
		}

		resolved[desiredResource.ResourceKey()] = desiredResource
	}
	a.desiredResources = resolved

	return allResourcesFound
}

type OnResourceUpdated int

const (
	OnResourceUpdatedNoChange OnResourceUpdated = iota
	OnResourceUpdatedDesiredResourceFound
	OnResourceUpdatedDesiredResourceUpdated
	OnResourceUpdatedDesiredResourceRemoved
	OnResourceUpdatedChildResourceFound
	OnResourceUpdatedChildResourceUpdated
	OnResourceUpdatedChildResourceRemoved
)

func (a *ApplicationInstance) calculateResourceChange(
	ctx context.Context, newRes *cache.Resource, oldRes *cache.Resource,
) OnResourceUpdated {
	_, span := tracer.Start(ctx, "ApplicationInstance.calculateResourceChange")
	defer span.End()
	// Changes that could occur:
	// A present resource is updated => resource is updated
	// A present resource is removed => resource is now missing
	// A missing resource is added => resource is now present
	// A child resource is removed => child resource is deleted
	// A child of a present or child resource is added => child resource is added
	// We explicitly ignore unknown resources here, the full refresh loop can figure that out.

	var newResourceDesired *DesiredResource
	var oldResourceDesired *DesiredResource

	if newRes != nil {
		newResourceDesired = a.desiredResources[newRes.Info.(ResourceInfo).ResourceKey]
	}

	if oldRes != nil {
		oldResourceDesired = a.desiredResources[oldRes.Info.(ResourceInfo).ResourceKey]
	}

	if newResourceDesired != nil {
		if _, ok := a.missingMonitoredResources[newResourceDesired.Id]; ok {
			return OnResourceUpdatedDesiredResourceFound
		}

		if _, ok := a.presentMonitoredResources[newResourceDesired.Id]; ok {
			return OnResourceUpdatedDesiredResourceUpdated
		}
	}

	if newRes != nil {
		if _, ok := a.childMonitoredResources[newRes.Info.(ResourceInfo).ResourceKey]; ok {
			return OnResourceUpdatedChildResourceUpdated
		}
	}

	if oldResourceDesired != nil {
		if _, ok := a.presentMonitoredResources[oldResourceDesired.Id]; ok {
			return OnResourceUpdatedDesiredResourceRemoved
		}
	}

	if oldRes != nil {
		if _, ok := a.childMonitoredResources[oldRes.Info.(ResourceInfo).ResourceKey]; ok {
			return OnResourceUpdatedChildResourceRemoved
		}
	}

	if newRes != nil {
		ownerRefs := newRes.Info.(ResourceInfo).OwnerRefs
		for _, ownerRef := range ownerRefs {
			ownerResourceKey, err := ownerRefResourceKey(ownerRef, newRes.Ref.Namespace)
			if err != nil {
				continue
			}

			if _, ok := a.trackedResourceKeys[ownerResourceKey]; ok {
				return OnResourceUpdatedChildResourceFound
			}
		}
	}

	return OnResourceUpdatedNoChange
}

// GetDesiredResources returns a slice of all desired resources
func (a *ApplicationInstance) GetDesiredResources() []*DesiredResource {
	return slices.Collect(maps.Values(a.desiredResources))
}

// resourceKeysOfInterest yields a desired resource under both the key it's stored by and its current key,
// which can differ around namespace resolution, so it may yield the same key more than once.
func (a *ApplicationInstance) resourceKeysOfInterest() iter.Seq[kube.ResourceKey] {
	return func(yield func(kube.ResourceKey) bool) {
		for key, desiredResource := range a.desiredResources {
			if !yield(key) || !yield(desiredResource.ResourceKey()) {
				return
			}
		}
		for key := range a.trackedResourceKeys {
			if !yield(key) {
				return
			}
		}
	}
}

func (a *ApplicationInstance) upsertPresentMonitoredResource(resource *PresentMonitoredResource) {
	a.trackedResourceKeys[resource.ResourceKey()] = struct{}{}
	a.presentMonitoredResources[resource.DesiredResourceId] = resource
}

func (a *ApplicationInstance) deletePresentMonitoredResource(desiredResourceId DesiredResourceId) {
	if presentMonitoredResource, ok := a.presentMonitoredResources[desiredResourceId]; ok {
		delete(a.trackedResourceKeys, presentMonitoredResource.ResourceKey())
		delete(a.presentMonitoredResources, desiredResourceId)
	}
}

func (a *ApplicationInstance) upsertChildMonitoredResource(resource *ChildMonitoredResource) {
	a.trackedResourceKeys[resource.ResourceKey()] = struct{}{}
	a.childMonitoredResources[resource.ResourceKey()] = resource
}

func (a *ApplicationInstance) deleteChildMonitoredResource(resourceKey kube.ResourceKey) {
	if childMonitoredResource, ok := a.childMonitoredResources[resourceKey]; ok {
		delete(a.trackedResourceKeys, childMonitoredResource.ResourceKey())
		delete(a.childMonitoredResources, resourceKey)
	}
}

func (a *ApplicationInstance) upsertMissingMonitoredResource(resource *MissingMonitoredResource) {
	a.missingMonitoredResources[resource.DesiredResourceId] = resource
}

func (a *ApplicationInstance) deleteMissingMonitoredResource(desiredResourceId DesiredResourceId) {
	delete(a.missingMonitoredResources, desiredResourceId)
}

func nonNil[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return map[K]V{}
	}
	return m
}
