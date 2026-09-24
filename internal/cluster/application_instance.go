package cluster

import (
	"context"
	"maps"
	"slices"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"go.opentelemetry.io/otel/attribute"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
	"github.com/octopusdeploy/kubernetes-monitor/internal/utilities"
)

type (
	ApplicationInstanceId string
	Version               string
)

type ApplicationInstance struct {
	ApplicationInstanceId     ApplicationInstanceId
	hashSalt                  crypto.HashSalt
	desiredResources          *utilities.ConcurrentMap[kube.ResourceKey, *DesiredResource]
	presentMonitoredResources *utilities.ConcurrentMap[DesiredResourceId, *PresentMonitoredResource]
	childMonitoredResources   *utilities.ConcurrentMap[kube.ResourceKey, *ChildMonitoredResource]
	missingMonitoredResources *utilities.ConcurrentMap[DesiredResourceId, *MissingMonitoredResource]
	unknownMonitoredResources *utilities.ConcurrentMap[DesiredResourceId, *UnknownMonitoredResource]

	// Resource keys that are in presentMonitoredResources and childMonitoredResources
	trackedResourceKeys *utilities.ConcurrentMap[kube.ResourceKey, bool]
}

// NewApplicationInstance safely constructs ApplicationInstances
func NewApplicationInstance(id ApplicationInstanceId, hashSalt crypto.HashSalt) *ApplicationInstance {
	return &ApplicationInstance{
		ApplicationInstanceId:     id,
		hashSalt:                  hashSalt,
		desiredResources:          utilities.NewConcurrentMap[kube.ResourceKey, *DesiredResource](),
		presentMonitoredResources: utilities.NewConcurrentMap[DesiredResourceId, *PresentMonitoredResource](),
		childMonitoredResources:   utilities.NewConcurrentMap[kube.ResourceKey, *ChildMonitoredResource](),
		missingMonitoredResources: utilities.NewConcurrentMap[DesiredResourceId, *MissingMonitoredResource](),
		unknownMonitoredResources: utilities.NewConcurrentMap[DesiredResourceId, *UnknownMonitoredResource](),

		// Resource keys that are in presentMonitoredResources and childMonitoredResources
		trackedResourceKeys: utilities.NewConcurrentMap[kube.ResourceKey, bool](),
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
		a.unknownMonitoredResources.Set(
			desiredResource.Id,
			NewUnknownMonitoredResource(clusterId, desiredResource, ""),
		)
		a.desiredResources.Set(key, desiredResource)
	}
}

// ReplaceDesiredResources swaps the entire DesiredResource set for this application instance.
func (a *ApplicationInstance) ReplaceDesiredResources(
	ctx context.Context, clusterId ClusterId, desiredResources map[kube.ResourceKey]*DesiredResource,
) {
	_, span := tracer.Start(ctx, "ApplicationInstance.ReplaceDesiredResources")
	defer span.End()
	span.SetAttributes(attribute.String("applicationInstanceId", string(a.ApplicationInstanceId)))

	a.desiredResources.ReplaceAll(desiredResources)

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
	a.desiredResources.Iterate(func(key kube.ResourceKey, resource *DesiredResource) bool {
		if resource.Version == nil {
			return true
		}

		if *resource.Version != versionToKeep {
			a.desiredResources.Remove(key)
		}
		return true
	})
}

// DeleteDesiredResources removes only the listed desired resource IDs from the in-memory desired
// resource map, leaving all others untouched
func (a *ApplicationInstance) DeleteDesiredResources(resourceIds []DesiredResourceId) {
	for _, id := range resourceIds {
		a.desiredResources.Iterate(func(key kube.ResourceKey, resource *DesiredResource) bool {
			if resource.Id == id {
				a.desiredResources.Remove(key)
				return false
			}
			return true
		})
	}
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
		desiredResource, found := a.desiredResources.Get(ownerResourceKey)
		if found {
			resource, found := a.presentMonitoredResources.Get(desiredResource.Id)
			if found {
				return resource.ResourceId
			}
		}

		resource, found := a.childMonitoredResources.Get(ownerResourceKey)
		if found {
			return resource.ResourceId
		}
	}

	return ""
}

// GetRootParentResource recursively searches for the top level ownerId for the child resource provided.
func (a *ApplicationInstance) GetRootParentResource(ownerId types.UID) types.UID {
	a.childMonitoredResources.Iterate(func(_ kube.ResourceKey, possibleParentResource *ChildMonitoredResource) bool {
		if ownerId == possibleParentResource.ResourceId {
			ownerId = a.GetRootParentResource(possibleParentResource.OwnerId)
			return false
		}
		return true
	})

	return ownerId
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
		desiredResource, _ := a.desiredResources.Get(newRes.Info.(ResourceInfo).ResourceKey)
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
		desiredResource, _ := a.desiredResources.Get(newRes.Info.(ResourceInfo).ResourceKey)
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
		desiredResource, _ := a.desiredResources.Get(oldRes.Info.(ResourceInfo).ResourceKey)
		delete(updatedMissingMonitoredResources, desiredResource.Id)
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
func (a *ApplicationInstance) ReplaceMonitoredResourcesFromCluster(ctx context.Context, cluster *Cluster) error {
	// Try to periodically resolve any namespaces that we weren't able to previously.
	// discoveryHealthy gates whether a resource whose type is absent from discovery may be
	// reported Missing (its CRD was deleted) versus kept Unknown (discovery only partially
	// succeeded, so absence isn't conclusive).
	namespacedMap, discoveryHealthy := cluster.GetNamespaceMapWithHealth(ctx)
	a.resolveNamespaces(namespacedMap)

	present, child, missing, unknown, err := cluster.getMonitoredResources(
		a.desiredResources.GetAsMap(),
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
	a.presentMonitoredResources.ReplaceAll(presentMonitoredResources)
	a.childMonitoredResources.ReplaceAll(childMonitoredResources)
	a.missingMonitoredResources.ReplaceAll(missingMonitoredResources)
	a.unknownMonitoredResources.ReplaceAll(unknownMonitoredResources)

	trackedResourceKeys := map[kube.ResourceKey]bool{}
	a.presentMonitoredResources.Iterate(func(
		_ DesiredResourceId, presentMonitoredResource *PresentMonitoredResource,
	) bool {
		trackedResourceKeys[presentMonitoredResource.ResourceKey()] = true
		return true
	})

	a.childMonitoredResources.Iterate(func(_ kube.ResourceKey, childMonitoredResource *ChildMonitoredResource) bool {
		trackedResourceKeys[childMonitoredResource.ResourceKey()] = true
		return true
	})

	a.trackedResourceKeys.ReplaceAll(trackedResourceKeys)
}

// MonitoredResourceChanges snapshots every monitored resource the application instance holds as the
// complete state to send for it.
func (a *ApplicationInstance) MonitoredResourceChanges(clusterId ClusterId) *ApplicationInstanceChanges {
	return &ApplicationInstanceChanges{
		ApplicationInstanceId:     a.ApplicationInstanceId,
		ClusterId:                 clusterId,
		PresentMonitoredResources: a.presentMonitoredResources.GetAll(),
		ChildMonitoredResources:   a.childMonitoredResources.GetAll(),
		MissingMonitoredResources: a.missingMonitoredResources.GetAll(),
		UnknownMonitoredResources: a.unknownMonitoredResources.GetAll(),
	}
}

// ResolveNamespaces loops over DesiredResources to find any do not have a resolved namespace
// and tries to resolve them
func (a *ApplicationInstance) ResolveNamespaces(namespacedMap map[v1.TypeMeta]bool) bool {
	return a.resolveNamespaces(namespacedMap)
}

func (a *ApplicationInstance) resolveNamespaces(namespacedMap map[v1.TypeMeta]bool) bool {
	allResourcesFound := true

	a.desiredResources.Iterate(func(key kube.ResourceKey, desiredResource *DesiredResource) bool {
		if !desiredResource.ResolveNamespace(namespacedMap) {
			allResourcesFound = false
			return true
		}

		updatedResourceKey := desiredResource.ResourceKey()
		if key != updatedResourceKey {
			a.desiredResources.Remove(key)
		}
		a.desiredResources.Set(updatedResourceKey, desiredResource)

		return true
	})

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
		newResourceDesired, _ = a.desiredResources.Get(newRes.Info.(ResourceInfo).ResourceKey)
	}

	if oldRes != nil {
		oldResourceDesired, _ = a.desiredResources.Get(oldRes.Info.(ResourceInfo).ResourceKey)
	}

	if newResourceDesired != nil {
		if _, ok := a.missingMonitoredResources.Get(newResourceDesired.Id); ok {
			return OnResourceUpdatedDesiredResourceFound
		}

		if _, ok := a.presentMonitoredResources.Get(newResourceDesired.Id); ok {
			return OnResourceUpdatedDesiredResourceUpdated
		}
	}

	if newRes != nil {
		if _, ok := a.childMonitoredResources.Get(newRes.Info.(ResourceInfo).ResourceKey); ok {
			return OnResourceUpdatedChildResourceUpdated
		}
	}

	if oldResourceDesired != nil {
		if _, ok := a.presentMonitoredResources.Get(oldResourceDesired.Id); ok {
			return OnResourceUpdatedDesiredResourceRemoved
		}
	}

	if oldRes != nil {
		if _, ok := a.childMonitoredResources.Get(oldRes.Info.(ResourceInfo).ResourceKey); ok {
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

			if _, ok := a.trackedResourceKeys.Get(ownerResourceKey); ok {
				return OnResourceUpdatedChildResourceFound
			}
		}
	}

	return OnResourceUpdatedNoChange
}

// Getters and setters, you can expose unexposed methods if needed - just be sure to consider locking

// GetDesiredResources returns a slice of all desired resources
func (a *ApplicationInstance) GetDesiredResources() []*DesiredResource {
	return a.desiredResources.GetAll()
}

func (a *ApplicationInstance) upsertPresentMonitoredResource(resource *PresentMonitoredResource) {
	a.trackedResourceKeys.Set(resource.ResourceKey(), true)
	a.presentMonitoredResources.Set(resource.DesiredResourceId, resource)
}

func (a *ApplicationInstance) deletePresentMonitoredResource(desiredResourceId DesiredResourceId) {
	presentMonitoredResource, _ := a.presentMonitoredResources.Get(desiredResourceId)

	a.trackedResourceKeys.Remove(presentMonitoredResource.ResourceKey())
	a.presentMonitoredResources.Remove(desiredResourceId)
}

func (a *ApplicationInstance) upsertChildMonitoredResource(resource *ChildMonitoredResource) {
	a.trackedResourceKeys.Set(resource.ResourceKey(), true)
	a.childMonitoredResources.Set(resource.ResourceKey(), resource)
}

func (a *ApplicationInstance) deleteChildMonitoredResource(resourceKey kube.ResourceKey) {
	childMonitoredResource, _ := a.childMonitoredResources.Get(resourceKey)
	a.trackedResourceKeys.Remove(childMonitoredResource.ResourceKey())
	a.childMonitoredResources.Remove(resourceKey)
}

func (a *ApplicationInstance) upsertMissingMonitoredResource(resource *MissingMonitoredResource) {
	a.missingMonitoredResources.Set(resource.DesiredResourceId, resource)
}

func (a *ApplicationInstance) deleteMissingMonitoredResource(desiredResourceId DesiredResourceId) {
	a.missingMonitoredResources.Remove(desiredResourceId)
}
