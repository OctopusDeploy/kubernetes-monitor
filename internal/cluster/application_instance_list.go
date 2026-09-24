package cluster

import (
	"context"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"
	"k8s.io/apimachinery/pkg/runtime/schema"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
	"github.com/octopusdeploy/kubernetes-monitor/internal/utilities"
)

type ApplicationInstanceList struct {
	applicationInstances *utilities.ConcurrentMap[ApplicationInstanceId, *ApplicationInstance]
	trackedResourceKeys  *utilities.ConcurrentMap[kube.ResourceKey, bool]
}

func NewApplicationInstanceList() *ApplicationInstanceList {
	return &ApplicationInstanceList{
		applicationInstances: utilities.NewConcurrentMap[ApplicationInstanceId, *ApplicationInstance](),

		// Resource keys that are in any application instance's list of desired
		// resources or are related to those desired resources
		trackedResourceKeys: utilities.NewConcurrentMap[kube.ResourceKey, bool](),
	}
}

// UpsertApplicationInstance replaces the provided ApplicationInstance in the ApplicationInstanceList
func (l *ApplicationInstanceList) UpsertApplicationInstance(applicationInstance *ApplicationInstance) {
	l.upsertApplicationInstance(applicationInstance)
}

func (l *ApplicationInstanceList) upsertApplicationInstance(applicationInstance *ApplicationInstance) {
	l.applicationInstances.Set(applicationInstance.ApplicationInstanceId, applicationInstance)
	l.updateTrackedResourceKeys(applicationInstance)
}

func (l *ApplicationInstanceList) Iterate(
	callback func(
		applicationInstanceId ApplicationInstanceId, applicationInstance *ApplicationInstance,
	) bool,
) {
	l.applicationInstances.Iterate(callback)
}

func (l *ApplicationInstanceList) Get(applicationInstanceId ApplicationInstanceId) (*ApplicationInstance, bool) {
	val, ok := l.applicationInstances.Get(applicationInstanceId)
	return val, ok
}

// MergeDesiredResources is an intermediate between the functions of the same name on Cluster and ApplicationInstance
// This layer's responsibility is to ensure TrackedResourceKeys are added
// TODO: Can we remove ApplicationInstanceList as a concept and move it's fields up to Cluster?
func (l *ApplicationInstanceList) MergeDesiredResources(
	ctx context.Context, targetCluster *Cluster, applicationInstanceId ApplicationInstanceId, hashSalt crypto.HashSalt,
	desiredResources map[kube.ResourceKey]*DesiredResource,
) (*ApplicationInstance, error) {
	ctx, span := tracer.Start(ctx, "ApplicationInstanceList.MergeDesiredResources")
	defer span.End()
	setDesiredResourceAttributes(span, targetCluster, applicationInstanceId, desiredResources)

	applicationInstance := l.getOrCreateApplicationInstance(applicationInstanceId, hashSalt)
	applicationInstance.MergeDesiredResources(ctx, targetCluster.ClusterId, desiredResources)

	return l.rescanApplicationInstance(ctx, targetCluster, applicationInstance, desiredResources)
}

// ReplaceDesiredResources is the complete-list counterpart to MergeDesiredResources, for when the
// caller is supplying the entire desired state rather than a delta
func (l *ApplicationInstanceList) ReplaceDesiredResources(
	ctx context.Context, targetCluster *Cluster, applicationInstanceId ApplicationInstanceId, hashSalt crypto.HashSalt,
	desiredResources map[kube.ResourceKey]*DesiredResource,
) (*ApplicationInstance, error) {
	ctx, span := tracer.Start(ctx, "ApplicationInstanceList.ReplaceDesiredResources")
	defer span.End()
	setDesiredResourceAttributes(span, targetCluster, applicationInstanceId, desiredResources)

	applicationInstance := l.getOrCreateApplicationInstance(applicationInstanceId, hashSalt)
	applicationInstance.ReplaceDesiredResources(ctx, targetCluster.ClusterId, desiredResources)

	return l.rescanApplicationInstance(ctx, targetCluster, applicationInstance, desiredResources)
}

func setDesiredResourceAttributes(
	span trace.Span, targetCluster *Cluster, applicationInstanceId ApplicationInstanceId,
	desiredResources map[kube.ResourceKey]*DesiredResource,
) {
	span.SetAttributes(attribute.String("applicationInstanceId", string(applicationInstanceId)))
	span.SetAttributes(attribute.Int("desiredResourceCount", len(desiredResources)))
	span.SetAttributes(attribute.String("clusterId", string(targetCluster.ClusterId)))
}

func (l *ApplicationInstanceList) getOrCreateApplicationInstance(
	applicationInstanceId ApplicationInstanceId, hashSalt crypto.HashSalt,
) *ApplicationInstance {
	applicationInstance, found := l.Get(applicationInstanceId)
	if !found {
		applicationInstance = NewApplicationInstance(applicationInstanceId, crypto.HashSalt(hashSalt))
	}

	return applicationInstance
}

func (l *ApplicationInstanceList) rescanApplicationInstance(
	ctx context.Context, targetCluster *Cluster, applicationInstance *ApplicationInstance,
	incomingDesiredResources map[kube.ResourceKey]*DesiredResource,
) (*ApplicationInstance, error) {
	for _, desiredResource := range incomingDesiredResources {
		l.AddTrackedResourceKey(desiredResource.ResourceKey())
	}

	// getMonitoredResources assumes a synced cache; Sync no longer happens inside it so that a
	// whole-cluster sweep costs one sync rather than one per application instance.
	if err := targetCluster.Sync(); err != nil {
		return nil, err
	}

	err := applicationInstance.ReplaceMonitoredResourcesFromCluster(ctx, targetCluster)
	if err != nil {
		return nil, err
	}

	l.UpsertApplicationInstance(applicationInstance)

	return applicationInstance, nil
}

func (l *ApplicationInstanceList) GetChangesForUpdatedResource(
	ctx context.Context, clusterId ClusterId, resolver ManifestResolver,
	newRes *cache.Resource, oldRes *cache.Resource,
) ([]*ApplicationInstanceChanges, error) {
	ctx, span := tracer.Start(ctx, "ApplicationInstanceList.GetChangesForUpdatedResource")
	defer span.End()
	span.SetAttributes(attribute.String("clusterId", string(clusterId)))
	var oldKey kube.ResourceKey
	var newKey kube.ResourceKey
	if oldRes == nil || oldRes.Resource == nil {
		oldKey = kube.ResourceKey{}
	} else {
		oldKey = oldRes.ResourceKey()
	}
	if newRes == nil || newRes.Resource == nil {
		newKey = kube.ResourceKey{}
	} else {
		newKey = newRes.ResourceKey()
	}
	span.SetAttributes(attribute.String("oldRes", oldKey.String()))
	span.SetAttributes(attribute.String("newRes", newKey.String()))
	updateApplicationInstanceRequests := make([]*ApplicationInstanceChanges, 0)
	var errs error
	l.applicationInstances.Iterate(func(
		applicationInstanceId ApplicationInstanceId, applicationInstance *ApplicationInstance,
	) bool {
		ctx2, span2 := tracer.Start(ctx, "ApplicationInstanceList.GetChangesForUpdatedResource")
		defer span2.End()
		span2.SetAttributes(attribute.String("applicationInstanceId", string(applicationInstanceId)))

		updateApplicationInstanceRequest, err := applicationInstance.GetChangesForUpdatedResource(
			ctx2,
			clusterId,
			resolver,
			newRes,
			oldRes,
		)
		if err != nil {
			errs = multierr.Append(errs, err)
			return true
		}

		if updateApplicationInstanceRequest != nil {
			l.upsertApplicationInstance(applicationInstance)
			updateApplicationInstanceRequests = append(
				updateApplicationInstanceRequests,
				updateApplicationInstanceRequest,
			)
		}

		return true
	})

	return updateApplicationInstanceRequests, errs
}

func (l *ApplicationInstanceList) AddTrackedResourceKey(resourceKey kube.ResourceKey) {
	l.trackedResourceKeys.Set(resourceKey, true)
}

func (l *ApplicationInstanceList) RemoveTrackedResourceKey(resourceKey kube.ResourceKey) {
	l.trackedResourceKeys.Remove(resourceKey)
}

func (l *ApplicationInstanceList) ResourceIsTracked(resourceKey kube.ResourceKey) bool {
	return l.trackedResourceKeys.Exists(resourceKey)
}

func (l *ApplicationInstanceList) OwnerResourceIsTracked(
	resourceKey kube.ResourceKey, ownerRefs []v1.OwnerReference,
) bool {
	for _, ownerRef := range ownerRefs {
		ownerResourceKey, err := ownerRefResourceKey(ownerRef, resourceKey.Namespace)
		if err != nil {
			return false
		}

		if l.ResourceIsTracked(ownerResourceKey) {
			// Start tracking this resource immediately so we save the resource manifest in the cluster cache
			l.AddTrackedResourceKey(resourceKey)
			return true
		}
	}

	return false
}

func (l *ApplicationInstanceList) updateTrackedResourceKeys(applicationInstance *ApplicationInstance) {
	desiredResources := applicationInstance.GetDesiredResources()
	for _, res := range desiredResources {
		l.AddTrackedResourceKey(res.ResourceKey())
	}
}

func ownerRefResourceKey(ownerRef v1.OwnerReference, namespace string) (kube.ResourceKey, error) {
	gv, err := schema.ParseGroupVersion(ownerRef.APIVersion)
	return kube.NewResourceKey(gv.Group, ownerRef.Kind, namespace, ownerRef.Name), err
}
