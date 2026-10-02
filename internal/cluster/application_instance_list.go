package cluster

import (
	"context"
	"iter"
	"maps"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/multierr"
	"k8s.io/apimachinery/pkg/runtime/schema"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
)

// ApplicationInstanceList is owned by its target's goroutine, so it isn't safe for concurrent use.
type ApplicationInstanceList struct {
	applicationInstances map[ApplicationInstanceId]*ApplicationInstance
}

func NewApplicationInstanceList() *ApplicationInstanceList {
	return &ApplicationInstanceList{
		applicationInstances: map[ApplicationInstanceId]*ApplicationInstance{},
	}
}

// UpsertApplicationInstance replaces the provided ApplicationInstance in the ApplicationInstanceList
func (l *ApplicationInstanceList) UpsertApplicationInstance(applicationInstance *ApplicationInstance) {
	l.applicationInstances[applicationInstance.ApplicationInstanceId] = applicationInstance
}

func (l *ApplicationInstanceList) All() iter.Seq2[ApplicationInstanceId, *ApplicationInstance] {
	return maps.All(l.applicationInstances)
}

func (l *ApplicationInstanceList) Get(applicationInstanceId ApplicationInstanceId) (*ApplicationInstance, bool) {
	applicationInstance, ok := l.applicationInstances[applicationInstanceId]
	return applicationInstance, ok
}

func (l *ApplicationInstanceList) getOrCreateApplicationInstance(
	applicationInstanceId ApplicationInstanceId, hashSalt crypto.HashSalt,
) *ApplicationInstance {
	applicationInstance, found := l.Get(applicationInstanceId)
	if !found {
		applicationInstance = NewApplicationInstance(applicationInstanceId, hashSalt)
	}

	return applicationInstance
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
	for applicationInstanceId, applicationInstance := range l.applicationInstances {
		instanceCtx, instanceSpan := tracer.Start(ctx, "ApplicationInstanceList.GetChangesForUpdatedResource")
		instanceSpan.SetAttributes(attribute.String("applicationInstanceId", string(applicationInstanceId)))

		updateApplicationInstanceRequest, err := applicationInstance.GetChangesForUpdatedResource(
			instanceCtx,
			clusterId,
			resolver,
			newRes,
			oldRes,
		)
		instanceSpan.End()
		if err != nil {
			errs = multierr.Append(errs, err)
			continue
		}

		if updateApplicationInstanceRequest != nil {
			updateApplicationInstanceRequests = append(updateApplicationInstanceRequests, updateApplicationInstanceRequest)
		}
	}

	return updateApplicationInstanceRequests, errs
}

func (l *ApplicationInstanceList) resourceKeysOfInterest() map[kube.ResourceKey]struct{} {
	keys := map[kube.ResourceKey]struct{}{}
	for _, applicationInstance := range l.applicationInstances {
		applicationInstance.addResourceKeysOfInterest(keys)
	}
	return keys
}

func ownerRefResourceKey(ownerRef v1.OwnerReference, namespace string) (kube.ResourceKey, error) {
	gv, err := schema.ParseGroupVersion(ownerRef.APIVersion)
	return kube.NewResourceKey(gv.Group, ownerRef.Kind, namespace, ownerRef.Name), err
}
