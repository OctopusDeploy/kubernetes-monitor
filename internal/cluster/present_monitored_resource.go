package cluster

import (
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	engineHealth "github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"

	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
	"github.com/octopusdeploy/kubernetes-monitor/internal/health"
)

type PresentMonitoredResource struct {
	ClusterId
	ResourceId        types.UID
	DesiredResourceId DesiredResourceId
	Status            *engineHealth.HealthStatus
	Manifest          *SanitizedManifest
	SyncStatus        *SyncStatus
	GroupVersionKind  *schema.GroupVersionKind
	Name              string
	Namespace         string
	ResourceVersion   string
}

func (r *PresentMonitoredResource) ResourceKey() kube.ResourceKey {
	return kube.ResourceKey{
		Group:     r.GroupVersionKind.Group,
		Kind:      r.GroupVersionKind.Kind,
		Namespace: r.Namespace,
		Name:      r.Name,
	}
}

func NewPresentMonitoredResource(
	clusterId ClusterId,
	newRes *cache.Resource,
	manifest *unstructured.Unstructured,
	desiredResource *DesiredResource,
	salt crypto.HashSalt,
) (*PresentMonitoredResource, error) {
	gvk := newRes.Ref.GroupVersionKind()

	healthStatus := health.GetHealthStatus(manifest)
	sanitizedManifest := sanitizeManifest(newRes, manifest, salt)
	return &PresentMonitoredResource{
		ClusterId:         clusterId,
		DesiredResourceId: desiredResource.Id,
		Status:            healthStatus,
		SyncStatus:        getSyncStatus(sanitizedManifest, desiredResource),
		GroupVersionKind:  &gvk,
		Name:              newRes.Ref.Name,
		Namespace:         newRes.Ref.Namespace,
		ResourceId:        newRes.Ref.UID,
		ResourceVersion:   newRes.ResourceVersion,
		Manifest:          sanitizedManifest,
	}, nil
}
