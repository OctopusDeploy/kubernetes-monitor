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

type ChildMonitoredResource struct {
	ClusterId
	OwnerId          types.UID
	ResourceId       types.UID
	RootOwnerId      types.UID
	Status           *engineHealth.HealthStatus
	Manifest         *SanitizedManifest
	GroupVersionKind *schema.GroupVersionKind
	Name             string
	Namespace        string
	ResourceVersion  string
}

func (r *ChildMonitoredResource) ResourceKey() kube.ResourceKey {
	return kube.ResourceKey{
		Group:     r.GroupVersionKind.Group,
		Kind:      r.GroupVersionKind.Kind,
		Namespace: r.Namespace,
		Name:      r.Name,
	}
}

func NewChildMonitoredResource(
	clusterId ClusterId,
	newRes *cache.Resource,
	manifest *unstructured.Unstructured,
	ownerId types.UID,
	rootOwnerId types.UID,
	salt crypto.HashSalt,
) (*ChildMonitoredResource, error) {
	gvk := newRes.Ref.GroupVersionKind()

	healthStatus := health.GetHealthStatus(manifest)
	sanitizedManifest := sanitizeManifest(newRes, manifest, salt)
	return &ChildMonitoredResource{
		ClusterId:        clusterId,
		OwnerId:          ownerId,
		RootOwnerId:      rootOwnerId,
		Status:           healthStatus,
		GroupVersionKind: &gvk,
		Name:             newRes.Ref.Name,
		Namespace:        newRes.Ref.Namespace,
		ResourceId:       newRes.Ref.UID,
		ResourceVersion:  newRes.Ref.ResourceVersion,
		Manifest:         sanitizedManifest,
	}, nil
}
