package cluster

import (
	"strconv"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	engineHealth "github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/kubernetes"
)

type PresentMonitoredResourceBuilder struct {
	clusterId         ClusterId
	resourceId        types.UID
	desiredResourceId DesiredResourceId
	status            *engineHealth.HealthStatus
	manifest          *SanitizedManifest
	syncStatus        *SyncStatus
	groupVersionKind  *schema.GroupVersionKind
	name              string
	namespace         string
	resourceVersion   string
}

func NewPresentMonitoredResourceBuilder() *PresentMonitoredResourceBuilder {
	return &PresentMonitoredResourceBuilder{
		clusterId:         ClusterId("cluster-id"),
		resourceId:        types.UID(uuid.NewString()),
		desiredResourceId: DesiredResourceId(types.UID(uuid.NewString())),
		status:            &engineHealth.HealthStatus{Status: "Healthy", Message: ""},
		manifest:          nil,
		syncStatus:        &SyncStatus{Status: "OutOfSync", Message: ""},
		groupVersionKind: &schema.GroupVersionKind{
			Group:   "apps",
			Version: "v1",
			Kind:    "Deployment",
		},
		name:            "present-resource",
		namespace:       "present-resource-ns",
		resourceVersion: "1",
	}
}

func (b *PresentMonitoredResourceBuilder) WithDesiredResourceId(
	value DesiredResourceId,
) *PresentMonitoredResourceBuilder {
	b.desiredResourceId = value
	return b
}

func (b *PresentMonitoredResourceBuilder) WithUID(value types.UID) *PresentMonitoredResourceBuilder {
	b.resourceId = value
	return b
}

func (b *PresentMonitoredResourceBuilder) WithName(value string) *PresentMonitoredResourceBuilder {
	b.name = value
	return b
}

func (b *PresentMonitoredResourceBuilder) WithNamespace(value string) *PresentMonitoredResourceBuilder {
	b.namespace = value
	return b
}

func (b *PresentMonitoredResourceBuilder) WithGroup(value string) *PresentMonitoredResourceBuilder {
	b.groupVersionKind.Group = value
	return b
}

func (b *PresentMonitoredResourceBuilder) WithKind(value string) *PresentMonitoredResourceBuilder {
	b.groupVersionKind.Kind = value
	return b
}

func (b *PresentMonitoredResourceBuilder) WithVersion(value string) *PresentMonitoredResourceBuilder {
	b.groupVersionKind.Version = value
	return b
}

func (b *PresentMonitoredResourceBuilder) WithManifest(
	value *unstructured.Unstructured,
) *PresentMonitoredResourceBuilder {
	b.manifest = (*SanitizedManifest)(value)
	return b
}

func (b *PresentMonitoredResourceBuilder) WithJsonPatch(value string) *PresentMonitoredResourceBuilder {
	b.syncStatus.JsonPatch = value
	return b
}

func (b *PresentMonitoredResourceBuilder) AsInSync() *PresentMonitoredResourceBuilder {
	b.syncStatus.Status = "InSync"
	return b
}

func (b *PresentMonitoredResourceBuilder) ForDesiredResource(
	desiredResource DesiredResource,
) *PresentMonitoredResourceBuilder {
	return b.
		WithDesiredResourceId(desiredResource.Id).
		WithName(desiredResource.Details.Name).
		WithNamespace(desiredResource.Details.Namespace).
		WithGroup(desiredResource.Details.ManifestType.GroupVersionKind().Group).
		WithKind(desiredResource.Details.ManifestType.Kind).
		WithVersion(desiredResource.Details.ManifestType.GroupVersionKind().Version).
		AsInSync().
		WithManifest(desiredResource.Manifest)
}

func (b *PresentMonitoredResourceBuilder) Build() PresentMonitoredResource {
	if b.manifest == nil {
		apiVersion, kind := b.groupVersionKind.ToAPIVersionAndKind()
		b.manifest = (*SanitizedManifest)(
			kubernetes.NewUnstructuredBuilder().
				WithUID(b.resourceId).
				WithName(b.name).
				WithNamespace(b.namespace).
				WithKind(kind).
				WithAPIVersion(apiVersion).
				Build(),
		)
	}

	return PresentMonitoredResource{
		ClusterId:         b.clusterId,
		ResourceId:        b.resourceId,
		DesiredResourceId: b.desiredResourceId,
		Status:            b.status,
		Manifest:          b.manifest,
		SyncStatus:        b.syncStatus,
		GroupVersionKind:  b.groupVersionKind,
		Name:              b.name,
		Namespace:         b.namespace,
		ResourceVersion:   b.resourceVersion,
	}
}

func (r *PresentMonitoredResource) ToResource(withUpdate bool) *cache.Resource {
	resourceVersion := r.ResourceVersion

	if withUpdate {
		// Resource version is normally a number, try to increment it but just put a big number otherwise
		// The spec just says different = an update
		i, err := strconv.Atoi(resourceVersion)
		if err == nil {
			resourceVersion = strconv.Itoa(i + 1)
		} else {
			resourceVersion = "1000000"
		}
	}

	apiVersion, kind := r.GroupVersionKind.ToAPIVersionAndKind()

	return &cache.Resource{
		ResourceVersion: resourceVersion,
		Ref: v1.ObjectReference{
			Kind:            kind,
			Namespace:       r.Namespace,
			Name:            r.Name,
			UID:             r.ResourceId,
			APIVersion:      apiVersion,
			ResourceVersion: resourceVersion,
		},
		OwnerRefs:         []metav1.OwnerReference{},
		CreationTimestamp: &metav1.Time{},
		Info: ResourceInfo{
			ResourceKey: r.ResourceKey(),
			OwnerRefs:   []metav1.OwnerReference{},
		},
		Resource: (*unstructured.Unstructured)(r.Manifest),
	}
}
