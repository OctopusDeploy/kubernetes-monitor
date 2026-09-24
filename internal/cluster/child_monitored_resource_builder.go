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

type ChildMonitoredResourceBuilder struct {
	clusterId        ClusterId
	resourceId       types.UID
	owner            *metav1.OwnerReference
	rootOwner        *metav1.OwnerReference
	status           *engineHealth.HealthStatus
	manifest         *unstructured.Unstructured
	syncStatus       *SyncStatus
	groupVersionKind *schema.GroupVersionKind
	name             string
	namespace        string
	resourceVersion  string
}

func NewChildMonitoredResourceBuilder() *ChildMonitoredResourceBuilder {
	return &ChildMonitoredResourceBuilder{
		clusterId:        ClusterId("cluster-id"),
		resourceId:       types.UID(uuid.NewString()),
		owner:            nil,
		rootOwner:        nil,
		status:           nil,
		manifest:         nil,
		syncStatus:       &SyncStatus{Status: "OutOfSync", Message: ""},
		groupVersionKind: &schema.GroupVersionKind{},
		name:             "child-resource",
		namespace:        "child-resource-ns",
		resourceVersion:  "1",
	}
}

func (b *ChildMonitoredResourceBuilder) WithUID(value types.UID) *ChildMonitoredResourceBuilder {
	b.resourceId = value
	return b
}

func (b *ChildMonitoredResourceBuilder) WithOwner(value metav1.OwnerReference) *ChildMonitoredResourceBuilder {
	b.owner = &value
	return b
}

func (b *ChildMonitoredResourceBuilder) WithRootOwner(value metav1.OwnerReference) *ChildMonitoredResourceBuilder {
	b.rootOwner = &value
	return b
}

func (b *ChildMonitoredResourceBuilder) WithPresentResourceOwner(
	value PresentMonitoredResource,
) *ChildMonitoredResourceBuilder {
	apiVersion, kind := value.GroupVersionKind.ToAPIVersionAndKind()

	// Children should share namespace with it's parent
	b.namespace = value.Namespace

	b.owner = &metav1.OwnerReference{
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       value.Name,
		UID:        value.ResourceId,
	}

	b.rootOwner = &metav1.OwnerReference{
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       value.Name,
		UID:        value.ResourceId,
	}
	return b
}

func (b *ChildMonitoredResourceBuilder) WithChildResourceOwner(
	value ChildMonitoredResource,
) *ChildMonitoredResourceBuilder {
	apiVersion, kind := value.GroupVersionKind.ToAPIVersionAndKind()

	// Children should share namespace with it's parent
	b.namespace = value.Namespace

	b.owner = &metav1.OwnerReference{
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       value.Name,
		UID:        value.ResourceId,
	}

	b.rootOwner = &metav1.OwnerReference{
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       value.Name,
		UID:        value.RootOwnerId,
	}

	return b
}

func (b *ChildMonitoredResourceBuilder) WithName(value string) *ChildMonitoredResourceBuilder {
	b.name = value
	return b
}

func (b *ChildMonitoredResourceBuilder) WithNamespace(value string) *ChildMonitoredResourceBuilder {
	b.namespace = value
	return b
}

func (b *ChildMonitoredResourceBuilder) WithGroup(value string) *ChildMonitoredResourceBuilder {
	b.groupVersionKind.Group = value
	return b
}

func (b *ChildMonitoredResourceBuilder) WithKind(value string) *ChildMonitoredResourceBuilder {
	b.groupVersionKind.Kind = value
	return b
}

func (b *ChildMonitoredResourceBuilder) WithVersion(value string) *ChildMonitoredResourceBuilder {
	b.groupVersionKind.Version = value
	return b
}

func (b *ChildMonitoredResourceBuilder) Build() ChildMonitoredResource {
	if b.manifest == nil {
		apiVersion, kind := b.groupVersionKind.ToAPIVersionAndKind()
		b.manifest = kubernetes.NewUnstructuredBuilder().
			WithUID(b.resourceId).
			WithName(b.name).
			WithNamespace(b.namespace).
			WithKind(kind).
			WithAPIVersion(apiVersion).
			WithOwnerReference(*b.owner).
			Build()
	}

	return ChildMonitoredResource{
		ClusterId:        b.clusterId,
		OwnerId:          b.owner.UID,
		RootOwnerId:      b.rootOwner.UID,
		ResourceId:       b.resourceId,
		Status:           b.status,
		Manifest:         (*SanitizedManifest)(b.manifest),
		GroupVersionKind: b.groupVersionKind,
		Name:             b.name,
		Namespace:        b.namespace,
		ResourceVersion:  b.resourceVersion,
	}
}

func (r *ChildMonitoredResource) ToResource(withUpdate bool) *cache.Resource {
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
		OwnerRefs:         (*unstructured.Unstructured)(r.Manifest).GetOwnerReferences(),
		CreationTimestamp: &metav1.Time{},
		Info: ResourceInfo{
			ResourceKey: r.ResourceKey(),
			OwnerRefs:   (*unstructured.Unstructured)(r.Manifest).GetOwnerReferences(),
		},
		Resource: (*unstructured.Unstructured)(r.Manifest),
	}
}
