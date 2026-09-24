package cluster

import (
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DesiredResourceId string

type DesiredResource struct {
	Id                  DesiredResourceId // ID of the desired resource defined by server
	Details             ResourceDetails   // Unique properties of a resource to be compared
	AssumedNamespace    string            // We cannot be sure that a resource is namespaced until we check with the k8s server
	IsNamespaceResolved bool              // Starts as false when the Kubernetes monitor receives this and is only changed to true when we have evaluated if NamespaceIfNamespacedResource is what we want
	IsClusterScoped     bool              // Set during namespace resolution, true when the K8s API confirms this resource type is not namespaced

	// Deprecated: only DeleteDesiredResourcesExceptForVersion reads this, serving PruneOtherVersionsCommand, which Server no longer sends.
	// Nil for resources that arrived on a command carrying no version.
	Version *Version // ID of the deployment that this resource was last applied in.

	Manifest      *unstructured.Unstructured // Manifest is a unstructured representation of the deployed manifest
	IgnoredFields []string                   // IgnoredFields is a list of fields to ignore when comparing manifests
	OrphanedAt    *time.Time                 // When Octopus stopped expecting this resource, or nil while it is still expected
}

// IsOrphaned reports whether Octopus has stopped expecting this resource, which the monitor reports back
// in place of the resource's sync status.
func (d *DesiredResource) IsOrphaned() bool {
	return d.OrphanedAt != nil
}

type ResourceDetails struct {
	Name         string      // Name of the kubernetes item
	Namespace    string      // Non-namespaced items represented with an empty string
	ManifestType v1.TypeMeta // Kind and ApiVersion
}

func CreateResourceDetailsFromOwnerRef(dependantNamespace string, ownerRef v1.OwnerReference) ResourceDetails {
	return ResourceDetails{
		Name:      ownerRef.Name,
		Namespace: dependantNamespace,
		ManifestType: v1.TypeMeta{
			Kind:       ownerRef.Kind,
			APIVersion: ownerRef.APIVersion,
		},
	}
}

func (d *DesiredResource) ResourceKey() kube.ResourceKey {
	gvk := d.Details.ManifestType.GroupVersionKind()

	// If the namespace has not been resolved, we use the assumed namespace to avoid collisions
	// In this case extra desired resources are preferable to missing desired resources
	if !d.IsNamespaceResolved {
		return kube.ResourceKey{Group: gvk.Group, Kind: gvk.Kind, Namespace: d.AssumedNamespace, Name: d.Details.Name}
	}

	return kube.ResourceKey{Group: gvk.Group, Kind: gvk.Kind, Namespace: d.Details.Namespace, Name: d.Details.Name}
}

func (d *DesiredResource) ResolveNamespace(namespacedMap map[v1.TypeMeta]bool) bool {
	if !d.IsNamespaceResolved {
		isNamespaced, found := namespacedMap[d.Details.ManifestType]
		if !found {
			return false
		}

		if isNamespaced {
			d.Details.Namespace = d.AssumedNamespace
			// Ensure the manifest has the namespace set so gitops-engine's GetManagedLiveObjs
			// doesn't reject it. When the cache is in namespace-scoped mode
			// (SetNamespaces + SetClusterResources(false)), GetManagedLiveObjs returns a hard error
			// for any manifest with an empty .metadata.namespace.
			if d.Manifest != nil && d.Manifest.GetNamespace() == "" && d.AssumedNamespace != "" {
				d.Manifest.SetNamespace(d.AssumedNamespace)
			}
		} else {
			d.IsClusterScoped = true
		}

		d.IsNamespaceResolved = true
	}

	return true
}
