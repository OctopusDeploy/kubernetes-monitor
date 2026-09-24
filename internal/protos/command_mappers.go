package protos

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

// ToClusterDesiredResource maps a proto DesiredResource to a cluster.DesiredResource without any namespace resolution
func (v *DesiredResource) ToClusterDesiredResource() *cluster.DesiredResource {
	return v.toClusterDesiredResource(nil)
}

// ToClusterDesiredResourceForVersion is ToClusterDesiredResource for the one command that still
// carries a deployment version.
//
// Deprecated: retained for UpdateDesiredResourcesCommand
func (v *DesiredResource) ToClusterDesiredResourceForVersion(version *Version) *cluster.DesiredResource {
	return v.toClusterDesiredResource(version.FromProtoOrNil())
}

func (v *DesiredResource) toClusterDesiredResource(version *cluster.Version) *cluster.DesiredResource {
	apiVersion := v.ResourceDetails.GroupVersionKind.Version

	if v.ResourceDetails.GroupVersionKind.Group != "" {
		apiVersion = fmt.Sprintf(
			"%s/%s",
			v.ResourceDetails.GroupVersionKind.Group,
			v.ResourceDetails.GroupVersionKind.Version,
		)
	}

	desiredResource := &cluster.DesiredResource{
		Id: v.DesiredResourceId.FromProto(),
		Details: cluster.ResourceDetails{
			Name: v.ResourceDetails.Name,
			// Empty as we don't know if this is a namespaced resource or not until we fully ingest it
			Namespace: "",
			ManifestType: v1.TypeMeta{
				Kind:       v.ResourceDetails.GroupVersionKind.Kind,
				APIVersion: apiVersion,
			},
		},
		AssumedNamespace:    v.ResourceDetails.AssumedNamespace,
		IsNamespaceResolved: false,
		Version:             version,
		Manifest:            unmarshalToUnstructured(v.GetManifest().GetValue()),
		IgnoredFields:       nil,
		OrphanedAt:          toOrphanedAt(v.GetOrphanedAt()),
	}

	return desiredResource
}

// toOrphanedAt keeps an unset timestamp as nil rather than letting AsTime turn it into the Unix epoch,
// because presence is what distinguishes an orphan from a resource Octopus still expects.
func toOrphanedAt(orphanedAt *timestamppb.Timestamp) *time.Time {
	if orphanedAt == nil {
		return nil
	}

	asTime := orphanedAt.AsTime()

	return &asTime
}

func unmarshalToUnstructured(manifest string) *unstructured.Unstructured {
	if manifest == "" || manifest == "null" {
		return nil
	}

	var unstructuredManifest unstructured.Unstructured
	err := yaml.Unmarshal([]byte(manifest), &unstructuredManifest)
	if err != nil {
		return nil
	}

	return &unstructuredManifest
}
