package cluster

import (
	"time"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/kubernetes"
)

type DesiredResourceBuilder struct {
	id                  DesiredResourceId
	name                string
	namespace           string
	isNamespaceResolved bool
	isClusterScoped     bool
	version             *Version
	kind                string
	apiVersion          string
	manifest            *unstructured.Unstructured
	orphanedAt          *time.Time
}

func NewDesiredResourceBuilder() *DesiredResourceBuilder {
	defaultVersion := Version("v1")

	return &DesiredResourceBuilder{
		id:                  DesiredResourceId(uuid.New().String()),
		name:                "default-name",
		version:             &defaultVersion,
		kind:                "Deployment",
		apiVersion:          "apps/v1",
		isNamespaceResolved: true,
		manifest: kubernetes.NewUnstructuredBuilder().
			WithAPIVersion("apps/v1").
			WithKind("Deployment").
			WithNamespace("default").
			WithName("default-name").
			WithSpec(map[string]interface{}{
				"containers": []interface{}{
					map[string]interface{}{
						"name":  "example-container",
						"image": "nginx",
					},
				},
			}).
			Build(),
	}
}

func (b *DesiredResourceBuilder) WithId(id DesiredResourceId) *DesiredResourceBuilder {
	b.id = id
	return b
}

func (b *DesiredResourceBuilder) WithName(name string) *DesiredResourceBuilder {
	b.name = name
	return b
}

func (b *DesiredResourceBuilder) WithNamespace(namespace string) *DesiredResourceBuilder {
	b.namespace = namespace
	return b
}

func (b *DesiredResourceBuilder) WithVersion(version string) *DesiredResourceBuilder {
	typedVersion := Version(version)
	b.version = &typedVersion
	return b
}

func (b *DesiredResourceBuilder) WithoutVersion() *DesiredResourceBuilder {
	b.version = nil
	return b
}

func (b *DesiredResourceBuilder) WithKind(kind string) *DesiredResourceBuilder {
	b.kind = kind
	return b
}

func (b *DesiredResourceBuilder) WithApiVersion(apiVersion string) *DesiredResourceBuilder {
	b.apiVersion = apiVersion
	return b
}

func (b *DesiredResourceBuilder) WithManifest(manifest *unstructured.Unstructured) *DesiredResourceBuilder {
	b.manifest = manifest
	return b
}

func (b *DesiredResourceBuilder) WithClusterScoped() *DesiredResourceBuilder {
	b.isClusterScoped = true
	return b
}

func (b *DesiredResourceBuilder) WithUnresolvedNamespace() *DesiredResourceBuilder {
	b.isNamespaceResolved = false
	return b
}

func (b *DesiredResourceBuilder) WithOrphanedAt(value time.Time) *DesiredResourceBuilder {
	b.orphanedAt = &value
	return b
}

func (b *DesiredResourceBuilder) WithOrphan() *DesiredResourceBuilder {
	return b.WithOrphanedAt(time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC))
}

func (b *DesiredResourceBuilder) Build() DesiredResource {
	namespace := ""

	if b.isNamespaceResolved {
		namespace = b.namespace
	}

	return DesiredResource{
		Id: DesiredResourceId(b.id),
		Details: ResourceDetails{
			Name:      b.name,
			Namespace: namespace,
			ManifestType: v1.TypeMeta{
				Kind:       b.kind,
				APIVersion: b.apiVersion,
			},
		},
		AssumedNamespace:    b.namespace,
		IsNamespaceResolved: b.isNamespaceResolved,
		IsClusterScoped:     b.isClusterScoped,
		Version:             b.version,
		Manifest:            b.manifest,
		OrphanedAt:          b.orphanedAt,
	}
}
