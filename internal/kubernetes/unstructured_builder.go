package kubernetes

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type UnstructuredBuilder struct {
	obj            map[string]interface{}
	uid            types.UID
	name           string
	namespace      string
	apiVersion     string
	kind           string
	ownerReference v1.OwnerReference
}

func NewUnstructuredBuilder() *UnstructuredBuilder {
	return &UnstructuredBuilder{
		obj: map[string]interface{}{},
	}
}

func (b *UnstructuredBuilder) WithUID(value types.UID) *UnstructuredBuilder {
	b.uid = value
	return b
}

func (b *UnstructuredBuilder) WithAPIVersion(value string) *UnstructuredBuilder {
	b.apiVersion = value
	return b
}

func (b *UnstructuredBuilder) WithKind(value string) *UnstructuredBuilder {
	b.kind = value
	return b
}

func (b *UnstructuredBuilder) WithNamespace(value string) *UnstructuredBuilder {
	b.namespace = value
	return b
}

func (b *UnstructuredBuilder) WithName(name string) *UnstructuredBuilder {
	b.name = name
	return b
}

func (b *UnstructuredBuilder) WithLabels(labels map[string]string) *UnstructuredBuilder {
	if metadata, ok := b.obj["metadata"].(map[string]interface{}); ok {
		metadata["labels"] = labels
	} else {
		b.obj["metadata"] = map[string]interface{}{"labels": labels}
	}
	return b
}

func (b *UnstructuredBuilder) WithAnnotations(annotations map[string]string) *UnstructuredBuilder {
	if metadata, ok := b.obj["metadata"].(map[string]interface{}); ok {
		metadata["annotations"] = annotations
	} else {
		b.obj["metadata"] = map[string]interface{}{"annotations": annotations}
	}
	return b
}

func (b *UnstructuredBuilder) WithSpec(spec map[string]interface{}) *UnstructuredBuilder {
	b.obj["spec"] = spec
	return b
}

func (b *UnstructuredBuilder) WithOwnerReference(ownerRef v1.OwnerReference) *UnstructuredBuilder {
	b.ownerReference = ownerRef
	return b
}

func (b *UnstructuredBuilder) Build() *unstructured.Unstructured {
	un := &unstructured.Unstructured{Object: b.obj}
	un.SetUID(b.uid)
	un.SetName(b.name)
	un.SetNamespace(b.namespace)
	un.SetAPIVersion(b.apiVersion)
	un.SetKind(b.kind)
	un.SetOwnerReferences([]v1.OwnerReference{b.ownerReference})

	return un
}
