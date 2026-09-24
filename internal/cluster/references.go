package cluster

import (
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func getOwnerReferences(obj *unstructured.Unstructured) []v1.OwnerReference {
	ownerRefs := obj.GetOwnerReferences()
	gvk := obj.GroupVersionKind()

	// Handle special edge case for endpoints and a few other resource types that are technically owned but don't have owner refs on the unstructured object
	// Required because the gitops-engine cache doesn't update the obj owner refs with the inferred owner refs
	// Based on https://github.com/argoproj/gitops-engine/blob/72bcdda3f0a5b80432d9f72e5b30827a530ac349/pkg/cache/references.go#L28-L57
	switch {
	// Special case for endpoint. Remove after https://github.com/kubernetes/kubernetes/issues/28483 is fixed
	case gvk.Group == "" && gvk.Kind == kube.EndpointsKind && len(ownerRefs) == 0:
		ownerRefs = append(ownerRefs, v1.OwnerReference{
			UID:        obj.GetUID(),
			Name:       obj.GetName(),
			Kind:       kube.ServiceKind,
			APIVersion: "v1",
		})

	// Special case for Operator Lifecycle Manager ClusterServiceVersion:
	case gvk.Group == "operators.coreos.com" && gvk.Kind == "ClusterServiceVersion":
		if obj.GetAnnotations()["olm.operatorGroup"] != "" {
			ownerRefs = append(ownerRefs, v1.OwnerReference{
				UID:        obj.GetUID(),
				Name:       obj.GetAnnotations()["olm.operatorGroup"],
				Kind:       "OperatorGroup",
				APIVersion: "operators.coreos.com/v1",
			})
		}

	// Edge case: consider auto-created service account tokens as a child of service account objects
	case gvk.Kind == kube.SecretKind && gvk.Group == "":
		if yes, ref := isServiceAccountTokenSecret(obj); yes {
			ownerRefs = append(ownerRefs, ref)
		}
	}

	return ownerRefs
}

func isServiceAccountTokenSecret(obj *unstructured.Unstructured) (bool, v1.OwnerReference) {
	ref := v1.OwnerReference{
		UID:        obj.GetUID(),
		APIVersion: "v1",
		Kind:       kube.ServiceAccountKind,
	}

	if typeVal, ok, err := unstructured.NestedString(obj.Object, "type"); !ok || err != nil ||
		typeVal != "kubernetes.io/service-account-token" {
		return false, ref
	}

	annotations := obj.GetAnnotations()
	if annotations == nil {
		return false, ref
	}

	id, okId := annotations["kubernetes.io/service-account.uid"]
	name, okName := annotations["kubernetes.io/service-account.name"]
	if okId && okName {
		ref.Name = name
		ref.UID = types.UID(id)
	}
	return ref.Name != "" && ref.UID != "", ref
}
