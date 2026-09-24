package cluster

import (
	"log/slog"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
)

type SanitizedManifest unstructured.Unstructured

func sanitizeManifest(
	resource *cache.Resource, manifest *unstructured.Unstructured, salt crypto.HashSalt,
) *SanitizedManifest {
	// If the salt isn't populated, then don't use the manifest
	if salt == "" {
		slog.Default().Error("sanitizeManifest: salt is empty",
			slog.Any("resourceRef", resource.Ref))
		return nil
	}

	if manifest == nil {
		slog.Default().Error("sanitizeManifest: manifest is nil",
			slog.Any("resourceRef", resource.Ref))
		return nil
	}
	if !isSecret(resource) && !hasLastAppliedAnnotation(manifest) {
		return (*SanitizedManifest)(manifest)
	}

	clone := manifest.DeepCopy()

	if isSecret(resource) {
		sanitizeSecret(clone, salt)
	}

	// The last applied annotation can expose sensitive information because we cannot reliably scrub it
	// of sensitive values as it's converted to JSON.
	// We don't want to do this forever, however the fix in server will take some time to rollout.
	// This can be removed once the scrubbed has rolled out to all customers.
	if hasLastAppliedAnnotation(clone) {
		removeLastAppliedAnnotation(clone)
	}

	return (*SanitizedManifest)(clone)
}

func isSecret(resource *cache.Resource) bool {
	return resource.Ref.APIVersion == "v1" && resource.Ref.Kind == "Secret"
}

func sanitizeSecret(resource *unstructured.Unstructured, salt crypto.HashSalt) {
	d := resource.Object["data"]
	if d == nil {
		return
	}

	data := d.(map[string]interface{})
	for s := range data {
		data[s] = crypto.HashString(salt, data[s].(string))
	}
}

func hasLastAppliedAnnotation(resource *unstructured.Unstructured) bool {
	annotations := resource.GetAnnotations()
	if annotations == nil {
		return false
	}

	_, ok := annotations["kubectl.kubernetes.io/last-applied-configuration"]
	return ok
}

func removeLastAppliedAnnotation(clone *unstructured.Unstructured) {
	annotations := clone.GetAnnotations()
	if annotations == nil {
		return
	}

	delete(annotations, "kubectl.kubernetes.io/last-applied-configuration")
	clone.SetAnnotations(annotations)
}
