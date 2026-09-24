package cluster

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	v1 "k8s.io/api/core/v1"
)

func TestSecretsScrubData(t *testing.T) {
	secretData := "super-secret-data"
	secretName := "my-secret-password"

	secretManifest := createSecret(secretName, secretData)
	resource := createResource("v1", "Secret", secretManifest)

	sanitizedManifest := sanitizeManifest(&resource, &secretManifest, "salt")

	actualSecret := sanitizedManifest.Object["data"].(map[string]interface{})[secretName]
	if actualSecret == secretData {
		t.Errorf("Expected secret to have been sanitized")
	}
}

func TestSecretsWithoutData(t *testing.T) {
	secretManifest := unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{},
		},
	}

	resource := createResource("v1", "Secret", secretManifest)

	sanitizedManifest := sanitizeManifest(&resource, &secretManifest, "salt")

	if sanitizedManifest.Object["data"] != nil {
		t.Errorf("Expected secret to be empty")
	}
}

func TestEmptySalt_Nil(t *testing.T) {
	secretData := "super-secret-data"
	secretName := "my-secret-password"

	secretManifest := createSecret(secretName, secretData)
	resource := createResource("v1", "Secret", secretManifest)

	sanitizedManifest := sanitizeManifest(&resource, &secretManifest, "")

	if sanitizedManifest != nil {
		t.Errorf("Expected manifest to be nil if salt is empty")
	}
}

func TestNilManifest_LogsAndReturnsNil(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	secretManifest := createSecret("my-secret-password", "super-secret-data")
	resource := createResource("v1", "Secret", secretManifest)

	sanitizedManifest := sanitizeManifest(&resource, nil, "salt")

	if sanitizedManifest != nil {
		t.Errorf("Expected nil manifest to produce nil result, got %+v", sanitizedManifest)
	}
	if !strings.Contains(buf.String(), "manifest is nil") {
		t.Errorf("Expected error log about nil manifest, got: %q", buf.String())
	}
}

func TestLastAppliedAnnotationRemoved(t *testing.T) {
	manifest := createResourceWithLastAppliedAnnotation()
	resource := createResource("v1", "Namespace", manifest)

	sanitizedManifest := sanitizeManifest(&resource, &manifest, "salt")

	if sanitizedManifest.Object["metadata"].(map[string]interface{})["annotations"].(map[string]interface{})["kubectl.kubernetes.io/last-applied-configuration"] != nil {
		t.Errorf("Expected last applied annotation to be removed")
	}
}

func createResource(apiVersion string, kind string, resource unstructured.Unstructured) cache.Resource {
	dd := cache.Resource{
		Ref: v1.ObjectReference{
			Kind:            kind,
			Namespace:       "my-namespace",
			Name:            "my-secret",
			UID:             "",
			APIVersion:      apiVersion,
			ResourceVersion: "",
			FieldPath:       "",
		},
		Resource: &resource,
	}
	return dd
}

func createSecret(secretName string, secretData string) unstructured.Unstructured {
	return unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{},
			"data": map[string]interface{}{
				secretName: secretData,
			},
		},
	}
}

func createResourceWithLastAppliedAnnotation() unstructured.Unstructured {
	return unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name": "my-manifest",
				"annotations": map[string]interface{}{
					"kubectl.kubernetes.io/last-applied-configuration": "some-configuration",
				},
			},
		},
	}
}
