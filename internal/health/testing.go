package health

import (
	"os"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

func assertHealth(t *testing.T, healthStatus *health.HealthStatus, err error, expectedStatus health.HealthStatusCode) {
	if err != nil {
		t.Errorf("Error getting health status: %v", err)
	}

	if healthStatus == nil {
		t.Fatalf("Expected non-nil health, got nil")
	}

	if healthStatus.Status != expectedStatus {
		t.Errorf("Expected health status %s, got %s", expectedStatus, healthStatus.Status)
	}
}

func assertNilHealth(t *testing.T, healthStatus *health.HealthStatus, err error) {
	if err != nil {
		t.Errorf("Error getting health status: %v", err)
	}

	if healthStatus != nil {
		t.Errorf("Expected nil health status, got %s", healthStatus.Status)
	}
}

func pathToUnstructured(t *testing.T, yamlPath string) *unstructured.Unstructured {
	yamlBytes, err := os.ReadFile(yamlPath)
	if err != nil {
		t.Errorf("Error reading file: %v", err)
	}

	var obj unstructured.Unstructured
	err = yaml.Unmarshal(yamlBytes, &obj)
	if err != nil {
		t.Errorf("Error unmarshalling yaml: %v", err)
	}

	return &obj
}
