package cluster

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/octopusdeploy/kubernetes-monitor/internal/kubernetes"
)

func TestLogParse(t *testing.T) {
	reader, err := os.Open("../../testdata/longlog.txt")
	if err != nil {
		t.Errorf("failed to open cluster.go: %v", err)
		t.Fail()
	}
	defer reader.Close()

	logs, err := parseLogs(context.TODO(), reader)
	if err != nil {
		t.Errorf("failed to parse logs: %v", err)
		t.Fail()
	}

	if len(logs) != 149796 {
		t.Errorf("expected 149796 log lines, got %d", len(logs))
		t.Fail()
	}
}

func Test_getSyncStatus(t *testing.T) {
	// Arrange
	desiredUnstructured := kubernetes.NewUnstructuredBuilder().
		WithName("deployment").
		WithNamespace("test").
		WithKind("Deployment").
		WithAPIVersion("apps/v1").
		WithLabels(map[string]string{
			"app": "expected",
		}).
		WithAnnotations(map[string]string{
			"app": "expected",
		}).
		Build()

	changedUnstructured := kubernetes.NewUnstructuredBuilder().
		WithName("deployment").
		WithNamespace("test").
		WithKind("Deployment").
		WithAPIVersion("apps/v1").
		WithLabels(map[string]string{
			"app":  "unexpected",
			"app2": "test2",
		}).
		WithAnnotations(map[string]string{
			"app": "unexpected",
		}).
		Build()

	desired := NewDesiredResourceBuilder().WithManifest(desiredUnstructured).Build()

	// Act
	syncStatus := getSyncStatus((*SanitizedManifest)(changedUnstructured), &desired)

	// Assert
	var jsonPatch interface{}
	err := json.Unmarshal([]byte(syncStatus.JsonPatch), &jsonPatch)
	if err != nil {
		t.Errorf("failed to unmarshal JsonPatch: %v", err)
	}

	expected := []interface{}{
		map[string]interface{}{
			"op":    "replace",
			"path":  "/metadata/annotations/app",
			"value": "expected",
		},
		map[string]interface{}{
			"op":    "replace",
			"path":  "/metadata/labels/app",
			"value": "expected",
		},
	}

	assert.Equal(t, syncStatus.Status, SyncStatusOutOfSync)
	assert.ElementsMatch(t, jsonPatch, expected)
}

func Test_getSyncStatus_NilSanitizedManifest(t *testing.T) {
	desiredUnstructured := kubernetes.NewUnstructuredBuilder().
		WithName("deployment").WithNamespace("test").
		WithKind("Deployment").WithAPIVersion("apps/v1").
		Build()
	desired := NewDesiredResourceBuilder().WithManifest(desiredUnstructured).Build()

	syncStatus := getSyncStatus(nil, &desired)

	assert.Equal(t, SyncStatusUnknown, syncStatus.Status)
	assert.Equal(t, "Missing resource manifest", syncStatus.Message)
}

func Test_getSyncStatus_NilDesiredManifest(t *testing.T) {
	liveUnstructured := kubernetes.NewUnstructuredBuilder().
		WithName("deployment").WithNamespace("test").
		WithKind("Deployment").WithAPIVersion("apps/v1").
		Build()
	desired := DesiredResource{Id: "id", Manifest: nil}

	syncStatus := getSyncStatus((*SanitizedManifest)(liveUnstructured), &desired)

	assert.Equal(t, SyncStatusUnknown, syncStatus.Status)
	assert.Equal(t, "Missing desired resource manifest", syncStatus.Message)
}
