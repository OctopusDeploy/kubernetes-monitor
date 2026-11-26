package cluster

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"

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

// I'm not quite sure how it's possible, but ownership loops can occur. This asserts that we don't follow this loop infinitely
func TestGetRootParentResource(t *testing.T) {
	child := func(name string, resourceId, ownerId types.UID) (kube.ResourceKey, *ChildMonitoredResource) {
		return kube.ResourceKey{Kind: "Pod", Namespace: "ns", Name: name},
			&ChildMonitoredResource{ResourceId: resourceId, OwnerId: ownerId}
	}
	build := func(entries ...func() (kube.ResourceKey, *ChildMonitoredResource)) map[kube.ResourceKey]*ChildMonitoredResource {
		m := map[kube.ResourceKey]*ChildMonitoredResource{}
		for _, e := range entries {
			k, v := e()
			m[k] = v
		}
		return m
	}
	entry := func(name string, resourceId, ownerId types.UID) func() (kube.ResourceKey, *ChildMonitoredResource) {
		return func() (kube.ResourceKey, *ChildMonitoredResource) { return child(name, resourceId, ownerId) }
	}

	t.Run("follows chain to root", func(t *testing.T) {
		resources := build(entry("pod", "pod", "rs"), entry("rs", "rs", "deploy"))
		assert.Equal(t, types.UID("deploy"), getRootParentResource("rs", resources))
	})

	t.Run("self-owned resource terminates", func(t *testing.T) {
		resources := build(entry("a", "a", "a"))
		assert.Equal(t, types.UID("a"), getRootParentResource("a", resources))
	})

	t.Run("multi-resource cycle terminates", func(t *testing.T) {
		resources := build(entry("a", "a", "b"), entry("b", "b", "c"), entry("c", "c", "a"))
		assert.Equal(t, types.UID("c"), getRootParentResource("a", resources))
	})
}
