package health

import (
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
	"k8s.io/apimachinery/pkg/runtime"

	appsv1 "k8s.io/api/apps/v1"
)

func TestIgnoresIfReplicaSetObservedGenerationIsOlder(t *testing.T) {
	replicaSet := filePathToReplicaSet(t, "./testdata/replicaset-old-observed-generation.yaml")

	healthStatus, err := getAppsv1ReplicaSetHealth(replicaSet)

	assertNilHealth(t, healthStatus, err)
}

func TestIgnoresIfReplicaSetHasAnyReadyReplicas(t *testing.T) {
	replicaSet := filePathToReplicaSet(t, "./testdata/replicaset-progressing.yaml")

	healthStatus, err := getAppsv1ReplicaSetHealth(replicaSet)

	assertNilHealth(t, healthStatus, err)
}

func TestIgnoresIfReplicaSetHasNoDesiredReplicas(t *testing.T) {
	replicaSet := filePathToReplicaSet(t, "./testdata/replicaset-no-replicas.yaml")

	healthStatus, err := getAppsv1ReplicaSetHealth(replicaSet)

	assertNilHealth(t, healthStatus, err)
}

func TestHealthIsDegradedWhenReplicaSetHasNoReadyReplicas(t *testing.T) {
	replicaSet := filePathToReplicaSet(t, "./testdata/replicaset-progressing-no-ready-replicas.yaml")

	healthStatus, err := getAppsv1ReplicaSetHealth(replicaSet)

	assertHealth(t, healthStatus, err, health.HealthStatusDegraded)
}

func filePathToReplicaSet(t *testing.T, yamlPath string) *appsv1.ReplicaSet {
	obj := pathToUnstructured(t, yamlPath)

	var replicaSet appsv1.ReplicaSet
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &replicaSet)
	if err != nil {
		t.Errorf("failed to convert unstructured ReplicaSet to typed: %v", err)
	}

	return &replicaSet
}
