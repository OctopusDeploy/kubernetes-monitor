package health

import (
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
	"k8s.io/apimachinery/pkg/runtime"

	appsv1 "k8s.io/api/apps/v1"
)

func TestIgnoresWhenDeploymentIsPaused(t *testing.T) {
	deployment := filePathToDeployment(t, "./testdata/deployment-suspended.yaml")

	healthStatus, err := getAppsv1DeploymentHealth(deployment)

	assertNilHealth(t, healthStatus, err)
}

func TestIgnoresIfDeploymentObservedGenerationIsOlder(t *testing.T) {
	deployment := filePathToDeployment(t, "./testdata/deployment-old-observed-generation.yaml")

	healthStatus, err := getAppsv1DeploymentHealth(deployment)

	assertNilHealth(t, healthStatus, err)
}

func TestIgnoresIfDeploymentHasAnyReadyReplicas(t *testing.T) {
	deployment := filePathToDeployment(t, "./testdata/deployment-progressing.yaml")

	healthStatus, err := getAppsv1DeploymentHealth(deployment)

	assertNilHealth(t, healthStatus, err)
}

func TestIgnoresIfDeploymentHasNoDesiredReplicas(t *testing.T) {
	deployment := filePathToDeployment(t, "./testdata/deployment-no-replicas.yaml")

	healthStatus, err := getAppsv1DeploymentHealth(deployment)

	assertNilHealth(t, healthStatus, err)
}

func TestHealthIsDegradedWhenDeploymentHasNoReadyReplicas(t *testing.T) {
	deployment := filePathToDeployment(t, "./testdata/deployment-progressing-no-ready-replicas.yaml")

	healthStatus, err := getAppsv1DeploymentHealth(deployment)

	assertHealth(t, healthStatus, err, health.HealthStatusDegraded)
}

func filePathToDeployment(t *testing.T, yamlPath string) *appsv1.Deployment {
	obj := pathToUnstructured(t, yamlPath)

	var deployment appsv1.Deployment
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &deployment)
	if err != nil {
		t.Errorf("failed to convert unstructured Deployment to typed: %v", err)
	}

	return &deployment
}
