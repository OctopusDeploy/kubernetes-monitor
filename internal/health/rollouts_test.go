package health

import (
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
)

func TestRolloutHealthy(t *testing.T) {
	obj := pathToUnstructured(t, "./testdata/rollout-healthy.yaml")

	healthStatus := GetHealthStatus(obj)

	assertHealth(t, healthStatus, nil, health.HealthStatusHealthy)
}

func TestRolloutDegraded(t *testing.T) {
	obj := pathToUnstructured(t, "./testdata/rollout-degraded.yaml")

	healthStatus := GetHealthStatus(obj)

	assertHealth(t, healthStatus, nil, health.HealthStatusDegraded)
}

func TestRolloutProgressing(t *testing.T) {
	obj := pathToUnstructured(t, "./testdata/rollout-progressing.yaml")

	healthStatus := GetHealthStatus(obj)

	assertHealth(t, healthStatus, nil, health.HealthStatusProgressing)
}

func TestRolloutPausedIsSuspended(t *testing.T) {
	obj := pathToUnstructured(t, "./testdata/rollout-paused.yaml")

	healthStatus := GetHealthStatus(obj)

	assertHealth(t, healthStatus, nil, health.HealthStatusSuspended)
}

func TestRolloutWithNoStatusIsProgressing(t *testing.T) {
	obj := pathToUnstructured(t, "./testdata/rollout-no-status.yaml")

	healthStatus := GetHealthStatus(obj)

	assertHealth(t, healthStatus, nil, health.HealthStatusProgressing)
}
