package health

import (
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
)

func TestGetHealthStatusCallsCustomHealthChecks(t *testing.T) {
	obj := pathToUnstructured(t, "./testdata/deployment-progressing-no-ready-replicas.yaml")

	healthStatus := GetHealthStatus(obj)

	assertHealth(t, healthStatus, nil, health.HealthStatusDegraded)
}

func TestGetHealthStatusCallsDefaultHealthChecksProgressing(t *testing.T) {
	obj := pathToUnstructured(t, "./testdata/deployment-progressing.yaml")

	healthStatus := GetHealthStatus(obj)

	assertHealth(t, healthStatus, nil, health.HealthStatusProgressing)
}

func TestGetHealthStatusCallsDefaultHealthChecksHealthy(t *testing.T) {
	obj := pathToUnstructured(t, "./testdata/deployment-healthy.yaml")

	healthStatus := GetHealthStatus(obj)

	assertHealth(t, healthStatus, nil, health.HealthStatusHealthy)
}
