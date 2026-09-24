package health

import (
	"fmt"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"

	appsv1 "k8s.io/api/apps/v1"
)

func getAppsv1DeploymentHealth(deployment *appsv1.Deployment) (*health.HealthStatus, error) {
	// Fall through for condition from the default health check
	if deployment.Spec.Paused {
		return nil, nil
	}

	// Condition from the default health check
	if deployment.Generation <= deployment.Status.ObservedGeneration {
		// Custom condition: when we want some replicas and have no ready replicas, we want to show a degraded state for our roll up
		if deployment.Spec.Replicas != nil && *deployment.Spec.Replicas != 0 && deployment.Status.ReadyReplicas == 0 {
			return &health.HealthStatus{
				Status:  health.HealthStatusDegraded,
				Message: fmt.Sprintf("No replicas are ready, assuming deployment is degraded"),
			}, nil
		}
	}

	return nil, nil
}
