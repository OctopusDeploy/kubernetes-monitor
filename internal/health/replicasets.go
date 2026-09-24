package health

import (
	"fmt"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"

	appsv1 "k8s.io/api/apps/v1"
)

func getAppsv1ReplicaSetHealth(replicaSet *appsv1.ReplicaSet) (*health.HealthStatus, error) {
	// Condition from the default health check
	if replicaSet.Generation <= replicaSet.Status.ObservedGeneration {
		// Custom condition: when we want some replicas and have no ready replicas, we want to show a degraded state for our roll up
		if replicaSet.Spec.Replicas != nil && *replicaSet.Spec.Replicas != 0 && replicaSet.Status.ReadyReplicas == 0 {
			return &health.HealthStatus{
				Status:  health.HealthStatusDegraded,
				Message: fmt.Sprintf("No replicas are ready, assuming replicaset is degraded"),
			}, nil
		}
	}

	return nil, nil
}
