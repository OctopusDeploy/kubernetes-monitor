package kwok_integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

func waitForDeployment(ctx context.Context, t *testing.T, cfg *envconf.Config, name string) appsv1.Deployment {
	var deployment appsv1.Deployment
	if err := cfg.Client().Resources().Get(ctx, name, cfg.Namespace(), &deployment); err != nil {
		t.Fatal(err)
	}

	// wait for the deployment to finish becoming available
	t.Log("Wait for deployment to become ready", deployment.Name)
	err := wait.For(
		conditions.New(cfg.Client().Resources()).
			DeploymentConditionMatch(&deployment, appsv1.DeploymentAvailable, corev1.ConditionTrue),
		wait.WithTimeout(time.Minute*1),
	)
	if err != nil {
		t.Fatal(err)
	}

	return deployment
}

func mapToDesiredResource(cfg *envconf.Config, deployment appsv1.Deployment) *cluster.DesiredResource {
	gvks, boolean, err := cfg.Client().Resources().GetScheme().ObjectKinds(&deployment)
	if err != nil || len(gvks) == 0 || len(gvks) > 1 || boolean {
		return nil
	}

	gvk := gvks[0]
	apiVersion := strings.TrimLeft(fmt.Sprintf("%s/%s", gvk.Group, gvk.Version), "/")

	desiredDeployment := cluster.NewDesiredResourceBuilder().
		WithName(deployment.Name).
		WithNamespace(deployment.Namespace).
		WithKind(gvk.Kind).
		WithApiVersion(apiVersion).
		WithManifest(&unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": apiVersion,
				"kind":       gvk.Kind,
				"metadata": map[string]interface{}{
					"name":        deployment.Name,
					"namespace":   deployment.Namespace,
					"annotations": deployment.Annotations,
				},
			},
		}).Build()

	return &desiredDeployment
}

func newDesiredDeployment(name string, namespace string) cluster.DesiredResource {
	return cluster.NewDesiredResourceBuilder().
		WithName(name).
		WithNamespace(namespace).
		WithKind("Deployment").
		WithApiVersion("apps/v1").
		WithManifest(&unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      name,
					"namespace": namespace,
				},
			},
		}).Build()
}

func createBasicDeployments(ctx context.Context, cfg *envconf.Config, t *testing.T, deploymentPrefixes []string) {
	for i := 0; i < len(deploymentPrefixes); i++ {
		name := deploymentPrefixes[i]
		createBasicDeployment(ctx, cfg, t, name)
	}
}

func createBasicDeployment(ctx context.Context, cfg *envconf.Config, t *testing.T, name string) {
	deployment := newBasicDeployment(cfg.Namespace(), name, 1)

	t.Log("Creating deployment", deployment.Name)
	if err := cfg.Client().Resources().Create(ctx, deployment); err != nil {
		t.Fatal(err)
	}
}

func createComparableResourcesForDeployment(namePrefix string, namespace string) []ComparableResource {
	return []ComparableResource{
		{Name: namePrefix, Namespace: namespace, Kind: "Deployment"},
		{Name: namePrefix, Namespace: namespace, Kind: "ReplicaSet"},
		{Name: namePrefix, Namespace: namespace, Kind: "Pod"},
	}
}

func newBasicDeployment(namespace string, name string, replicaCount int32) *appsv1.Deployment {
	podSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:  "my-container",
				Image: "nginx",
			},
		},
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "type",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"kwok"},
								},
							},
						},
					},
				},
			},
		},
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{"app": "test-app"}},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicaCount,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "test-app"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "test-app"}},
				Spec:       podSpec,
			},
		},
	}
}
