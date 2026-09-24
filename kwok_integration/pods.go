package kwok_integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func createBasicPod(ctx context.Context, cfg *envconf.Config, t *testing.T, name string) {
	waitForDefaultServiceAccount(t, cfg)

	pod := newBasicPod(cfg.Namespace(), name)

	t.Log("Creating pod", pod.Name)
	if err := cfg.Client().Resources().Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	waitForPod(ctx, t, cfg, name)
}

func newBasicPod(namespace string, name string) *corev1.Pod {
	podSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:  fmt.Sprintf("%s-container", name),
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
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: podSpec,
	}
}

func waitForPod(ctx context.Context, t *testing.T, cfg *envconf.Config, name string) corev1.Pod {
	var pod corev1.Pod
	if err := cfg.Client().Resources().Get(ctx, name, cfg.Namespace(), &pod); err != nil {
		t.Fatal(err)
	}

	// wait for the pod to become ready
	t.Log("Wait for pod to become ready", pod.Name)
	err := wait.For(
		conditions.New(cfg.Client().Resources()).PodReady(&pod),
		wait.WithTimeout(time.Minute*1),
		wait.WithInterval(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}

	return pod
}
