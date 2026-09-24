//go:build memory_profile || snapshot_gen

package kwok_integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type memTestConfig struct {
	deployments       int
	replicas          int32
	desiredPct        float64
	createConcurrency int
	qps               float32
	burst             int
	holdSeconds       int
}

// highQPSClient returns a kubernetes.Clientset with QPS/Burst overridden.
// e2e-framework's default client caps at ~5 QPS which makes GB-scale runs
// take hours.
func highQPSClient(t *testing.T, env *envconf.Config, cfg memTestConfig) kubernetes.Interface {
	t.Helper()
	restCfg := withQPS(env.Client().RESTConfig(), cfg)
	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		t.Fatalf("kubernetes.NewForConfig: %v", err)
	}
	return client
}

func withQPS(base *rest.Config, cfg memTestConfig) *rest.Config {
	c := rest.CopyConfig(base)
	c.QPS = cfg.qps
	c.Burst = cfg.burst
	return c
}

// createDeployments builds a full Deployment -> ReplicaSet -> Pods stack per
// workload group, bypassing kube-controller-manager so creation is bounded
// only by the apiserver's QPS. Each Deployment uses a unique selector that
// doesn't match any real pod, so the Deployment controller won't interfere.
// The ReplicaSet selector matches the Pods we create, so the RS controller
// sees a steady-state.
func createDeployments(
	ctx context.Context, t *testing.T, client kubernetes.Interface,
	namespace, namePrefix string, opts memTestConfig,
) {
	t.Helper()
	start := time.Now()

	sem := make(chan struct{}, opts.createConcurrency)
	var wg sync.WaitGroup
	var failed atomic.Int64

	for i := 0; i < opts.deployments; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			name := fmt.Sprintf("%s-%d", namePrefix, i)
			if err := createStack(ctx, client, namespace, name, opts.replicas); err != nil {
				failed.Add(1)
				t.Logf("create stack %s: %v", name, err)
			}
		}(i)
	}
	wg.Wait()

	t.Logf("created %d stacks (%d deployments + %d RSes + %d pods) in %s (%d failed)",
		opts.deployments, opts.deployments, opts.deployments,
		int(opts.replicas)*opts.deployments, time.Since(start), failed.Load())
	if failed.Load() > 0 {
		t.Fatalf("%d stack creations failed", failed.Load())
	}
}

func createStack(ctx context.Context, client kubernetes.Interface, namespace, name string, replicas int32) error {
	dClient := client.AppsV1().Deployments(namespace)
	rsClient := client.AppsV1().ReplicaSets(namespace)
	podClient := client.CoreV1().Pods(namespace)

	mgmtLabel := name + "-mgmt"
	podLabel := name + "-pod"

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{"app": name}},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(0),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": mgmtLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": mgmtLabel}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "nginx"}}},
			},
		},
	}
	createdD, err := dClient.Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("deployment: %w", err)
	}

	rsName := name + "-rs"
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsName,
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "Deployment",
				Name: createdD.Name, UID: createdD.UID, Controller: boolPtr(true), BlockOwnerDeletion: boolPtr(true),
			}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": podLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": podLabel}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "nginx"}}},
			},
		},
	}
	createdRS, err := rsClient.Create(ctx, rs, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("replicaset: %w", err)
	}

	for i := int32(0); i < replicas; i++ {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%d", rsName, i),
				Namespace: namespace,
				Labels:    map[string]string{"app": podLabel},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         "apps/v1",
						Kind:               "ReplicaSet",
						Name:               createdRS.Name,
						UID:                createdRS.UID,
						Controller:         boolPtr(true),
						BlockOwnerDeletion: boolPtr(true),
					},
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "nginx"}}},
		}
		if _, err := podClient.Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("pod %d: %w", i, err)
		}
	}
	return nil
}

func int32Ptr(v int32) *int32 { return &v }
func boolPtr(b bool) *bool    { return &b }

func sanitizeName(s string) string {
	return strings.NewReplacer("_", "-", "/", "-", ".", "-").Replace(s)
}
