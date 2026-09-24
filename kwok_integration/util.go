package kwok_integration

import (
	"context"
	"testing"
	"time"

	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func waitForDefaultServiceAccount(t *testing.T, cfg *envconf.Config) {
	waitForDefaultServiceAccountInNamespace(t, cfg, cfg.Namespace())
}

func waitForDefaultServiceAccountInNamespace(t *testing.T, cfg *envconf.Config, namespace string) {
	serviceAccount := corev1.ServiceAccount{
		ObjectMeta: v1.ObjectMeta{
			Name:      "default",
			Namespace: namespace,
		},
	}

	if err := cfg.Client().
		Resources().
		Get(context.Background(), serviceAccount.Name, namespace, &serviceAccount); err != nil {
		t.Logf("Waiting for default service account in namespace: %s", namespace)
	} else {
		return
	}

	err := wait.For(
		conditions.New(cfg.Client().Resources()).ResourceMatch(&serviceAccount, func(object k8s.Object) bool {
			return true
		}),
		wait.WithTimeout(1*time.Minute),
		wait.WithImmediate(),
	)
	if err != nil {
		t.Fatalf("Timed out waiting for default service account: %v", err)
	}
}
