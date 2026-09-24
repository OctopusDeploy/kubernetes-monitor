//go:build snapshot_gen

// Snapshot generator for the memory profile test. See memory-profile.md.

package kwok_integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Snapshots are keyed by deployments/replicas; match those to a memory test scenario.

func TestGenerateSnapshot_Smoke(t *testing.T) {
	generateSnapshot(t, memTestConfig{
		deployments:       200,
		replicas:          5,
		createConcurrency: 32,
		qps:               200,
		burst:             400,
	}, "smoke")
}

func TestGenerateSnapshot_3kDeployments15Replicas(t *testing.T) {
	generateSnapshot(t, memTestConfig{
		deployments:       3000,
		replicas:          15,
		createConcurrency: 64,
		qps:               1000,
		burst:             2000,
	}, "3k-15")
}

func generateSnapshot(t *testing.T, cfg memTestConfig, label string) {
	t.Helper()
	t.Logf("snapshot gen config: %+v", cfg)

	namePrefix := sanitizeName(fmt.Sprintf("snap-%s", strings.ToLower(label)))
	dbPath, manifestPath := snapshotPaths(cfg)

	feat := features.New("generate memory profile snapshot").
		Setup(func(ctx context.Context, t *testing.T, env *envconf.Config) context.Context {
			client := highQPSClient(t, env, cfg)
			if err := ensureNamespace(ctx, client, snapshotNamespace); err != nil {
				t.Fatalf("ensure namespace %s: %v", snapshotNamespace, err)
			}
			waitForDefaultServiceAccountInNamespace(t, env, snapshotNamespace)
			createDeployments(ctx, t, client, snapshotNamespace, namePrefix, cfg)
			return ctx
		}).
		Assess("save etcd snapshot", func(ctx context.Context, t *testing.T, env *envconf.Config) context.Context {
			if kwokClusterName == "" {
				t.Fatal("kwokClusterName is empty — TestMain did not set it")
			}

			saveCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			defer cancel()
			t.Logf("saving snapshot to %s (cluster=%s)", dbPath, kwokClusterName)
			if err := snapshotSave(saveCtx, kwokClusterName, dbPath); err != nil {
				t.Fatalf("snapshot save: %v", err)
			}

			manifest := snapshotManifest{
				Namespace:   snapshotNamespace,
				NamePrefix:  namePrefix,
				Deployments: cfg.deployments,
				Replicas:    cfg.replicas,
			}
			if err := writeSnapshotManifest(manifestPath, manifest); err != nil {
				t.Fatalf("write manifest: %v", err)
			}
			t.Logf("snapshot + manifest written: %s, %s", dbPath, manifestPath)
			return ctx
		}).
		Feature()

	testenv.Test(t, feat)
}

func ensureNamespace(ctx context.Context, client kubernetes.Interface, name string) error {
	_, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}
