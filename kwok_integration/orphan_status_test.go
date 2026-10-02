package kwok_integration

import (
	"context"
	"testing"
	"time"

	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	appsv1 "k8s.io/api/apps/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

func TestOrphanStatus(t *testing.T) {
	var desiredDeployment *appsv1.Deployment
	resourceName := "deployment-orphan"

	orphanFeature := features.New("Orphan").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			desiredDeployment = CreateBasicDeployment(ctx, cfg, t, resourceName, GenerateData(nil))

			desiredResource := *mapToDesiredResource(cfg, *desiredDeployment)
			orphanedAt := time.Now()
			desiredResource.OrphanedAt = &orphanedAt

			testCluster := createTestCluster(t, cfg)
			if err := testCluster.ReplaceDesiredResources(
				ctx, testApplicationInstanceId, desiredResourceMap(&desiredResource), testHashSalt,
			); err != nil {
				t.Fatal(err)
			}

			waitForDeployment(ctx, t, cfg, resourceName)
			return context.WithValue(ctx, testContextKey("testCluster"), testCluster)
		}).
		Assess("Orphaned resource: Orphaned", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			liveDeployment := GetMonitoredDeployment(ctx, t, cfg, resourceName)

			AssertSyncStatus(t, liveDeployment, cluster.SyncStatusOrphaned)

			return ctx
		}).
		Assess("Orphaned resource drifted from manifest: still Orphaned, not OutOfSync", func(
			ctx context.Context, t *testing.T, cfg *envconf.Config,
		) context.Context {
			UpdateDeploymentAnnotations(ctx, t, cfg, resourceName, *GenerateData(func(data *map[string]string) { (*data)["field1"] = "changed" }))

			liveDeployment := GetMonitoredDeployment(ctx, t, cfg, resourceName)

			AssertSyncStatus(t, liveDeployment, cluster.SyncStatusOrphaned)

			return ctx
		}).
		Feature()

	testenv.Test(t, orphanFeature)
}
