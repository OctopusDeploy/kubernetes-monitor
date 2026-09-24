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
	kind := "Deployment"

	orphanFeature := features.New("Orphan").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			desiredDeployment = CreateBasicDeployment(ctx, cfg, t, resourceName, GenerateData(nil))

			desiredResource := *mapToDesiredResource(cfg, *desiredDeployment)
			orphanedAt := time.Now()
			desiredResource.OrphanedAt = &orphanedAt

			testCluster := createTestCluster(t, cfg)

			applicationInstance := cluster.NewApplicationInstanceBuilder().
				WithDesiredResources([]*cluster.DesiredResource{&desiredResource}).
				Build()

			testCluster.ApplicationInstances.UpsertApplicationInstance(&applicationInstance)

			waitForDeployment(ctx, t, cfg, resourceName)
			return context.WithValue(ctx, testContextKey("testCluster"), testCluster)
		}).
		Assess("Orphaned resource: Orphaned", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			liveDeployment := GetMonitoredResource(ctx, t, kind, resourceName)

			AssertSyncStatus(t, liveDeployment, cluster.SyncStatusOrphaned)

			return ctx
		}).
		Assess("Orphaned resource drifted from manifest: still Orphaned, not OutOfSync", func(
			ctx context.Context, t *testing.T, cfg *envconf.Config,
		) context.Context {
			modifiedDeployment := GetCurrentDeployment(ctx, t, cfg, resourceName)
			modifiedDeployment.Annotations = *GenerateData(func(data *map[string]string) { (*data)["field1"] = "changed" })
			UpdateDeployment(ctx, t, cfg, modifiedDeployment)

			liveDeployment := GetMonitoredResource(ctx, t, kind, resourceName)

			AssertSyncStatus(t, liveDeployment, cluster.SyncStatusOrphaned)

			return ctx
		}).
		Feature()

	testenv.Test(t, orphanFeature)
}
