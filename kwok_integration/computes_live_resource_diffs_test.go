package kwok_integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	appsv1 "k8s.io/api/apps/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

// Test with deployments too (for Pod)
func TestDiffMonitoredResources(t *testing.T) {
	var desiredDeployment *appsv1.Deployment
	resourceName := "deployment-diff"

	diffFeature := features.New("Diff").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			desiredDeployment = CreateBasicDeployment(ctx, cfg, t, resourceName, GenerateData(nil))

			desiredResource := *mapToDesiredResource(cfg, *desiredDeployment)

			testCluster := createTestCluster(t, cfg)
			if err := testCluster.ReplaceDesiredResources(
				ctx, testApplicationInstanceId, desiredResourceMap(&desiredResource), testHashSalt,
			); err != nil {
				t.Fatal(err)
			}

			waitForDeployment(ctx, t, cfg, resourceName)
			return context.WithValue(ctx, testContextKey("testCluster"), testCluster)
		}).
		Assess("Unmodified: InSync", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			liveDeployment := GetMonitoredDeployment(ctx, t, cfg, resourceName)

			AssertSyncStatus(t, liveDeployment, cluster.SyncStatusInSync)

			return ctx
		}).
		Assess("Added fields: InSync", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			UpdateDeploymentAnnotations(
				ctx,
				t,
				cfg,
				resourceName,
				*GenerateData(func(data *map[string]string) { (*data)["field3"] = "poiu" }),
			)

			liveConfigMap := GetMonitoredDeployment(ctx, t, cfg, resourceName)

			AssertSyncStatus(t, liveConfigMap, cluster.SyncStatusInSync)

			return ctx
		}).
		Assess("Modified field: OutOfSync", func(
			ctx context.Context, t *testing.T, cfg *envconf.Config,
		) context.Context {
			UpdateDeploymentAnnotations(
				ctx,
				t,
				cfg,
				resourceName,
				*GenerateData(func(data *map[string]string) { (*data)["field1"] = "changed" }),
			)

			liveConfigMap := GetMonitoredDeployment(ctx, t, cfg, resourceName)

			AssertSyncStatus(t, liveConfigMap, cluster.SyncStatusOutOfSync)

			return ctx
		}).
		Assess("Removed field: OutOfSync", func(
			ctx context.Context, t *testing.T, cfg *envconf.Config,
		) context.Context {
			UpdateDeploymentAnnotations(
				ctx,
				t,
				cfg,
				resourceName,
				*GenerateData(func(data *map[string]string) { delete((*data), "field1") }),
			)

			liveConfigMap := GetMonitoredDeployment(ctx, t, cfg, resourceName)

			AssertSyncStatus(t, liveConfigMap, cluster.SyncStatusOutOfSync)

			return ctx
		}).
		Feature()

	testenv.Test(t, diffFeature)
}

func AssertSyncStatus(
	t *testing.T, presentMonitoredResource *cluster.PresentMonitoredResource, expectedStatus cluster.SyncStatusCode,
) {
	if presentMonitoredResource.SyncStatus.Status != expectedStatus {
		t.Errorf("Expected sync status to be %s: %s", expectedStatus, presentMonitoredResource.SyncStatus.Status)
	}
}

// UpdateDeploymentAnnotations retries because the deployment controller rewrites its revision annotation
// whenever the annotations are replaced, racing the update.
func UpdateDeploymentAnnotations(
	ctx context.Context, t *testing.T, cfg *envconf.Config, name string, annotations map[string]string,
) {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment := GetCurrentDeployment(ctx, t, cfg, name)
		deployment.Annotations = annotations
		return cfg.Client().Resources().Update(ctx, deployment)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func GenerateData(customizations func(*map[string]string)) *map[string]string {
	data := map[string]string{
		"field1": "unchanged1",
		"field2": "unchanged2",
	}
	if customizations != nil {
		customizations(&data)
	}

	return &data
}

func CreateBasicDeployment(
	ctx context.Context, cfg *envconf.Config, t *testing.T, name string, annotations *map[string]string,
) *appsv1.Deployment {
	deployment := newBasicDeployment(cfg.Namespace(), name, 1)
	deployment.Annotations = *annotations

	t.Log("Creating deployment", deployment.Name)
	if err := cfg.Client().Resources().Create(ctx, deployment); err != nil {
		t.Fatal(err)
	}

	return deployment
}

func GetCurrentDeployment(
	ctx context.Context, t *testing.T, cfg *envconf.Config, resourceName string,
) *appsv1.Deployment {
	var modifiedConfigMap appsv1.Deployment
	if err := cfg.Client().Resources().Get(ctx, resourceName, cfg.Namespace(), &modifiedConfigMap); err != nil {
		t.Fatal(err)
	}

	return &modifiedConfigMap
}

// GetMonitoredDeployment sweeps until the swept deployment has caught up with the API server, because
// changes reach the shared cache through its watch rather than a forced relist.
func GetMonitoredDeployment(
	ctx context.Context, t *testing.T, cfg *envconf.Config, name string,
) *cluster.PresentMonitoredResource {
	t.Helper()
	testCluster := ctx.Value(testContextKey("testCluster")).(*testTarget)

	var monitored *cluster.PresentMonitoredResource
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var live appsv1.Deployment
		require.NoError(c, cfg.Client().Resources().Get(ctx, name, cfg.Namespace(), &live))

		changes := testCluster.sweep(ctx)
		require.Len(c, changes, 1)

		monitored = findPresentResource(changes[0].PresentMonitoredResources, "Deployment", name)
		require.NotNil(c, monitored)
		require.Equal(c, live.ResourceVersion, monitored.ResourceVersion)
	}, time.Minute, 200*time.Millisecond)
	return monitored
}

func findPresentResource(
	resources []*cluster.PresentMonitoredResource, kind string, name string,
) *cluster.PresentMonitoredResource {
	for _, resource := range resources {
		if resource.GroupVersionKind.Kind == kind && resource.Name == name {
			return resource
		}
	}
	return nil
}
