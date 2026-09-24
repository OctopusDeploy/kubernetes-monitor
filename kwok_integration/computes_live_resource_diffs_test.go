package kwok_integration

import (
	"context"
	"strings"
	"testing"

	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	appsv1 "k8s.io/api/apps/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

// Test with deployments too (for Pod)
func TestDiffMonitoredResources(t *testing.T) {
	var desiredDeployment *appsv1.Deployment
	resourceName := "deployment-diff"
	kind := "Deployment"

	diffFeature := features.New("Diff").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			desiredDeployment = CreateBasicDeployment(ctx, cfg, t, resourceName, GenerateData(nil))

			desiredResource := *mapToDesiredResource(cfg, *desiredDeployment)

			testCluster := createTestCluster(t, cfg)

			applicationInstance := cluster.NewApplicationInstanceBuilder().
				WithDesiredResources([]*cluster.DesiredResource{&desiredResource}).
				Build()

			testCluster.ApplicationInstances.UpsertApplicationInstance(&applicationInstance)

			waitForDeployment(ctx, t, cfg, resourceName)
			return context.WithValue(ctx, testContextKey("testCluster"), testCluster)
		}).
		Assess("Unmodified: InSync", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			liveDeployment := GetMonitoredResource(ctx, t, kind, resourceName)

			AssertSyncStatus(t, liveDeployment, cluster.SyncStatusInSync)

			return ctx
		}).
		Assess("Added fields: InSync", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			modifiedDeployment := GetCurrentDeployment(ctx, t, cfg, resourceName)
			modifiedDeployment.Annotations = *GenerateData(func(data *map[string]string) { (*data)["field3"] = "poiu" })
			UpdateDeployment(ctx, t, cfg, modifiedDeployment)

			liveConfigMap := GetMonitoredResource(ctx, t, kind, resourceName)

			AssertSyncStatus(t, liveConfigMap, cluster.SyncStatusInSync)

			return ctx
		}).
		Assess("Modified field: OutOfSync", func(
			ctx context.Context, t *testing.T, cfg *envconf.Config,
		) context.Context {
			modifiedDeployment := GetCurrentDeployment(ctx, t, cfg, resourceName)
			modifiedDeployment.Annotations = *GenerateData(func(data *map[string]string) { (*data)["field1"] = "changed" })
			UpdateDeployment(ctx, t, cfg, modifiedDeployment)

			liveConfigMap := GetMonitoredResource(ctx, t, kind, resourceName)

			AssertSyncStatus(t, liveConfigMap, cluster.SyncStatusOutOfSync)

			return ctx
		}).
		Assess("Removed field: OutOfSync", func(
			ctx context.Context, t *testing.T, cfg *envconf.Config,
		) context.Context {
			modifiedDeployment := GetCurrentDeployment(ctx, t, cfg, resourceName)
			modifiedDeployment.Annotations = *GenerateData(func(data *map[string]string) { delete((*data), "field1") })
			UpdateDeployment(ctx, t, cfg, modifiedDeployment)

			liveConfigMap := GetMonitoredResource(ctx, t, kind, resourceName)

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

func UpdateDeployment(ctx context.Context, t *testing.T, cfg *envconf.Config, deployment *appsv1.Deployment) {
	if err := cfg.Client().Resources().Update(ctx, deployment); err != nil {
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

func GetMonitoredResource(
	ctx context.Context, t *testing.T, kind string, prefix string,
) *cluster.PresentMonitoredResource {
	testCluster := ctx.Value(testContextKey("testCluster")).(*cluster.Cluster)

	testCluster.RequestCacheRefresh()

	var applicationInstanceUpdates []cluster.ApplicationInstanceChanges
	for applicationInstanceUpdate := range testCluster.GetApplicationInstanceUpdates(context.TODO()) {
		applicationInstanceUpdates = append(applicationInstanceUpdates, *applicationInstanceUpdate)
	}

	if len(applicationInstanceUpdates) != 1 {
		t.Errorf("Expected 1 ApplicationInstanceUpdate, found %d", len(applicationInstanceUpdates))
	}

	var matchingResources []*cluster.PresentMonitoredResource

	for _, resource := range applicationInstanceUpdates[0].PresentMonitoredResources {
		if resource.GroupVersionKind.Kind == kind &&
			strings.HasPrefix(resource.Name, prefix) {
			matchingResources = append(matchingResources, resource)
		}
	}

	if len(matchingResources) != 1 {
		t.Errorf("Expected 1 matching resource for kind %s, found: %d", kind, len(matchingResources))
	}
	return matchingResources[0]
}
