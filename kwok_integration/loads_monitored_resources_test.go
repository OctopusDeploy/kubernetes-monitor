package kwok_integration

import (
	"context"
	"strings"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

func TestLoadsMonitoredResources(t *testing.T) {
	deploymentPrefixes := []string{"deployment-1", "deployment-2"}

	// The default namespace always exists and is never the randomly named namespace this feature
	// monitors, so a resource in it is reliably outside the monitored set.
	const unmonitoredNamespace = "default"

	deploymentFeature := features.New("appsv1/deployment").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			createBasicDeployments(ctx, cfg, t, deploymentPrefixes)

			desiredDeployments := make([]*cluster.DesiredResource, 0)
			for i := 0; i < len(deploymentPrefixes); i++ {
				deployment := waitForDeployment(ctx, t, cfg, deploymentPrefixes[i])
				desiredDeployment := mapToDesiredResource(cfg, deployment)

				if desiredDeployment == nil {
					t.Fatal("Failed to map desired resource")
				} else {
					desiredDeployments = append(desiredDeployments, desiredDeployment)
				}
			}

			applicationInstance := cluster.NewApplicationInstanceBuilder().
				WithDesiredResources(desiredDeployments).
				Build()

			testCluster := createTestCluster(t, cfg)
			testCluster.ApplicationInstances.UpsertApplicationInstance(&applicationInstance)

			return context.WithValue(ctx, testContextKey("testCluster"), testCluster)
		}).
		Assess("loads present and children resources", func(
			ctx context.Context, t *testing.T, cfg *envconf.Config,
		) context.Context {
			testCluster := ctx.Value(testContextKey("testCluster")).(*cluster.Cluster)

			_ = testCluster.Sync()

			var applicationInstanceUpdates []cluster.ApplicationInstanceChanges
			for applicationInstanceUpdate := range testCluster.GetApplicationInstanceUpdates(context.TODO()) {
				applicationInstanceUpdates = append(applicationInstanceUpdates, *applicationInstanceUpdate)
			}

			if len(applicationInstanceUpdates) != 1 {
				t.Fatalf("Expected 1 ApplicationInstanceUpdate, found %d", len(applicationInstanceUpdates))
			}

			expectedResources := append(
				createComparableResourcesForDeployment(deploymentPrefixes[0], cfg.Namespace()),
				createComparableResourcesForDeployment(deploymentPrefixes[1], cfg.Namespace())...,
			)

			if applicationInstanceUpdates[0].GetAllResourceCount() != len(expectedResources) {
				t.Errorf(
					"Expected %d total resources, found %d",
					len(expectedResources),
					applicationInstanceUpdates[0].GetAllResourceCount(),
				)
			}

			if getPresentAndChildResourceCount(applicationInstanceUpdates[0]) != len(expectedResources) {
				t.Errorf(
					"Expected %d present and child resources, found %d",
					len(expectedResources),
					getPresentAndChildResourceCount(applicationInstanceUpdates[0]),
				)
			}

			actualResources := createComparableResourcesForApplicationInstanceUpdate(applicationInstanceUpdates[0])

			if diff := cmp.Diff(
				expectedResources,
				actualResources,
				cmpopts.SortSlices(SortComparableResource),
			); diff != "" {
				t.Error(diff)
			}

			return ctx
		}).
		Assess("loads missing resources", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			testCluster := ctx.Value(testContextKey("testCluster")).(*cluster.Cluster)

			missingDesiredResource := cluster.NewDesiredResourceBuilder().Build()

			testCluster.ApplicationInstances.Iterate(func(
				applicationInstanceId cluster.ApplicationInstanceId,
				updatedApplicationInstance *cluster.ApplicationInstance,
			) bool {
				updatedApplicationInstance.MergeDesiredResources(
					context.TODO(),
					testCluster.ClusterId,
					map[kube.ResourceKey]*cluster.DesiredResource{
						missingDesiredResource.ResourceKey(): &missingDesiredResource,
					},
				)
				testCluster.ApplicationInstances.UpsertApplicationInstance(updatedApplicationInstance)
				return true
			})

			testCluster.RequestCacheRefresh()

			var applicationInstanceUpdates []cluster.ApplicationInstanceChanges
			for applicationInstanceUpdate := range testCluster.GetApplicationInstanceUpdates(context.TODO()) {
				applicationInstanceUpdates = append(applicationInstanceUpdates, *applicationInstanceUpdate)
			}

			if len(applicationInstanceUpdates) != 1 {
				t.Fatalf("Expected 1 ApplicationInstanceUpdate, found %d", len(applicationInstanceUpdates))
			}

			expectedMissingResources := []ComparableResource{
				{
					Name: missingDesiredResource.Details.Name, Namespace: missingDesiredResource.Details.Namespace,
					Kind: missingDesiredResource.Details.ManifestType.Kind,
				},
			}

			if len(applicationInstanceUpdates[0].MissingMonitoredResources) != len(expectedMissingResources) {
				t.Errorf(
					"Expected %d missing resources, found %d",
					len(expectedMissingResources),
					len(applicationInstanceUpdates[0].MissingMonitoredResources),
				)
			}

			actualMissingResource := createComparableResourcesForMissingResources(
				applicationInstanceUpdates[0].MissingMonitoredResources,
			)

			if diff := cmp.Diff(
				expectedMissingResources,
				actualMissingResource,
				cmpopts.SortSlices(SortComparableResource),
			); diff != "" {
				t.Error(diff)
			}

			return ctx
		}).
		Assess("loads unknown resources", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			// Unknown is only reachable through a classification branch that runs after
			// getMonitoredResources checks whether the resource's type is still known to the
			// cluster: a type the cluster has never heard of is reported Missing, not Unknown. So
			// this resource is an ordinary Deployment whose namespace resolves normally, and what
			// makes it Unknown is that its namespace sits outside the monitored set.
			//
			// It gets its own cluster because restricting the shared one to a namespace would
			// change what the cache watches for every other assessment in this feature.
			scopedCluster := createTestClusterWithTargetNamespaces(t, cfg, []string{cfg.Namespace()})

			unknownDesiredResource := newDesiredDeployment(
				"out-of-scope-deployment",
				unmonitoredNamespace,
			)

			applicationInstance := cluster.NewApplicationInstanceBuilder().
				WithDesiredResources([]*cluster.DesiredResource{&unknownDesiredResource}).
				Build()
			scopedCluster.ApplicationInstances.UpsertApplicationInstance(&applicationInstance)

			var applicationInstanceUpdates []cluster.ApplicationInstanceChanges
			for applicationInstanceUpdate := range scopedCluster.GetApplicationInstanceUpdates(context.TODO()) {
				applicationInstanceUpdates = append(applicationInstanceUpdates, *applicationInstanceUpdate)
			}

			if len(applicationInstanceUpdates) != 1 {
				t.Fatalf("Expected 1 ApplicationInstanceUpdate, found %d", len(applicationInstanceUpdates))
			}

			expectedUnknownResourceDesiredResourceIds := []cluster.DesiredResourceId{
				unknownDesiredResource.Id,
			}

			if len(
				applicationInstanceUpdates[0].UnknownMonitoredResources,
			) != len(
				expectedUnknownResourceDesiredResourceIds,
			) {
				t.Errorf(
					"Expected %d unknown resources, found %d",
					len(expectedUnknownResourceDesiredResourceIds),
					len(applicationInstanceUpdates[0].UnknownMonitoredResources),
				)
			}

			actualUnknownResourceDesiredResourceIds := make([]cluster.DesiredResourceId, 0)

			for _, val := range applicationInstanceUpdates[0].UnknownMonitoredResources {
				actualUnknownResourceDesiredResourceIds = append(
					actualUnknownResourceDesiredResourceIds,
					val.DesiredResourceId,
				)

				// Pin which branch produced the Unknown status, so that a resource going Unknown
				// for an unrelated reason can't satisfy this assessment.
				if !strings.Contains(val.Status.Message, unmonitoredNamespace) {
					t.Errorf(
						"Expected unknown status to name namespace %q, got %q",
						unmonitoredNamespace,
						val.Status.Message,
					)
				}
			}

			if diff := cmp.Diff(
				expectedUnknownResourceDesiredResourceIds,
				actualUnknownResourceDesiredResourceIds,
				cmpopts.SortSlices(SortComparableResource),
			); diff != "" {
				t.Error(diff)
			}

			// An out-of-scope resource must be reported Unknown and nothing else. Reporting it
			// Missing is the regression to catch: it would tell server the resource is gone when
			// the monitor simply isn't allowed to look for it.
			if len(applicationInstanceUpdates[0].MissingMonitoredResources) != 0 {
				t.Errorf(
					"Expected 0 missing resources, found %d",
					len(applicationInstanceUpdates[0].MissingMonitoredResources),
				)
			}

			if getPresentAndChildResourceCount(applicationInstanceUpdates[0]) != 0 {
				t.Errorf(
					"Expected 0 present and child resources, found %d",
					getPresentAndChildResourceCount(applicationInstanceUpdates[0]),
				)
			}

			return ctx
		}).
		Feature()

	testenv.Test(t, deploymentFeature)
}

func createComparableResourcesForApplicationInstanceUpdate(
	update cluster.ApplicationInstanceChanges,
) []ComparableResource {
	actualResources := []ComparableResource{}
	for _, resource := range update.PresentMonitoredResources {
		actualResources = append(actualResources, mapPresentMonitoredResourceToComparableResource(resource))
	}
	for _, update := range update.ChildMonitoredResources {
		actualResources = append(actualResources, mapChildMonitoredResourceToComparableResource(update))
	}
	return actualResources
}

func createComparableResourcesForMissingResources(
	missingMonitoredResources []*cluster.MissingMonitoredResource,
) []ComparableResource {
	actualResources := []ComparableResource{}
	for _, resource := range missingMonitoredResources {
		actualResources = append(actualResources, mapMissingMonitoredResourceToComparableResource(resource))
	}
	return actualResources
}

func getPresentAndChildResourceCount(update cluster.ApplicationInstanceChanges) int {
	return len(update.PresentMonitoredResources) +
		len(update.ChildMonitoredResources)
}
