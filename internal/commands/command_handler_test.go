package commands

import (
	"context"
	"io"
	"log/slog"
	"maps"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache/mocks"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/mock"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"

	engineHealth "github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
	fakediscovery "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
	pb "github.com/octopusdeploy/kubernetes-monitor/internal/protos"
)

const (
	ClusterId             = "cluster-id"
	ClusterHost           = "cluster-host"
	ApplicationInstanceId = "application-instance-id"
)

type BasicUpdater struct {
	receivedUpdate      *cluster.ApplicationInstanceChanges
	receivedReplacement *cluster.ApplicationInstanceChanges
}

func (b *BasicUpdater) Update(_ context.Context, update *cluster.ApplicationInstanceChanges) {
	b.receivedUpdate = update
}

func (b *BasicUpdater) Replace(_ context.Context, replacement *cluster.ApplicationInstanceChanges) {
	b.receivedReplacement = replacement
}

func TestHandle_UpdateDesiredResourcesCommand_UpdatesApplicationInstance(t *testing.T) {
	commandHandler := getCommandHandler(cluster.NoOpUpdater{})

	expectedResource := cluster.NewDesiredResourceBuilder().Build()
	serializedManifest, _ := yaml.Marshal(expectedResource.Manifest.Object)

	pbResource := &pb.DesiredResource{
		DesiredResourceId: &pb.DesiredResourceId{Value: string(expectedResource.Id)},
		ResourceDetails: &pb.DesiredResourceDetails{
			Name:             expectedResource.Details.Name,
			AssumedNamespace: expectedResource.Details.Namespace,
			GroupVersionKind: &pb.GroupVersionKind{
				Group:   "",
				Version: expectedResource.Details.ManifestType.APIVersion,
				Kind:    expectedResource.Details.ManifestType.Kind,
			},
		},
		Manifest: &pb.YamlManifest{Value: string(serializedManifest)},
	}

	_ = commandHandler.handle(&pb.ServerToClientStream{
		Command: &pb.ServerToClientStream_UpdateDesiredResourcesCommand{
			UpdateDesiredResourcesCommand: &pb.UpdateDesiredResourcesCommand{
				ApplicationInstanceId: &pb.ApplicationInstanceId{Value: ApplicationInstanceId},
				ClusterId:             &pb.ClusterId{Value: ClusterId},
				Version:               &pb.Version{Value: string(*expectedResource.Version)},
				DesiredResources:      []*pb.DesiredResource{pbResource},
			},
		},
	})

	actualCluster, err := commandHandler.Clusters.GetCluster(ClusterId)
	if err != nil {
		t.Errorf("Cluster with ID %s not found in %v", ClusterId, maps.Keys(commandHandler.Clusters.GetAll()))
	}

	actual, ok := actualCluster.ApplicationInstances.Get(ApplicationInstanceId)

	if !ok {
		t.Errorf("Updated application instance not found")
	}

	if diff := cmp.Diff([]*cluster.DesiredResource{&expectedResource}, actual.GetDesiredResources()); !ok ||
		diff != "" {
		t.Error(diff)
	}
}

func TestHandle_UpdateDesiredResourcesCommand_SendsMonitoredResourcesUpdate(t *testing.T) {
	updater := BasicUpdater{}

	commandHandler := getCommandHandler(&updater)

	expectedResource := cluster.NewDesiredResourceBuilder().Build()
	serializedManifest, _ := yaml.Marshal(expectedResource.Manifest.Object)

	pbResource := &pb.DesiredResource{
		DesiredResourceId: &pb.DesiredResourceId{Value: string(expectedResource.Id)},
		ResourceDetails: &pb.DesiredResourceDetails{
			Name:             expectedResource.Details.Name,
			AssumedNamespace: expectedResource.Details.Namespace,
			GroupVersionKind: &pb.GroupVersionKind{
				Group:   "",
				Version: expectedResource.Details.ManifestType.APIVersion,
				Kind:    expectedResource.Details.ManifestType.Kind,
			},
		},
		Manifest: &pb.YamlManifest{Value: string(serializedManifest)},
	}

	_ = commandHandler.handle(&pb.ServerToClientStream{
		Command: &pb.ServerToClientStream_UpdateDesiredResourcesCommand{
			UpdateDesiredResourcesCommand: &pb.UpdateDesiredResourcesCommand{
				ApplicationInstanceId: &pb.ApplicationInstanceId{Value: ApplicationInstanceId},
				ClusterId:             &pb.ClusterId{Value: ClusterId},
				Version:               &pb.Version{Value: string(*expectedResource.Version)},
				DesiredResources:      []*pb.DesiredResource{pbResource},
			},
		},
	})

	gvk := expectedResource.Details.ManifestType.GroupVersionKind()

	expectedUpdate := &cluster.ApplicationInstanceChanges{
		ApplicationInstanceId: ApplicationInstanceId,
		ClusterId:             ClusterId,
		MissingMonitoredResources: []*cluster.MissingMonitoredResource{
			{
				ClusterId:         ClusterId,
				DesiredResourceId: expectedResource.Id,
				Status: &engineHealth.HealthStatus{
					Status: "Missing",
				},
				SyncStatus: &cluster.SyncStatus{
					Status: "OutOfSync",
				},
				GroupVersionKind: &gvk,
				Name:             expectedResource.Details.Name,
				Namespace:        expectedResource.Details.Namespace,
			},
		},
	}

	if diff := cmp.Diff(expectedUpdate, updater.receivedUpdate); diff != "" {
		t.Error(diff)
	}
}

func TestHandle_PruneOtherVersionsCommand_RemovesOldVersions(t *testing.T) {
	commandHandler := getCommandHandler(cluster.NoOpUpdater{})

	v1Resource := cluster.NewDesiredResourceBuilder().
		WithName("version-1").
		WithVersion("will not keep").
		Build()

	v2Resource := cluster.NewDesiredResourceBuilder().
		WithName("version-2").
		WithVersion("to keep").
		Build()

	applicationInstance := cluster.NewApplicationInstanceBuilder().
		WithDesiredResources([]*cluster.DesiredResource{&v1Resource, &v2Resource}).
		WithClusterId(ClusterId).
		Build()

	actualCluster, err := commandHandler.Clusters.GetCluster(ClusterId)
	if err != nil {
		t.Errorf("Cluster with ID %s not found in %v", ClusterId, maps.Keys(commandHandler.Clusters.GetAll()))
	}

	actualCluster.ApplicationInstances.UpsertApplicationInstance(&applicationInstance)

	_ = commandHandler.handle(&pb.ServerToClientStream{
		Command: &pb.ServerToClientStream_PruneOtherVersionsCommand{
			PruneOtherVersionsCommand: &pb.PruneOtherVersionsCommand{
				ApplicationInstanceId: &pb.ApplicationInstanceId{
					Value: string(applicationInstance.ApplicationInstanceId),
				},
				ClusterId: &pb.ClusterId{Value: ClusterId},
				Version:   &pb.Version{Value: "to keep"},
			},
		},
	})

	actual, ok := actualCluster.ApplicationInstances.Get(applicationInstance.ApplicationInstanceId)

	if !ok {
		t.Errorf("Updated application instance not found")
	}

	if diff := cmp.Diff([]*cluster.DesiredResource{&v2Resource}, actual.GetDesiredResources()); !ok || diff != "" {
		t.Error(diff)
	}
}

func TestHandle_DeleteDesiredResourcesCommand_OnlyRemovesListedResources(t *testing.T) {
	commandHandler := getCommandHandler(cluster.NoOpUpdater{})

	r1 := cluster.NewDesiredResourceBuilder().WithName("resource-1").Build()
	r2 := cluster.NewDesiredResourceBuilder().WithName("resource-2").Build()
	r3 := cluster.NewDesiredResourceBuilder().WithName("resource-3").Build()

	applicationInstance := cluster.NewApplicationInstanceBuilder().
		WithDesiredResources([]*cluster.DesiredResource{&r1, &r2, &r3}).
		WithClusterId(ClusterId).
		Build()

	actualCluster, err := commandHandler.Clusters.GetCluster(ClusterId)
	if err != nil {
		t.Errorf("Cluster with ID %s not found in %v", ClusterId, maps.Keys(commandHandler.Clusters.GetAll()))
	}

	actualCluster.ApplicationInstances.UpsertApplicationInstance(&applicationInstance)

	_ = commandHandler.handle(&pb.ServerToClientStream{
		Command: &pb.ServerToClientStream_DeleteDesiredResourcesCommand{
			DeleteDesiredResourcesCommand: &pb.DeleteDesiredResourcesCommand{
				ApplicationInstanceId: &pb.ApplicationInstanceId{
					Value: string(applicationInstance.ApplicationInstanceId),
				},
				ClusterId: &pb.ClusterId{Value: ClusterId},
				ResourceIds: []*pb.DesiredResourceId{
					{Value: string(r1.Id)},
					{Value: string(r3.Id)},
				},
			},
		},
	})

	actual, ok := actualCluster.ApplicationInstances.Get(applicationInstance.ApplicationInstanceId)
	if !ok {
		t.Errorf("Application instance not found")
	}

	if diff := cmp.Diff([]*cluster.DesiredResource{&r2}, actual.GetDesiredResources()); diff != "" {
		t.Error(diff)
	}
}

func TestHandle_ReplaceDesiredResourcesCommand(t *testing.T) {
	t.Run("Drops resources absent from the list", func(t *testing.T) {
		updater := BasicUpdater{}
		commandHandler := getCommandHandler(&updater)

		kept := cluster.NewDesiredResourceBuilder().WithName("kept").Build()
		dropped := cluster.NewDesiredResourceBuilder().WithName("dropped").Build()

		applicationInstance := cluster.NewApplicationInstanceBuilder().
			WithApplicationInstanceId(ApplicationInstanceId).
			WithDesiredResources([]*cluster.DesiredResource{&kept, &dropped}).
			WithClusterId(ClusterId).
			Build()

		actualCluster, err := commandHandler.Clusters.GetCluster(ClusterId)
		if err != nil {
			t.Fatalf("Cluster with ID %s not found in %v", ClusterId, maps.Keys(commandHandler.Clusters.GetAll()))
		}

		actualCluster.ApplicationInstances.UpsertApplicationInstance(&applicationInstance)

		_ = commandHandler.handle(&pb.ServerToClientStream{
			Command: &pb.ServerToClientStream_ReplaceDesiredResourcesCommand{
				ReplaceDesiredResourcesCommand: &pb.ReplaceDesiredResourcesCommand{
					ApplicationInstanceId: &pb.ApplicationInstanceId{Value: ApplicationInstanceId},
					ClusterId:             &pb.ClusterId{Value: ClusterId},
					DesiredResources:      []*pb.DesiredResource{toPbDesiredResource(kept)},
				},
			},
		})

		actual, ok := actualCluster.ApplicationInstances.Get(ApplicationInstanceId)
		if !ok {
			t.Fatal("Application instance not found")
		}

		// A complete list carries no deployment version, so what arrives has no version rather than the
		// one it was seeded with.
		expected := cluster.NewDesiredResourceBuilder().
			WithId(kept.Id).
			WithName("kept").
			WithoutVersion().
			Build()

		if diff := cmp.Diff([]*cluster.DesiredResource{&expected}, actual.GetDesiredResources()); diff != "" {
			t.Error(diff)
		}

		// A complete list goes to Server as a replacement, not a delta — a delta could never tell Server
		// that "dropped" is gone.
		if updater.receivedReplacement == nil {
			t.Error("Expected a replacement to be sent")
		}

		if updater.receivedUpdate != nil {
			t.Error("Expected no incremental update to be sent")
		}
	})

	t.Run("Empty list clears the application instance", func(t *testing.T) {
		updater := BasicUpdater{}
		commandHandler := getCommandHandler(&updater)

		existing := cluster.NewDesiredResourceBuilder().WithName("existing").Build()

		applicationInstance := cluster.NewApplicationInstanceBuilder().
			WithApplicationInstanceId(ApplicationInstanceId).
			WithDesiredResources([]*cluster.DesiredResource{&existing}).
			WithClusterId(ClusterId).
			Build()

		actualCluster, err := commandHandler.Clusters.GetCluster(ClusterId)
		if err != nil {
			t.Fatalf("Cluster with ID %s not found in %v", ClusterId, maps.Keys(commandHandler.Clusters.GetAll()))
		}

		actualCluster.ApplicationInstances.UpsertApplicationInstance(&applicationInstance)

		_ = commandHandler.handle(&pb.ServerToClientStream{
			Command: &pb.ServerToClientStream_ReplaceDesiredResourcesCommand{
				ReplaceDesiredResourcesCommand: &pb.ReplaceDesiredResourcesCommand{
					ApplicationInstanceId: &pb.ApplicationInstanceId{Value: ApplicationInstanceId},
					ClusterId:             &pb.ClusterId{Value: ClusterId},
					DesiredResources:      nil,
				},
			},
		})

		actual, ok := actualCluster.ApplicationInstances.Get(ApplicationInstanceId)
		if !ok {
			t.Fatal("Application instance not found")
		}

		if count := len(actual.GetDesiredResources()); count != 0 {
			t.Errorf("Expected no desired resources, but got %d", count)
		}

		// An empty complete list still has to reach Server, otherwise it keeps the state forever.
		if updater.receivedReplacement == nil {
			t.Fatal("Expected a replacement to be sent for an empty list")
		}

		if count := updater.receivedReplacement.GetAllResourceCount(); count != 0 {
			t.Errorf("Expected the replacement to be empty, but got %d resources", count)
		}
	})
}

type MockCachedDiscoveryClient struct {
	*fakediscovery.FakeDiscovery
}

func (m *MockCachedDiscoveryClient) Fresh() bool { return true }
func (m *MockCachedDiscoveryClient) Invalidate() {}

func getCommandHandler(updateMonitoredResourcesFunc cluster.MonitoredResourcesUpdater) *CommandHandler {
	mockCache := mocks.ClusterCache{}
	clusterInfo := cache.ClusterInfo{Server: ClusterHost}
	mockCache.On("EnsureSynced").Return(nil)
	mockCache.On("OnResourceUpdated", mock.AnythingOfType("cache.OnResourceUpdatedHandler")).
		Return(cache.Unsubscribe(func() {}))
	// A synced cache knows the built-in Deployment type used by these tests; an empty list
	// would be read as "the type's CRD is gone" and reclassify the resource away from Missing.
	mockCache.On("GetAPIResources").Return([]kube.APIResourceInfo{
		{GroupKind: schema.GroupKind{Group: "apps", Kind: "Deployment"}},
	})
	mockCache.On("IsNamespaced", schema.GroupKind{Group: "apps", Kind: "Deployment"}).Return(true, nil)
	mockCache.On("GetManagedLiveObjs", mock.AnythingOfType("[]*unstructured.Unstructured"), mock.AnythingOfType("func(*cache.Resource) bool")).
		Return(map[kube.ResourceKey]*unstructured.Unstructured{}, nil)
	mockCache.On(
		"IterateHierarchyV2",
		mock.AnythingOfType("[]kube.ResourceKey"),
		mock.AnythingOfType("func(*cache.Resource, map[kube.ResourceKey]*cache.Resource) bool"),
	)
	mockCache.On("GetClusterInfo").Return(clusterInfo)
	mockClientSet := fake.NewClientset()

	applicationInstanceList := cluster.NewApplicationInstanceList()
	restConfig := &rest.Config{Host: ClusterHost}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockDiscoveryClient := &MockCachedDiscoveryClient{}
	clusters := cluster.NewClusterList(
		context.Background(),
		restConfig,
		logger,
		updateMonitoredResourcesFunc,
		nil,
		false,
	)
	mockCluster := cluster.NewCluster(
		ClusterId,
		applicationInstanceList,
		logger,
		&mockCache,
		nil,
		mockDiscoveryClient,
		mockClientSet,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		updateMonitoredResourcesFunc,
		false,
		nil,
	)
	clusters.SetCluster(mockCluster)

	return NewCommandHandler(&clusters, nil, context.Background(), logger)
}

func toPbDesiredResource(resource cluster.DesiredResource) *pb.DesiredResource {
	serializedManifest, _ := yaml.Marshal(resource.Manifest.Object)

	return &pb.DesiredResource{
		DesiredResourceId: &pb.DesiredResourceId{Value: string(resource.Id)},
		ResourceDetails: &pb.DesiredResourceDetails{
			Name:             resource.Details.Name,
			AssumedNamespace: resource.Details.Namespace,
			GroupVersionKind: &pb.GroupVersionKind{
				Group:   "",
				Version: resource.Details.ManifestType.APIVersion,
				Kind:    resource.Details.ManifestType.Kind,
			},
		},
		Manifest: &pb.YamlManifest{Value: string(serializedManifest)},
	}
}
