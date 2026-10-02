package commands

import (
	"context"
	"io"
	"log/slog"
	"slices"
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
	commandHandler := getCommandHandler(t, cluster.NoOpUpdater{})

	expectedResource := cluster.NewDesiredResourceBuilder().Build()

	updateDesiredResources(t, commandHandler, *expectedResource.Version, expectedResource)

	if diff := cmp.Diff([]cluster.DesiredResourceId{expectedResource.Id}, desiredResourceIds(t, commandHandler)); diff != "" {
		t.Error(diff)
	}
}

func TestHandle_UpdateDesiredResourcesCommand_SendsMonitoredResourcesUpdate(t *testing.T) {
	updater := BasicUpdater{}

	commandHandler := getCommandHandler(t, &updater)

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
	commandHandler := getCommandHandler(t, cluster.NoOpUpdater{})

	v1Resource := cluster.NewDesiredResourceBuilder().
		WithName("version-1").
		WithVersion("will not keep").
		Build()

	v2Resource := cluster.NewDesiredResourceBuilder().
		WithName("version-2").
		WithVersion("to keep").
		Build()

	updateDesiredResources(t, commandHandler, "will not keep", v1Resource)
	updateDesiredResources(t, commandHandler, "to keep", v2Resource)

	handle(t, commandHandler, &pb.ServerToClientStream{
		Command: &pb.ServerToClientStream_PruneOtherVersionsCommand{
			PruneOtherVersionsCommand: &pb.PruneOtherVersionsCommand{
				ApplicationInstanceId: &pb.ApplicationInstanceId{Value: ApplicationInstanceId},
				ClusterId:             &pb.ClusterId{Value: ClusterId},
				Version:               &pb.Version{Value: "to keep"},
			},
		},
	})

	if diff := cmp.Diff([]cluster.DesiredResourceId{v2Resource.Id}, desiredResourceIds(t, commandHandler)); diff != "" {
		t.Error(diff)
	}
}

func TestHandle_DeleteDesiredResourcesCommand_OnlyRemovesListedResources(t *testing.T) {
	commandHandler := getCommandHandler(t, cluster.NoOpUpdater{})

	r1 := cluster.NewDesiredResourceBuilder().WithName("resource-1").Build()
	r2 := cluster.NewDesiredResourceBuilder().WithName("resource-2").Build()
	r3 := cluster.NewDesiredResourceBuilder().WithName("resource-3").Build()

	updateDesiredResources(t, commandHandler, "v1", r1, r2, r3)

	handle(t, commandHandler, &pb.ServerToClientStream{
		Command: &pb.ServerToClientStream_DeleteDesiredResourcesCommand{
			DeleteDesiredResourcesCommand: &pb.DeleteDesiredResourcesCommand{
				ApplicationInstanceId: &pb.ApplicationInstanceId{Value: ApplicationInstanceId},
				ClusterId:             &pb.ClusterId{Value: ClusterId},
				ResourceIds: []*pb.DesiredResourceId{
					{Value: string(r1.Id)},
					{Value: string(r3.Id)},
				},
			},
		},
	})

	if diff := cmp.Diff([]cluster.DesiredResourceId{r2.Id}, desiredResourceIds(t, commandHandler)); diff != "" {
		t.Error(diff)
	}
}

func TestHandle_ReplaceDesiredResourcesCommand(t *testing.T) {
	t.Run("Drops resources absent from the list", func(t *testing.T) {
		updater := BasicUpdater{}
		commandHandler := getCommandHandler(t, &updater)

		kept := cluster.NewDesiredResourceBuilder().WithName("kept").Build()
		dropped := cluster.NewDesiredResourceBuilder().WithName("dropped").Build()

		updateDesiredResources(t, commandHandler, "v1", kept, dropped)
		updater.receivedUpdate = nil

		handle(t, commandHandler, &pb.ServerToClientStream{
			Command: &pb.ServerToClientStream_ReplaceDesiredResourcesCommand{
				ReplaceDesiredResourcesCommand: &pb.ReplaceDesiredResourcesCommand{
					ApplicationInstanceId: &pb.ApplicationInstanceId{Value: ApplicationInstanceId},
					ClusterId:             &pb.ClusterId{Value: ClusterId},
					DesiredResources:      []*pb.DesiredResource{toPbDesiredResource(kept)},
				},
			},
		})

		// A complete list goes to Server as a replacement, not a delta — a delta could never tell Server
		// that "dropped" is gone.
		if updater.receivedReplacement == nil {
			t.Fatal("Expected a replacement to be sent")
		}

		if updater.receivedUpdate != nil {
			t.Error("Expected no incremental update to be sent")
		}

		if diff := cmp.Diff([]cluster.DesiredResourceId{kept.Id}, missingIds(updater.receivedReplacement)); diff != "" {
			t.Error(diff)
		}

		if diff := cmp.Diff([]cluster.DesiredResourceId{kept.Id}, desiredResourceIds(t, commandHandler)); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("Empty list clears the application instance", func(t *testing.T) {
		updater := BasicUpdater{}
		commandHandler := getCommandHandler(t, &updater)

		existing := cluster.NewDesiredResourceBuilder().WithName("existing").Build()

		updateDesiredResources(t, commandHandler, "v1", existing)

		handle(t, commandHandler, &pb.ServerToClientStream{
			Command: &pb.ServerToClientStream_ReplaceDesiredResourcesCommand{
				ReplaceDesiredResourcesCommand: &pb.ReplaceDesiredResourcesCommand{
					ApplicationInstanceId: &pb.ApplicationInstanceId{Value: ApplicationInstanceId},
					ClusterId:             &pb.ClusterId{Value: ClusterId},
					DesiredResources:      nil,
				},
			},
		})

		if ids := desiredResourceIds(t, commandHandler); len(ids) != 0 {
			t.Errorf("Expected no desired resources, but got %v", ids)
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

func handle(t *testing.T, commandHandler *CommandHandler, command *pb.ServerToClientStream) {
	t.Helper()
	if err := commandHandler.handle(command); err != nil {
		t.Fatalf("handle: %v", err)
	}
}

func updateDesiredResources(
	t *testing.T,
	commandHandler *CommandHandler,
	version cluster.Version,
	resources ...cluster.DesiredResource,
) {
	t.Helper()
	pbResources := make([]*pb.DesiredResource, 0, len(resources))
	for _, resource := range resources {
		pbResources = append(pbResources, toPbDesiredResource(resource))
	}

	handle(t, commandHandler, &pb.ServerToClientStream{
		Command: &pb.ServerToClientStream_UpdateDesiredResourcesCommand{
			UpdateDesiredResourcesCommand: &pb.UpdateDesiredResourcesCommand{
				ApplicationInstanceId: &pb.ApplicationInstanceId{Value: ApplicationInstanceId},
				ClusterId:             &pb.ClusterId{Value: ClusterId},
				Version:               &pb.Version{Value: string(version)},
				DesiredResources:      pbResources,
			},
		},
	})
}

// The mock cache holds no live objects, so a sweep reports every desired resource as Missing.
func desiredResourceIds(t *testing.T, commandHandler *CommandHandler) []cluster.DesiredResourceId {
	t.Helper()
	var ids []cluster.DesiredResourceId
	found := false
	for update := range commandHandler.Clusters.ApplicationInstanceUpdates(t.Context()) {
		if update.ApplicationInstanceId != ApplicationInstanceId {
			continue
		}
		found = true
		ids = append(ids, missingIds(update)...)
	}
	if !found {
		t.Fatal("Application instance not found")
	}
	slices.Sort(ids)
	return ids
}

func missingIds(changes *cluster.ApplicationInstanceChanges) []cluster.DesiredResourceId {
	var ids []cluster.DesiredResourceId
	for _, missing := range changes.MissingMonitoredResources {
		ids = append(ids, missing.DesiredResourceId)
	}
	slices.Sort(ids)
	return ids
}

type MockCachedDiscoveryClient struct {
	*fakediscovery.FakeDiscovery
}

func (m *MockCachedDiscoveryClient) Fresh() bool { return true }
func (m *MockCachedDiscoveryClient) Invalidate() {}

func getCommandHandler(t *testing.T, updater cluster.MonitoredResourcesUpdater) *CommandHandler {
	mockCache := mocks.ClusterCache{}
	clusterInfo := cache.ClusterInfo{Server: ClusterHost}
	mockCache.On("EnsureSynced").Return(nil)
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

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	clusters := cluster.NewClusterListFromConnection(t.Context(), logger, updater, cluster.ClusterConnection{
		Cache:         &mockCache,
		Discovery:     &MockCachedDiscoveryClient{},
		ClientSet:     fake.NewClientset(),
		DynamicClient: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
	})
	if _, err := clusters.EnsureCluster(t.Context(), ClusterId); err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}

	return NewCommandHandler(clusters, nil, t.Context(), logger)
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
