package kwok_integration

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
)

const (
	testHashSalt              = crypto.HashSalt("fake-salt")
	testApplicationInstanceId = cluster.ApplicationInstanceId("application-instance-id")
)

// testTarget pairs a target with the list that owns it, because sweeps run across the whole list.
type testTarget struct {
	*cluster.Cluster
	clusters *cluster.ClusterList
}

func createTestCluster(t *testing.T, cfg *envconf.Config) *testTarget {
	return createTestClusterWithTargetNamespaces(t, cfg, nil)
}

// Passing nil targetNamespaces monitors everything.
func createTestClusterWithTargetNamespaces(
	t *testing.T, cfg *envconf.Config, targetNamespaces []string,
) *testTarget {
	clusters := createTestClusterList(t, cfg, cluster.NoOpUpdater{}, targetNamespaces)
	return ensureTestTarget(t, clusters, "cluster-id")
}

// createTestClusterList shares one cluster cache between every target ensured on it, for the lifetime of t.
func createTestClusterList(
	t *testing.T, cfg *envconf.Config, updater cluster.MonitoredResourcesUpdater, targetNamespaces []string,
) *cluster.ClusterList {
	return cluster.NewClusterList(
		t.Context(),
		cfg.Client().RESTConfig(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		updater,
		targetNamespaces,
		false,
	)
}

func ensureTestTarget(t *testing.T, clusters *cluster.ClusterList, id cluster.ClusterId) *testTarget {
	target, err := clusters.EnsureCluster(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return &testTarget{Cluster: target, clusters: clusters}
}

func (target *testTarget) sweep(ctx context.Context) []*cluster.ApplicationInstanceChanges {
	var changes []*cluster.ApplicationInstanceChanges
	for change := range target.clusters.ApplicationInstanceUpdates(ctx) {
		if change.ClusterId == target.ClusterId {
			changes = append(changes, change)
		}
	}
	return changes
}

func desiredResourceMap(desiredResources ...*cluster.DesiredResource) map[kube.ResourceKey]*cluster.DesiredResource {
	desired := make(map[kube.ResourceKey]*cluster.DesiredResource, len(desiredResources))
	for _, desiredResource := range desiredResources {
		desired[desiredResource.ResourceKey()] = desiredResource
	}
	return desired
}
