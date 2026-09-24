package kwok_integration

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

func createTestCluster(t *testing.T, cfg *envconf.Config) *cluster.Cluster {
	return createTestClusterWithTargetNamespaces(t, cfg, nil)
}

// createTestClusterWithTargetNamespaces builds a Cluster restricted to targetNamespaces, deriving
// namespaceScopedMode the same way production does in cluster_list.go so the namespace scoping
// branches see the configuration they would in a real deployment. Passing nil monitors everything.
func createTestClusterWithTargetNamespaces(
	t *testing.T, cfg *envconf.Config, targetNamespaces []string,
) *cluster.Cluster {
	applicationInstances := cluster.NewApplicationInstanceList()
	config := cfg.Client().RESTConfig()
	clusterCache, resourceFilter, err := cluster.NewCache(
		t.Context(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		config,
		nil,
		applicationInstances,
		targetNamespaces,
		false,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	cachedDiscoveryClient := memory.NewMemCacheClient(clientset.DiscoveryClient)

	// Mirrors cluster_list.go: clusterScopedResources is false above, so any target namespace
	// list puts the cluster in namespace-scoped mode.
	namespaceScopedMode := len(targetNamespaces) > 0

	return cluster.NewCluster(
		"cluster-id",
		applicationInstances,
		logger,
		clusterCache,
		resourceFilter,
		cachedDiscoveryClient,
		clientset,
		dynamicClient,
		cluster.NoOpUpdater{},
		namespaceScopedMode,
		targetNamespaces,
	)
}
