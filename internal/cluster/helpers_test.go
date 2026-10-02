package cluster

import (
	"io"
	"log/slog"
	"testing"
)

const testClusterId ClusterId = "cluster-id"

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestSharedCluster(t *testing.T, connection ClusterConnection) *sharedCluster {
	t.Helper()
	return connection.sharedCluster(discardLogger(), newResourceInterests(t.Context(), discardLogger()))
}

// newTestCluster starts a target over shared that already holds applicationInstances.
func newTestCluster(
	t *testing.T,
	shared *sharedCluster,
	updater MonitoredResourcesUpdater,
	applicationInstances ...*ApplicationInstance,
) *Cluster {
	t.Helper()
	c, err := newCluster(t.Context(), testClusterId, discardLogger(), shared, updater)
	if err != nil {
		t.Fatalf("newCluster: %v", err)
	}

	seedApplicationInstances(t, c, applicationInstances...)
	return c
}

func seedApplicationInstances(t *testing.T, c *Cluster, applicationInstances ...*ApplicationInstance) {
	t.Helper()
	err := do(t.Context(), c.mailbox, func(list *ApplicationInstanceList) {
		for _, applicationInstance := range applicationInstances {
			list.UpsertApplicationInstance(applicationInstance)
		}
		c.publishInterests(t.Context(), list)
	})
	if err != nil {
		t.Fatalf("seeding application instances: %v", err)
	}
}

// storedApplicationInstance reads the target's application instance through its mailbox, so the read is
// ordered after every operation sent before it.
func storedApplicationInstance(t *testing.T, c *Cluster, id ApplicationInstanceId) (*ApplicationInstance, bool) {
	t.Helper()
	type stored struct {
		applicationInstance *ApplicationInstance
		ok                  bool
	}
	result, err := ask(t.Context(), c.mailbox, func(list *ApplicationInstanceList) stored {
		applicationInstance, ok := list.Get(id)
		return stored{applicationInstance, ok}
	})
	if err != nil {
		t.Fatalf("reading application instance: %v", err)
	}
	return result.applicationInstance, result.ok
}
