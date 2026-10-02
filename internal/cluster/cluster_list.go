package cluster

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"slices"

	"go.opentelemetry.io/otel/attribute"
	"k8s.io/client-go/rest"
)

// ClusterList owns every Octopus machine sent to this monitor. They all monitor the configured cluster
// through one sharedCluster, connected when the first machine arrives.
type ClusterList struct {
	logger  *slog.Logger
	mailbox mailbox[clusterRegistry]
}

type clusterRegistry struct {
	// lifetime bounds the shared cluster and every target, which outlive the requests that create them.
	lifetime context.Context

	logger  *slog.Logger
	updater MonitoredResourcesUpdater
	connect func() (*sharedCluster, error)

	shared   *sharedCluster
	clusters map[ClusterId]*Cluster
}

// NewClusterList ties the lifetime of every target, and the cluster connection they share, to parentContext.
func NewClusterList(
	parentContext context.Context,
	defaultClusterConfig *rest.Config,
	logger *slog.Logger,
	defaultMonitoredResourcesUpdater MonitoredResourcesUpdater,
	targetNamespaces []string,
	clusterScopedResources bool,
) *ClusterList {
	// TODO: When handling API targets these defaults will need to be updated
	return newClusterList(parentContext, logger, defaultMonitoredResourcesUpdater, func() (*sharedCluster, error) {
		if defaultClusterConfig == nil {
			return nil, errors.New("no cluster connectivity configuration found")
		}
		return newSharedCluster(
			parentContext, logger, defaultClusterConfig, targetNamespaces, clusterScopedResources)
	})
}

// NewClusterListFromConnection is NewClusterList over a connection that's already been made, such as fakes
// in tests. The connection's cache isn't told what the targets are interested in, because NewClusterList
// does that while building the cache.
func NewClusterListFromConnection(
	parentContext context.Context,
	logger *slog.Logger,
	monitoredResourcesUpdater MonitoredResourcesUpdater,
	connection ClusterConnection,
) *ClusterList {
	return newClusterList(parentContext, logger, monitoredResourcesUpdater, func() (*sharedCluster, error) {
		return connection.sharedCluster(logger, newResourceInterests(parentContext, logger)), nil
	})
}

func newClusterList(
	parentContext context.Context,
	logger *slog.Logger,
	updater MonitoredResourcesUpdater,
	connect func() (*sharedCluster, error),
) *ClusterList {
	l := &ClusterList{
		logger:  logger,
		mailbox: newMailbox[clusterRegistry](parentContext.Done()),
	}
	go l.mailbox.serve(&clusterRegistry{
		lifetime: parentContext,
		logger:   logger,
		updater:  updater,
		connect:  connect,
		clusters: map[ClusterId]*Cluster{},
	})
	return l
}

func (l *ClusterList) EnsureCluster(ctx context.Context, id ClusterId) (*Cluster, error) {
	ctx, span := tracer.Start(ctx, "ClusterList.EnsureCluster")
	defer span.End()
	span.SetAttributes(attribute.String("clusterId", string(id)))

	var cluster *Cluster
	err := call(ctx, l.mailbox, func(registry *clusterRegistry) (err error) {
		cluster, err = registry.ensure(id)
		return err
	})
	return cluster, err
}

func (l *ClusterList) GetCluster(ctx context.Context, id ClusterId) (*Cluster, error) {
	var cluster *Cluster
	err := call(ctx, l.mailbox, func(registry *clusterRegistry) error {
		var ok bool
		if cluster, ok = registry.clusters[id]; !ok {
			return fmt.Errorf("machine %s does not exist", id)
		}
		return nil
	})
	return cluster, err
}

// ApplicationInstanceUpdates refreshes discovery and syncs the cluster cache once per sweep, however many
// targets there are.
func (l *ClusterList) ApplicationInstanceUpdates(ctx context.Context) iter.Seq[*ApplicationInstanceChanges] {
	return func(yield func(*ApplicationInstanceChanges) bool) {
		var shared *sharedCluster
		var clusters []*Cluster
		if err := do(ctx, l.mailbox, func(registry *clusterRegistry) {
			shared = registry.shared
			clusters = slices.Collect(maps.Values(registry.clusters))
		}); err != nil {
			l.logger.Error("Skipping monitored resource sweep because the clusters are unavailable",
				slog.Any("error", err))
			return
		}

		// No target has arrived yet, so there's nothing to sweep.
		if shared == nil {
			return
		}

		shared.invalidateDiscovery()
		if err := shared.sync(); err != nil {
			l.logger.Error("Skipping monitored resource sweep because the cluster cache failed to sync",
				slog.Any("error", err))
			return
		}

		for _, cluster := range clusters {
			updates, err := cluster.applicationInstanceUpdates(ctx)
			if err != nil {
				l.logger.Error("Skipping monitored resource sweep for cluster",
					slog.Any("clusterId", cluster.ClusterId),
					slog.Any("error", err))
				continue
			}

			for _, update := range updates {
				if !yield(update) {
					return
				}
			}
		}
	}
}

func (r *clusterRegistry) ensure(id ClusterId) (*Cluster, error) {
	if cluster, ok := r.clusters[id]; ok {
		return cluster, nil
	}

	if r.shared == nil {
		shared, err := r.connect()
		if err != nil {
			return nil, err
		}
		r.shared = shared
	}

	cluster, err := newCluster(r.lifetime, id, r.logger, r.shared, r.updater)
	if err != nil {
		return nil, err
	}

	r.clusters[id] = cluster
	return cluster, nil
}
