package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const permissionFilterRefreshInterval = 1 * time.Minute

type ClusterList struct {
	mutex                            MutexWithLogging
	clusters                         map[ClusterId]*Cluster
	parentContext                    context.Context
	defaultClusterConfig             *rest.Config
	logger                           *slog.Logger
	defaultMonitoredResourcesUpdater MonitoredResourcesUpdater
	targetNamespaces                 []string
	clusterScopedResources           bool
}

func NewClusterList(
	parentContext context.Context,
	defaultClusterConfig *rest.Config,
	logger *slog.Logger,
	defaultMonitoredResourcesUpdater MonitoredResourcesUpdater,
	targetNamespaces []string,
	clusterScopedResources bool,
) ClusterList {
	return ClusterList{
		mutex:    NewMutexWithLogging(logger, "ClusterList"),
		clusters: map[ClusterId]*Cluster{},

		parentContext: parentContext,

		// TODO: When handling API targets these two defaults will need to be updated
		defaultClusterConfig:             defaultClusterConfig,
		defaultMonitoredResourcesUpdater: defaultMonitoredResourcesUpdater,
		targetNamespaces:                 targetNamespaces,
		clusterScopedResources:           clusterScopedResources,

		logger: logger,
	}
}

func (l *ClusterList) EnsureCluster(ctx context.Context, id ClusterId) (*Cluster, error) {
	_, span := tracer.Start(ctx, "ClusterList.EnsureCluster")
	defer span.End()

	span.SetAttributes(attribute.String("clusterId", string(id)))

	if l.defaultClusterConfig == nil {
		return nil, errors.New("no cluster connectivity configuration found")
	}

	span.AddEvent("Locking cluster list", trace.WithAttributes(attribute.String("clusterId", string(id))))
	l.mutex.Lock("EnsureCluster")
	span.AddEvent("Locked cluster list", trace.WithAttributes(attribute.String("clusterId", string(id))))
	defer l.mutex.Unlock("EnsureCluster")

	// Check if cluster already exists
	if existingCluster, ok := l.clusters[id]; ok {
		clusterServer := existingCluster.clusterServer
		if clusterServer != l.defaultClusterConfig.Host {
			// Check the cluster saved is the same as what's provided
			return nil, fmt.Errorf(
				"machine %s already exists with host %s. Refusing to replace with host %s",
				id,
				clusterServer,
				l.defaultClusterConfig.Host,
			)
		}
		return existingCluster, nil
	}

	applicationInstanceList := NewApplicationInstanceList()

	clientset, err := kubernetes.NewForConfig(l.defaultClusterConfig)
	if err != nil {
		return nil, err
	}

	clusterCache, resourceFilter, err := NewCache(
		l.parentContext,
		l.logger,
		l.defaultClusterConfig,
		clientset,
		applicationInstanceList,
		l.targetNamespaces,
		l.clusterScopedResources,
		permissionFilterRefreshInterval,
	)
	if err != nil {
		return nil, err
	}

	dynamicClient, err := dynamic.NewForConfig(l.defaultClusterConfig)
	if err != nil {
		return nil, err
	}

	cachedDiscoveryClient := memory.NewMemCacheClient(clientset.DiscoveryClient)

	namespaceScopedMode := len(l.targetNamespaces) > 0 && !l.clusterScopedResources
	l.clusters[id] = NewCluster(
		id,
		applicationInstanceList,
		l.logger,
		clusterCache,
		resourceFilter,
		cachedDiscoveryClient,
		clientset,
		dynamicClient,
		l.defaultMonitoredResourcesUpdater,
		namespaceScopedMode,
		l.targetNamespaces,
	)
	return l.clusters[id], nil
}

func (l *ClusterList) GetAll() map[ClusterId]*Cluster {
	l.mutex.Lock("GetAll")
	defer l.mutex.Unlock("GetAll")
	return l.clusters
}

func (l *ClusterList) GetCluster(id ClusterId) (*Cluster, error) {
	l.mutex.Lock("GetCluster")
	defer l.mutex.Unlock("GetCluster")

	if c, ok := l.clusters[id]; ok {
		return c, nil
	}

	return nil, fmt.Errorf("machine %s does not exist", id)
}

func (l *ClusterList) SetCluster(cluster *Cluster) {
	l.mutex.Lock("SetCluster")
	defer l.mutex.Unlock("SetCluster")
	l.clusters[cluster.ClusterId] = cluster
}
