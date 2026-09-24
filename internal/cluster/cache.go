package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Cache interface {
	cache.ClusterCache
}

// clusterSyncRetryTimeout defaults to 10s. A full cluster sync sometimes takes longer than 10s,
// which leads to a bunch of retries stacking. Once per monitor loop should be enough.
const clusterSyncRetryTimeout = 2 * time.Minute

// CacheTracer wraps the OpenTelemetry tracer to allow us to use tracing inside GitOps Engine
type CacheTracer struct {
	tracer        trace.Tracer
	parentContext context.Context
}

func (c CacheTracer) StartSpan(operationName string) tracing.Span {
	_, span := c.tracer.Start(c.parentContext, operationName)
	return CacheSpan{realSpan: span}
}

type CacheSpan struct {
	realSpan trace.Span
}

func (c CacheSpan) Finish() {
	c.realSpan.End()
}

func (c CacheSpan) SetBaggageItem(key string, value any) {
	switch v := value.(type) {
	case int:
		c.realSpan.SetAttributes(attribute.Int(key, v))
	case string:
		c.realSpan.SetAttributes(attribute.String(key, v))
	case bool:
		c.realSpan.SetAttributes(attribute.Bool(key, v))
	default:
		// Try our best to output something useful
		c.realSpan.SetAttributes(attribute.String(key, fmt.Sprintf("%v", v)))
	}
}

func NewCache(
	ctx context.Context,
	logger *slog.Logger,
	config *rest.Config,
	clientset kubernetes.Interface,
	applicationInstances *ApplicationInstanceList,
	targetNamespaces []string,
	clusterScopedResources bool,
	permissionRefreshInterval time.Duration,
) (Cache, kube.ResourceFilter, error) {
	clusterCacheOpts := []cache.UpdateSettingsFunc{
		cache.SetPopulateResourceInfoHandler(onPopulateResourceInfoHandler(applicationInstances)),
		cache.SetTracer(CacheTracer{tracer: tracer, parentContext: context.TODO()}),
		cache.SetRespectRBAC(cache.RespectRbacNormal),
		cache.SetClusterSyncRetryTimeout(clusterSyncRetryTimeout),
	}

	if len(targetNamespaces) > 0 {
		clusterCacheOpts = append(clusterCacheOpts,
			cache.SetNamespaces(targetNamespaces),
			cache.SetClusterResources(clusterScopedResources),
		)
	}

	var resourceFilter kube.ResourceFilter
	var permissionFilter *PermissionResourceFilter
	if clientset != nil {
		discoveryClient := memory.NewMemCacheClient(clientset.Discovery())
		filter, err := NewPermissionResourceFilter(
			ctx,
			logger,
			clientset,
			discoveryClient,
			targetNamespaces,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("building permission filter: %w", err)
		}
		permissionFilter = filter
		resourceFilter = filter
	}

	clusterCache := cache.NewClusterCache(config, clusterCacheOpts...)
	clusterCache.OnResourceUpdated(removeTrackedResourceKeyOnResourceDeleted(applicationInstances))

	if permissionFilter != nil {
		go permissionFilter.startRefreshLoop(ctx, permissionRefreshInterval)
	}

	return clusterCache, resourceFilter, nil
}

type ResourceInfo struct {
	ResourceKey kube.ResourceKey
	OwnerRefs   []v1.OwnerReference
}

func onPopulateResourceInfoHandler(applicationInstances *ApplicationInstanceList) func(
	un *unstructured.Unstructured, isRoot bool,
) (any, bool) {
	return func(un *unstructured.Unstructured, isRoot bool) (any, bool) {
		resourceKey := kube.GetResourceKey(un)
		info := ResourceInfo{ResourceKey: resourceKey, OwnerRefs: getOwnerReferences(un)}

		if applicationInstances.ResourceIsTracked(resourceKey) {
			return info, true
		}

		if isRoot == false && applicationInstances.OwnerResourceIsTracked(resourceKey, info.OwnerRefs) {
			return info, true
		}

		return info, false
	}
}

func removeTrackedResourceKeyOnResourceDeleted(applicationInstances *ApplicationInstanceList) func(
	newRes *cache.Resource, oldRes *cache.Resource, namespaceResources map[kube.ResourceKey]*cache.Resource,
) {
	return func(
		newRes *cache.Resource, oldRes *cache.Resource, namespaceResources map[kube.ResourceKey]*cache.Resource,
	) {
		if oldRes == nil || oldRes.Resource == nil || newRes != nil {
			return
		}

		oldResourceCacheKey := oldRes.Info.(ResourceInfo).ResourceKey
		applicationInstances.RemoveTrackedResourceKey(oldResourceCacheKey)
	}
}
