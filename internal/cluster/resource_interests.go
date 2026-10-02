package cluster

import (
	"context"
	"log/slog"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// resourceInterests stops tracking a resource once no target wants it, because targets publish their
// complete interest set whenever their state changes.
type resourceInterests struct {
	logger  *slog.Logger
	mailbox mailbox[interestIndex]
}

type interestIndex struct {
	byTarget    map[ClusterId]map[kube.ResourceKey]struct{}
	byResource  map[kube.ResourceKey]map[ClusterId]struct{}
	subscribers map[ClusterId]chan<- resourceChange
	stopped     <-chan struct{}
}

func newResourceInterests(ctx context.Context, logger *slog.Logger) *resourceInterests {
	r := &resourceInterests{
		logger:  logger,
		mailbox: newMailbox[interestIndex](ctx.Done()),
	}
	go r.mailbox.serve(&interestIndex{
		byTarget:    map[ClusterId]map[kube.ResourceKey]struct{}{},
		byResource:  map[kube.ResourceKey]map[ClusterId]struct{}{},
		subscribers: map[ClusterId]chan<- resourceChange{},
		stopped:     ctx.Done(),
	})
	return r
}

func (r *resourceInterests) subscribe(ctx context.Context, target ClusterId, changes chan<- resourceChange) error {
	return do(ctx, r.mailbox, func(index *interestIndex) {
		index.subscribers[target] = changes
	})
}

// The index takes ownership of keys.
func (r *resourceInterests) set(ctx context.Context, target ClusterId, keys map[kube.ResourceKey]struct{}) error {
	return do(ctx, r.mailbox, func(index *interestIndex) {
		index.set(target, keys)
	})
}

// populateResourceInfo runs under the cluster cache's lock, which is safe because the index's goroutine never
// calls back into the cache.
func (r *resourceInterests) populateResourceInfo(un *unstructured.Unstructured, isRoot bool) (any, bool) {
	info := ResourceInfo{ResourceKey: kube.GetResourceKey(un), OwnerRefs: getOwnerReferences(un)}
	cacheManifest, err := ask(context.Background(), r.mailbox, func(index *interestIndex) bool {
		return index.cacheManifest(info, isRoot)
	})
	if err != nil {
		return info, false
	}
	return info, cacheManifest
}

// route runs under the cluster cache's lock, so it only hands the change on; each target's queue always
// accepts immediately and the target does the work later.
func (r *resourceInterests) route(newRes, oldRes *cache.Resource, _ map[kube.ResourceKey]*cache.Resource) {
	change := resourceChange{newRes: newRes, oldRes: oldRes}
	if err := do(context.Background(), r.mailbox, func(index *interestIndex) {
		index.route(change)
	}); err != nil {
		r.logger.Debug("Dropping resource change because the interest index has stopped", slog.Any("error", err))
	}
}

func (x *interestIndex) set(target ClusterId, keys map[kube.ResourceKey]struct{}) {
	for key := range x.byTarget[target] {
		if _, kept := keys[key]; !kept {
			x.forget(target, key)
		}
	}
	for key := range keys {
		x.addTarget(key, target)
	}
	x.byTarget[target] = keys
}

func (x *interestIndex) remember(target ClusterId, key kube.ResourceKey) {
	keys, ok := x.byTarget[target]
	if !ok {
		keys = map[kube.ResourceKey]struct{}{}
		x.byTarget[target] = keys
	}
	keys[key] = struct{}{}
	x.addTarget(key, target)
}

func (x *interestIndex) addTarget(key kube.ResourceKey, target ClusterId) {
	targets, ok := x.byResource[key]
	if !ok {
		targets = map[ClusterId]struct{}{}
		x.byResource[key] = targets
	}
	targets[target] = struct{}{}
}

func (x *interestIndex) forget(target ClusterId, key kube.ResourceKey) {
	targets := x.byResource[key]
	delete(targets, target)
	if len(targets) == 0 {
		delete(x.byResource, key)
	}
}

// cacheManifest makes a target interested in resources owned by something it's interested in, because it
// will monitor them as children; that lets their own children be recognised while the cache lists the cluster.
func (x *interestIndex) cacheManifest(info ResourceInfo, isRoot bool) bool {
	if len(x.byResource[info.ResourceKey]) > 0 {
		return true
	}
	if isRoot {
		return false
	}

	inherited := false
	for _, ownerRef := range info.OwnerRefs {
		ownerKey, err := ownerRefResourceKey(ownerRef, info.ResourceKey.Namespace)
		if err != nil {
			continue
		}
		for target := range x.byResource[ownerKey] {
			x.remember(target, info.ResourceKey)
			inherited = true
		}
	}
	return inherited
}

func (x *interestIndex) route(change resourceChange) {
	recipients := map[ClusterId]struct{}{}
	for _, res := range []*cache.Resource{change.newRes, change.oldRes} {
		info, ok := resourceInfo(res)
		if !ok {
			continue
		}
		for target := range x.byResource[info.ResourceKey] {
			recipients[target] = struct{}{}
		}
		for _, ownerRef := range info.OwnerRefs {
			ownerKey, err := ownerRefResourceKey(ownerRef, info.ResourceKey.Namespace)
			if err != nil {
				continue
			}
			for target := range x.byResource[ownerKey] {
				recipients[target] = struct{}{}
			}
		}
	}

	for target := range recipients {
		changes, ok := x.subscribers[target]
		if !ok {
			continue
		}
		select {
		case changes <- change:
		case <-x.stopped:
			return
		}
	}
}

func resourceInfo(res *cache.Resource) (ResourceInfo, bool) {
	if res == nil {
		return ResourceInfo{}, false
	}
	info, ok := res.Info.(ResourceInfo)
	return info, ok
}
