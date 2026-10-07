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
	keysByTarget      map[ClusterId]map[kube.ResourceKey]struct{}
	targetsByResource map[kube.ResourceKey]map[ClusterId]struct{}
	subscribers       map[ClusterId]chan<- resourceChange
	stopped           <-chan struct{}
}

func newResourceInterests(ctx context.Context, logger *slog.Logger) *resourceInterests {
	r := &resourceInterests{
		logger:  logger,
		mailbox: newMailbox[interestIndex](ctx.Done()),
	}
	go r.mailbox.serve(&interestIndex{
		keysByTarget:      map[ClusterId]map[kube.ResourceKey]struct{}{},
		targetsByResource: map[kube.ResourceKey]map[ClusterId]struct{}{},
		subscribers:       map[ClusterId]chan<- resourceChange{},
		stopped:           ctx.Done(),
	})
	return r
}

// There's no unsubscribe because every target shares the index's lifetime (the ClusterList's), so a target
// never stops reading while the index routes to it. Removing a target on its own would need one that drops
// both its subscriber and its keys, and route would need to stop waiting on a target that has gone.
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
		if index.isResourceInIndex(info.ResourceKey) {
			return true
		}
		// Roots have no owners to inherit interest from.
		return !isRoot && index.inheritInterestFromOwners(info)
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
	for key := range x.keysByTarget[target] {
		if _, kept := keys[key]; !kept {
			x.removeTargetFromKey(key, target)
		}
	}
	for key := range keys {
		x.addTargetToKey(key, target)
	}
	x.keysByTarget[target] = keys
}

func (x *interestIndex) remember(target ClusterId, key kube.ResourceKey) {
	x.addKeyToTarget(key, target)
	x.addTargetToKey(key, target)
}

func (x *interestIndex) addKeyToTarget(key kube.ResourceKey, target ClusterId) {
	keys, ok := x.keysByTarget[target]
	if !ok {
		keys = map[kube.ResourceKey]struct{}{}
		x.keysByTarget[target] = keys
	}
	keys[key] = struct{}{}
}

func (x *interestIndex) addTargetToKey(key kube.ResourceKey, target ClusterId) {
	targets, ok := x.targetsByResource[key]
	if !ok {
		targets = map[ClusterId]struct{}{}
		x.targetsByResource[key] = targets
	}
	targets[target] = struct{}{}
}

func (x *interestIndex) removeTargetFromKey(key kube.ResourceKey, target ClusterId) {
	targets := x.targetsByResource[key]
	delete(targets, target)
	if len(targets) == 0 {
		delete(x.targetsByResource, key)
	}
}

func (x *interestIndex) isResourceInIndex(key kube.ResourceKey) bool {
	return len(x.targetsByResource[key]) > 0
}

// inheritInterestFromOwners makes a target interested in a resource owned by something it's interested in,
// because it will monitor the resource as a child; that lets the resource's own children be recognised while
// the cache lists the cluster. It reports whether any target inherited the resource.
//
// Only direct owners are checked, so a resource populated before its owner was inherited (a grandchild
// listed before its parent) is missed here. It's picked up when the cache next populates it, on its next
// watch event or relist, or when the target's periodic sweep rescans and republishes its children.
func (x *interestIndex) inheritInterestFromOwners(info ResourceInfo) bool {
	inherited := false
	for _, ownerRef := range info.OwnerRefs {
		ownerKey, err := ownerRefResourceKey(ownerRef, info.ResourceKey.Namespace)
		if err != nil {
			continue
		}
		for target := range x.targetsByResource[ownerKey] {
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
		for target := range x.targetsByResource[info.ResourceKey] {
			recipients[target] = struct{}{}
		}
		for _, ownerRef := range info.OwnerRefs {
			ownerKey, err := ownerRefResourceKey(ownerRef, info.ResourceKey.Namespace)
			if err != nil {
				continue
			}
			for target := range x.targetsByResource[ownerKey] {
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
