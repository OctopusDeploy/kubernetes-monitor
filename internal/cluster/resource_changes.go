package cluster

import (
	"context"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
)

// Either side is nil when the resource was added or removed.
type resourceChange struct {
	newRes *cache.Resource
	oldRes *cache.Resource
}

func (c resourceChange) key() kube.ResourceKey {
	if c.newRes != nil {
		return c.newRes.ResourceKey()
	}
	return c.oldRes.ResourceKey()
}

// changeQueue has to accept changes immediately even while its target is busy, because the cluster cache
// routes them while holding its lock. Repeat changes to a queued resource are folded into it, keeping the
// original previous state, so the queue holds at most one change per resource.
type changeQueue struct {
	in  chan resourceChange
	out chan resourceChange
}

func newChangeQueue(ctx context.Context) *changeQueue {
	q := &changeQueue{
		in:  make(chan resourceChange),
		out: make(chan resourceChange),
	}
	go q.run(ctx)
	return q
}

func (q *changeQueue) run(ctx context.Context) {
	var order []kube.ResourceKey
	queued := map[kube.ResourceKey]resourceChange{}

	for {
		var out chan<- resourceChange
		var next resourceChange
		if len(order) > 0 {
			out = q.out
			next = queued[order[0]]
		}

		select {
		case <-ctx.Done():
			return

		case change := <-q.in:
			key := change.key()
			if existing, ok := queued[key]; ok {
				existing.newRes = change.newRes
				queued[key] = existing
				continue
			}
			queued[key] = change
			order = append(order, key)

		case out <- next:
			delete(queued, order[0])
			order = order[1:]
			if len(order) == 0 {
				order = nil
			}
		}
	}
}
