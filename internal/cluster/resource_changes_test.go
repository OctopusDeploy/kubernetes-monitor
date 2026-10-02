package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func versionOf(key kube.ResourceKey, resourceVersion string) *cache.Resource {
	res := testResource(key, "apps/v1", nil)
	res.ResourceVersion = resourceVersion
	return res
}

func nextChange(t *testing.T, q *changeQueue) resourceChange {
	t.Helper()
	select {
	case change := <-q.out:
		return change
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a queued change")
		return resourceChange{}
	}
}

func TestChangeQueue(t *testing.T) {
	t.Run("Folds repeat changes into one, keeping the original previous state", func(t *testing.T) {
		q := newChangeQueue(t.Context())
		original, first, latest := versionOf(
			deploymentKey,
			"1",
		), versionOf(
			deploymentKey,
			"2",
		), versionOf(
			deploymentKey,
			"3",
		)

		q.in <- resourceChange{newRes: first, oldRes: original}
		q.in <- resourceChange{newRes: latest, oldRes: first}
		q.in <- resourceChange{newRes: versionOf(otherKey, "1")}

		folded := nextChange(t, q)
		assert.Same(t, latest, folded.newRes)
		assert.Same(t, original, folded.oldRes)
		assert.Equal(t, otherKey, nextChange(t, q).key(), "the repeat changes should have produced only one entry")
	})

	t.Run("Delivers resources in the order they were first changed", func(t *testing.T) {
		q := newChangeQueue(t.Context())

		q.in <- resourceChange{newRes: versionOf(deploymentKey, "1")}
		q.in <- resourceChange{newRes: versionOf(otherKey, "1")}
		q.in <- resourceChange{newRes: versionOf(deploymentKey, "2")}

		assert.Equal(t, deploymentKey, nextChange(t, q).key())
		assert.Equal(t, otherKey, nextChange(t, q).key())
	})

	t.Run("Keeps a removal that follows an update", func(t *testing.T) {
		q := newChangeQueue(t.Context())
		live := versionOf(deploymentKey, "1")

		q.in <- resourceChange{newRes: versionOf(deploymentKey, "2"), oldRes: live}
		q.in <- resourceChange{oldRes: versionOf(deploymentKey, "2")}

		removal := nextChange(t, q)
		assert.Nil(t, removal.newRes)
		assert.Same(t, live, removal.oldRes)
	})

	t.Run("Folds a resource created then deleted before delivery into a change with neither side", func(t *testing.T) {
		q := newChangeQueue(t.Context())
		created := versionOf(deploymentKey, "1")

		q.in <- resourceChange{newRes: created}
		q.in <- resourceChange{oldRes: created}

		folded := nextChange(t, q)
		assert.Nil(t, folded.newRes)
		assert.Nil(t, folded.oldRes)
	})

	// The cache routes changes while holding its lock, so a busy target must never make it wait.
	t.Run("Accepts changes while nothing is reading them", func(t *testing.T) {
		q := newChangeQueue(t.Context())
		const resources = 1000

		accepted := make(chan struct{})
		go func() {
			defer close(accepted)
			for i := range resources {
				key := kube.NewResourceKey("apps", "Deployment", "default", fmt.Sprintf("deployment-%d", i))
				q.in <- resourceChange{newRes: versionOf(key, "1")}
			}
		}()

		select {
		case <-accepted:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "the queue stopped accepting changes while its reader was busy")
		}

		for range resources {
			nextChange(t, q)
		}
	})
}
