package kwok_integration

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	appsv1 "k8s.io/api/apps/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

const touchAnnotation = "kwok-integration/touch"

func TestTargetsOnOneClusterOnlyReceiveTheirOwnResources(t *testing.T) {
	feature := features.New("multiple targets/separate deployments").
		Assess("sweeps and incremental updates are routed to the target that desires them", func(
			ctx context.Context, t *testing.T, cfg *envconf.Config,
		) context.Context {
			alpha, bravo := "multi-alpha", "multi-bravo"
			createIsolatedDeployment(ctx, t, cfg, alpha)
			createIsolatedDeployment(ctx, t, cfg, bravo)
			waitForDeployment(ctx, t, cfg, alpha)
			waitForDeployment(ctx, t, cfg, bravo)

			recorder := &recordingUpdater{}
			clusters := createTestClusterList(t, cfg, recorder, nil)
			alphaTarget := ensureTestTarget(t, clusters, "machine-alpha")
			bravoTarget := ensureTestTarget(t, clusters, "machine-bravo")
			replaceDesiredDeployments(ctx, t, cfg, alphaTarget, alpha)
			replaceDesiredDeployments(ctx, t, cfg, bravoTarget, bravo)

			swept := sweepByTarget(ctx, clusters)
			assertMonitorsOnly(t, swept[alphaTarget.ClusterId], cfg.Namespace(), alpha)
			assertMonitorsOnly(t, swept[bravoTarget.ClusterId], cfg.Namespace(), bravo)

			mark := recorder.len()
			require.NoError(t, touchDeployment(ctx, cfg, alpha, "alpha-1"))
			require.NoError(t, touchDeployment(ctx, cfg, bravo, "bravo-1"))
			waitForTouch(t, recorder, mark, alphaTarget.ClusterId, alpha, "alpha-1")
			waitForTouch(t, recorder, mark, bravoTarget.ClusterId, bravo, "bravo-1")

			for _, call := range recorder.since(0) {
				switch call.changes.ClusterId {
				case alphaTarget.ClusterId:
					assert.False(t, mentions(call.changes, bravo), "alpha's target was sent bravo: %+v", call)
				case bravoTarget.ClusterId:
					assert.False(t, mentions(call.changes, alpha), "bravo's target was sent alpha: %+v", call)
				default:
					t.Errorf("unexpected target %s", call.changes.ClusterId)
				}
			}
			return ctx
		}).
		Feature()

	testenv.Test(t, feature)
}

func TestTargetsSharingADeploymentAreIndependent(t *testing.T) {
	feature := features.New("multiple targets/shared deployment").
		Assess("one target dropping the deployment doesn't stop the other's updates", func(
			ctx context.Context, t *testing.T, cfg *envconf.Config,
		) context.Context {
			shared := "multi-shared"
			createIsolatedDeployment(ctx, t, cfg, shared)
			waitForDeployment(ctx, t, cfg, shared)

			recorder := &recordingUpdater{}
			clusters := createTestClusterList(t, cfg, recorder, nil)
			first := ensureTestTarget(t, clusters, "machine-first")
			second := ensureTestTarget(t, clusters, "machine-second")
			replaceDesiredDeployments(ctx, t, cfg, first, shared)
			replaceDesiredDeployments(ctx, t, cfg, second, shared)

			swept := sweepByTarget(ctx, clusters)
			assertMonitorsOnly(t, swept[first.ClusterId], cfg.Namespace(), shared)
			assertMonitorsOnly(t, swept[second.ClusterId], cfg.Namespace(), shared)

			mark := recorder.len()
			require.NoError(t, touchDeployment(ctx, cfg, shared, "both"))
			waitForTouch(t, recorder, mark, first.ClusterId, shared, "both")
			waitForTouch(t, recorder, mark, second.ClusterId, shared, "both")

			removedAt := removeAllDesiredResources(ctx, t, recorder, first)

			require.NoError(t, touchDeployment(ctx, cfg, shared, "second-only"))
			waitForTouch(t, recorder, removedAt, second.ClusterId, shared, "second-only")
			assertNoUpdatesMentioning(t, recorder, removedAt, first.ClusterId, shared)

			swept = sweepByTarget(ctx, clusters)
			require.Len(t, swept[first.ClusterId], 1)
			assert.Zero(t, swept[first.ClusterId][0].GetAllResourceCount())
			assertMonitorsOnly(t, swept[second.ClusterId], cfg.Namespace(), shared)
			return ctx
		}).
		Feature()

	testenv.Test(t, feature)
}

func TestResourceChangesRacingDesiredResourceRemoval(t *testing.T) {
	feature := features.New("multiple targets/removal races").
		Assess("modifications racing removal aren't sent after the removal", func(
			ctx context.Context, t *testing.T, cfg *envconf.Config,
		) context.Context {
			racing := "race-modified"
			createIsolatedDeployment(ctx, t, cfg, racing)
			waitForDeployment(ctx, t, cfg, racing)

			recorder := &recordingUpdater{}
			clusters := createTestClusterList(t, cfg, recorder, nil)
			racer := ensureTestTarget(t, clusters, "machine-racer")
			// The observer keeps desiring the deployment, proving the watch delivered changes after the removal.
			observer := ensureTestTarget(t, clusters, "machine-observer")
			replaceDesiredDeployments(ctx, t, cfg, observer, racing)

			const touchesPerRound = 20
			for round := range 5 {
				replaceDesiredDeployments(ctx, t, cfg, racer, racing)

				// Without this, the removal could trivially win against changes the racer never subscribed to.
				mark := recorder.len()
				ready := fmt.Sprintf("ready-%d", round)
				require.NoError(t, touchDeployment(ctx, cfg, racing, ready))
				waitForTouch(t, recorder, mark, racer.ClusterId, racing, ready)

				removeAfter := round * touchesPerRound / 5
				removing := make(chan struct{})
				burst := make(chan error, 1)
				go func() {
					defer close(burst)
					for i := range touchesPerRound {
						if i == removeAfter {
							close(removing)
						}
						if err := touchDeployment(ctx, cfg, racing, fmt.Sprintf("race-%d-%d", round, i)); err != nil {
							burst <- err
							return
						}
					}
				}()

				<-removing
				removedAt := removeAllDesiredResources(ctx, t, recorder, racer)
				require.NoError(t, <-burst)

				settled := fmt.Sprintf("settled-%d", round)
				require.NoError(t, touchDeployment(ctx, cfg, racing, settled))
				waitForTouch(t, recorder, removedAt, observer.ClusterId, racing, settled)
				assertNoUpdatesMentioning(t, recorder, removedAt, racer.ClusterId, racing)
			}

			swept := sweepByTarget(ctx, clusters)
			require.Len(t, swept[racer.ClusterId], 1)
			assert.Zero(t, swept[racer.ClusterId][0].GetAllResourceCount())
			assertMonitorsOnly(t, swept[observer.ClusterId], cfg.Namespace(), racing)
			return ctx
		}).
		Assess("creation racing removal isn't sent after the removal", func(
			ctx context.Context, t *testing.T, cfg *envconf.Config,
		) context.Context {
			created := "race-created"

			recorder := &recordingUpdater{}
			clusters := createTestClusterList(t, cfg, recorder, nil)
			racer := ensureTestTarget(t, clusters, "machine-racer")
			observer := ensureTestTarget(t, clusters, "machine-observer")

			desired := desiredResourceMap(new(newDesiredDeployment(created, cfg.Namespace())))
			require.NoError(t, observer.ReplaceDesiredResources(ctx, testApplicationInstanceId, desired, testHashSalt))
			require.NoError(t, racer.ReplaceDesiredResources(ctx, testApplicationInstanceId, desired, testHashSalt))

			creating := make(chan error, 1)
			go func() {
				defer close(creating)
				if err := cfg.Client().Resources().Create(ctx, newIsolatedDeployment(cfg.Namespace(), created)); err != nil {
					creating <- err
				}
			}()
			removedAt := removeAllDesiredResources(ctx, t, recorder, racer)
			require.NoError(t, <-creating)

			// Waiting for availability lets the ReplicaSet and Pod churn through the cache too.
			waitForDeployment(ctx, t, cfg, created)
			require.NoError(t, touchDeployment(ctx, cfg, created, "settled"))
			waitForTouch(t, recorder, removedAt, observer.ClusterId, created, "settled")
			assertNoUpdatesMentioning(t, recorder, removedAt, racer.ClusterId, created)

			swept := sweepByTarget(ctx, clusters)
			require.Len(t, swept[racer.ClusterId], 1)
			assert.Zero(t, swept[racer.ClusterId][0].GetAllResourceCount())
			assertMonitorsOnly(t, swept[observer.ClusterId], cfg.Namespace(), created)
			return ctx
		}).
		Feature()

	testenv.Test(t, feature)
}

type updaterCall struct {
	replace bool
	changes *cluster.ApplicationInstanceChanges
}

// recordingUpdater is called concurrently from every target's goroutine and from callers replacing desired
// resources, so the order it records is the order the updater saw.
type recordingUpdater struct {
	mu    sync.Mutex
	calls []updaterCall
}

func (r *recordingUpdater) Update(_ context.Context, changes *cluster.ApplicationInstanceChanges) {
	r.record(updaterCall{changes: changes})
}

func (r *recordingUpdater) Replace(_ context.Context, changes *cluster.ApplicationInstanceChanges) {
	r.record(updaterCall{replace: true, changes: changes})
}

func (r *recordingUpdater) record(call updaterCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *recordingUpdater) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recordingUpdater) since(index int) []updaterCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls[index:])
}

// removeAllDesiredResources returns the index just past the target's replacement, after which nothing may
// mention a resource it used to desire.
func removeAllDesiredResources(
	ctx context.Context, t *testing.T, recorder *recordingUpdater, target *testTarget,
) int {
	t.Helper()
	before := recorder.len()
	require.NoError(t, target.ReplaceDesiredResources(
		ctx, testApplicationInstanceId, map[kube.ResourceKey]*cluster.DesiredResource{}, testHashSalt,
	))

	calls := recorder.since(before)
	index := slices.IndexFunc(calls, func(call updaterCall) bool {
		return call.replace && call.changes.ClusterId == target.ClusterId
	})
	require.GreaterOrEqual(t, index, 0, "the removal wasn't sent as a replacement")
	assert.Zero(t, calls[index].changes.GetAllResourceCount())
	return before + index + 1
}

func replaceDesiredDeployments(
	ctx context.Context, t *testing.T, cfg *envconf.Config, target *testTarget, names ...string,
) {
	t.Helper()
	desired := make([]*cluster.DesiredResource, 0, len(names))
	for _, name := range names {
		desired = append(desired, mapToDesiredResource(cfg, *GetCurrentDeployment(ctx, t, cfg, name)))
	}
	require.NoError(t, target.ReplaceDesiredResources(
		ctx, testApplicationInstanceId, desiredResourceMap(desired...), testHashSalt,
	))
}

func sweepByTarget(
	ctx context.Context, clusters *cluster.ClusterList,
) map[cluster.ClusterId][]*cluster.ApplicationInstanceChanges {
	swept := map[cluster.ClusterId][]*cluster.ApplicationInstanceChanges{}
	for changes := range clusters.ApplicationInstanceUpdates(ctx) {
		swept[changes.ClusterId] = append(swept[changes.ClusterId], changes)
	}
	return swept
}

func assertMonitorsOnly(
	t *testing.T, changes []*cluster.ApplicationInstanceChanges, namespace string, deployment string,
) {
	t.Helper()
	require.Len(t, changes, 1)
	assert.ElementsMatch(t,
		createComparableResourcesForDeployment(deployment, namespace),
		createComparableResourcesForApplicationInstanceUpdate(changes[0]),
	)
	assert.Empty(t, changes[0].MissingMonitoredResources)
	assert.Empty(t, changes[0].UnknownMonitoredResources)
}

func waitForTouch(
	t *testing.T, recorder *recordingUpdater, since int, target cluster.ClusterId, deployment string, token string,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		return slices.ContainsFunc(recorder.since(since), func(call updaterCall) bool {
			return !call.replace && call.changes.ClusterId == target && carriesTouch(call.changes, deployment, token)
		})
	}, time.Minute, 100*time.Millisecond, "%s never received %s touched with %q", target, deployment, token)
}

// assertNoUpdatesMentioning waits a little because the target processes its queue on its own goroutine, so
// a change routed to it may still be in flight after another target has received it.
func assertNoUpdatesMentioning(
	t *testing.T, recorder *recordingUpdater, since int, target cluster.ClusterId, deployment string,
) {
	t.Helper()
	sentAfterRemoval := func() []updaterCall {
		var sent []updaterCall
		for _, call := range recorder.since(since) {
			if call.changes.ClusterId == target && mentions(call.changes, deployment) {
				sent = append(sent, call)
			}
		}
		return sent
	}
	assert.Never(t, func() bool { return len(sentAfterRemoval()) > 0 }, 2*time.Second, 100*time.Millisecond)
	for _, call := range sentAfterRemoval() {
		t.Errorf("%s was sent %s after removing it (replace=%t): %+v",
			target, deployment, call.replace, describeChanges(call.changes))
	}
}

func carriesTouch(changes *cluster.ApplicationInstanceChanges, deployment string, token string) bool {
	return slices.ContainsFunc(changes.PresentMonitoredResources, func(resource *cluster.PresentMonitoredResource) bool {
		return resource.Name == deployment && resource.Manifest != nil &&
			resource.Manifest.Object["metadata"].(map[string]any)["annotations"] != nil &&
			resource.Manifest.Object["metadata"].(map[string]any)["annotations"].(map[string]any)[touchAnnotation] == token
	})
}

// mentions reports whether changes refer to the deployment or anything it owns. Owned resources are named
// after the deployment, and isolated deployments' names don't prefix one another.
func mentions(changes *cluster.ApplicationInstanceChanges, deployment string) bool {
	owned := func(name string) bool { return name == deployment || strings.HasPrefix(name, deployment+"-") }
	return slices.ContainsFunc(changes.PresentMonitoredResources, func(r *cluster.PresentMonitoredResource) bool {
		return owned(r.Name)
	}) || slices.ContainsFunc(changes.ChildMonitoredResources, func(r *cluster.ChildMonitoredResource) bool {
		return owned(r.Name)
	}) || slices.ContainsFunc(changes.MissingMonitoredResources, func(r *cluster.MissingMonitoredResource) bool {
		return owned(r.Name)
	}) || slices.ContainsFunc(changes.DeletedChildResourceKeys, func(key kube.ResourceKey) bool {
		return owned(key.Name)
	})
}

func describeChanges(changes *cluster.ApplicationInstanceChanges) []string {
	var described []string
	for _, r := range changes.PresentMonitoredResources {
		described = append(described, "present "+r.GroupVersionKind.Kind+"/"+r.Name)
	}
	for _, r := range changes.ChildMonitoredResources {
		described = append(described, "child "+r.GroupVersionKind.Kind+"/"+r.Name)
	}
	for _, r := range changes.MissingMonitoredResources {
		described = append(described, "missing "+r.GroupVersionKind.Kind+"/"+r.Name)
	}
	for _, r := range changes.UnknownMonitoredResources {
		described = append(described, fmt.Sprintf("unknown %s", r.DesiredResourceId))
	}
	for _, key := range changes.DeletedChildResourceKeys {
		described = append(described, "deleted child "+key.Kind+"/"+key.Name)
	}
	return described
}

// touchDeployment changes only metadata, so the deployment's children are left alone.
func touchDeployment(ctx context.Context, cfg *envconf.Config, name string, token string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var deployment appsv1.Deployment
		if err := cfg.Client().Resources().Get(ctx, name, cfg.Namespace(), &deployment); err != nil {
			return err
		}
		if deployment.Annotations == nil {
			deployment.Annotations = map[string]string{}
		}
		deployment.Annotations[touchAnnotation] = token
		return cfg.Client().Resources().Update(ctx, &deployment)
	})
}

// newIsolatedDeployment selects only its own pods; basic deployments share a selector, so their pods would
// all match one another's.
func newIsolatedDeployment(namespace string, name string) *appsv1.Deployment {
	deployment := newBasicDeployment(namespace, name, 1)
	labels := map[string]string{"app": name}
	deployment.Labels = labels
	deployment.Spec.Selector.MatchLabels = labels
	deployment.Spec.Template.Labels = labels
	return deployment
}

func createIsolatedDeployment(ctx context.Context, t *testing.T, cfg *envconf.Config, name string) {
	t.Helper()
	require.NoError(t, cfg.Client().Resources().Create(ctx, newIsolatedDeployment(cfg.Namespace(), name)))
}
