//go:build memory_profile

// Memory profile harness. See memory-profile.md for how to run it.

package kwok_integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/pyroscope-go"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

const (
	memTestPyroscopeURL = "http://localhost:5053"
	memTestGrafanaURL   = "http://localhost:5054"
	memTestOutputDir    = "testdata/memory-profiles"
)

// Small scenario used to validate the test scaffolding itself.
func TestCacheMemoryProfile_Smoke(t *testing.T) {
	runMemoryProfileFeature(t, memTestConfig{
		deployments:       200,
		replicas:          5,
		desiredPct:        0.05,
		createConcurrency: 32,
		qps:               200,
		burst:             400,
		holdSeconds:       10,
	})
}

func TestCacheMemoryProfile_1PercentDesired(t *testing.T) {
	runMemoryProfileFeature(t, memTestConfig{
		deployments:       3000,
		replicas:          15,
		desiredPct:        0.01,
		createConcurrency: 64,
		qps:               1000,
		burst:             2000,
		holdSeconds:       30,
	})
}

func TestCacheMemoryProfile_10PercentDesired(t *testing.T) {
	runMemoryProfileFeature(t, memTestConfig{
		deployments:       3000,
		replicas:          15,
		desiredPct:        0.10,
		createConcurrency: 64,
		qps:               1000,
		burst:             2000,
		holdSeconds:       30,
	})
}

func runMemoryProfileFeature(t *testing.T, cfg memTestConfig) {
	t.Helper()
	t.Logf("memory test config: %+v", cfg)

	namespace, namePrefix, restored := prepareResourcesOrRestore(t, cfg)

	feat := features.New("cache memory profile").
		Setup(func(ctx context.Context, t *testing.T, env *envconf.Config) context.Context {
			if restored {
				return ctx
			}
			namespace = env.Namespace()
			waitForDefaultServiceAccount(t, env)
			client := highQPSClient(t, env, cfg)
			createDeployments(ctx, t, client, namespace, namePrefix, cfg)
			return ctx
		}).
		Assess("sync real cluster cache and hold state", func(ctx context.Context, t *testing.T, env *envconf.Config) context.Context {
			profileCluster(ctx, t, env, namespace, namePrefix, cfg)
			return ctx
		}).
		Feature()

	testenv.Test(t, feat)
}

// Restore wipes env.Namespace(), so on the restore path we return the snapshot's namespace.
func prepareResourcesOrRestore(t *testing.T, cfg memTestConfig) (namespace, namePrefix string, restored bool) {
	t.Helper()
	defaultPrefix := sanitizeName(fmt.Sprintf("mem-%s", strings.ToLower(t.Name())))

	if !snapshotExists(cfg) {
		t.Logf("no snapshot for %s — resources will be created via the API", snapshotBasename(cfg))
		return "", defaultPrefix, false
	}

	if kwokClusterName == "" {
		t.Fatal("kwokClusterName is empty — TestMain did not set it")
	}

	dbPath, manifestPath := snapshotPaths(cfg)
	t.Logf("restoring snapshot %s (cluster=%s)", dbPath, kwokClusterName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := snapshotRestore(ctx, kwokClusterName, dbPath); err != nil {
		t.Fatalf("snapshot restore: %v", err)
	}
	m, err := readSnapshotManifest(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	t.Logf("restored snapshot (ns=%s, namePrefix=%s, %d deployments x %d replicas)",
		m.Namespace, m.NamePrefix, m.Deployments, m.Replicas)
	return m.Namespace, m.NamePrefix, true
}

func profileCluster(
	ctx context.Context,
	t *testing.T,
	cfg *envconf.Config,
	namespace, namePrefix string,
	opts memTestConfig,
) {
	t.Helper()

	if namespace == "" {
		namespace = cfg.Namespace()
	}
	client := highQPSClient(t, cfg, opts)
	testCluster := createTestClusterWithQPS(t, cfg, opts)
	seedDesiredResources(ctx, t, client, namespace, testCluster, namePrefix, opts)
	client = nil // Drop so the heap profile only reflects the cluster cache.

	tags := map[string]string{
		"test":        t.Name(),
		"deployments": strconv.Itoa(opts.deployments),
		"replicas":    strconv.Itoa(int(opts.replicas)),
		"desired_pct": strconv.FormatFloat(opts.desiredPct, 'f', -1, 64),
		"phase":       "populated",
	}

	pyroStop, pyroUp := tryStartPyroscope(t, tags)
	if pyroUp {
		t.Logf("grafana (this run, inuse heap): %s", thisRunLink(t.Name()))
		t.Logf("grafana (per-test comparison): %s", perTestComparisonLink())
		defer func() {
			if err := pyroStop.Stop(); err != nil {
				t.Logf("pyroscope stop: %v", err)
			}
		}()
	} else {
		t.Logf("pyroscope not reachable at %s, falling back to pprof heap dumps in %s",
			memTestPyroscopeURL, memTestOutputDir)
		writeHeapDump(t, "baseline")
	}

	syncStart := time.Now()
	if err := testCluster.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	t.Logf("cluster cache synced in %s", time.Since(syncStart))

	forceGC()
	reportRuntimeMemStats(t)
	// Without KeepAlive the cache is eligible for GC before we sample it.
	runtime.KeepAlive(testCluster)

	if !pyroUp {
		writeHeapDump(t, "populated")
	} else {
		t.Logf("holding state for %ds so pyroscope can sample the heap", opts.holdSeconds)
		// GC once per scrape interval so every sample reflects live heap only.
		holdWithPeriodicGC(time.Duration(opts.holdSeconds) * time.Second)
	}

	runtime.KeepAlive(testCluster)
}

// seedDesiredResources marks the first desiredPct of deployments as desired.
func seedDesiredResources(
	ctx context.Context, t *testing.T, client kubernetes.Interface, namespace string,
	testCluster *cluster.Cluster, namePrefix string, opts memTestConfig,
) {
	t.Helper()
	desiredTarget := int(float64(opts.deployments) * opts.desiredPct)
	if desiredTarget < 1 && opts.desiredPct > 0 {
		desiredTarget = 1
	}

	desired := make([]*cluster.DesiredResource, 0, desiredTarget)
	for i := 0; i < desiredTarget; i++ {
		name := fmt.Sprintf("%s-%d", namePrefix, i)
		d, err := client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get deployment %s: %v", name, err)
		}
		dr := mapDeploymentToDesiredResource(*d)
		if dr == nil {
			t.Fatalf("failed to map %s", name)
		}
		desired = append(desired, dr)
	}

	appInstance := cluster.NewApplicationInstanceBuilder().
		WithDesiredResources(desired).
		Build()
	testCluster.ApplicationInstances.UpsertApplicationInstance(&appInstance)
	t.Logf("marked %d/%d deployments as desired", desiredTarget, opts.deployments)
}

// mapDeploymentToDesiredResource is the lightweight version of
// deployments.go's mapToDesiredResource that skips the scheme lookup.
func mapDeploymentToDesiredResource(d appsv1.Deployment) *cluster.DesiredResource {
	manifest := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]interface{}{
			"name":      d.Name,
			"namespace": d.Namespace,
		},
	}}
	dr := cluster.NewDesiredResourceBuilder().
		WithName(d.Name).
		WithNamespace(d.Namespace).
		WithKind("Deployment").
		WithApiVersion("apps/v1").
		WithManifest(manifest).
		Build()
	return &dr
}

// createTestClusterWithQPS mirrors cluster.go's createTestCluster but threads
// a QPS-overridden rest.Config through to both the cluster cache and the
// clientset. The vanilla e2e-framework REST config defaults to 5 QPS, which
// makes sync on a populated cluster sluggish.
func createTestClusterWithQPS(t *testing.T, cfg *envconf.Config, opts memTestConfig) *cluster.Cluster {
	t.Helper()
	restCfg := withQPS(cfg.Client().RESTConfig(), opts)

	applicationInstances := cluster.NewApplicationInstanceList()
	clusterCache, _, err := cluster.NewCache(
		t.Context(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		restCfg,
		nil,
		applicationInstances,
		nil,
		false,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		t.Fatalf("NewForConfig: %v", err)
	}
	dynamicClient, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		t.Fatalf("dynamic.NewForConfig: %v", err)
	}
	cachedDiscoveryClient := memory.NewMemCacheClient(clientset.DiscoveryClient)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return cluster.NewCluster(
		"cluster-id",
		applicationInstances,
		logger,
		clusterCache,
		nil,
		cachedDiscoveryClient,
		clientset,
		dynamicClient,
		cluster.NoOpUpdater{},
		false,
		nil,
	)
}

// Two GCs to clear floating garbage from finalizers; FreeOSMemory releases to the OS.
func forceGC() {
	runtime.GC()
	runtime.GC()
	debug.FreeOSMemory()
}

func holdWithPeriodicGC(d time.Duration) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	deadline := time.After(d)
	for {
		select {
		case <-deadline:
			return
		case <-tick.C:
			forceGC()
		}
	}
}

func reportRuntimeMemStats(t *testing.T) {
	t.Helper()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.Logf("heap alloc=%s  sys=%s  heap_objects=%d  num_gc=%d",
		humanBytes(ms.HeapAlloc), humanBytes(ms.Sys), ms.HeapObjects, ms.NumGC)
}

func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// profileTypeID picks inuse_space (heap gauge) over alloc_space (allocation counter) so
// Grafana's time-series panel shows heap size rather than allocation rate.
func grafanaProfileLink(baseURL, testName, profileTypeID string, groupBy []string, labelSelector string) string {
	if groupBy == nil {
		groupBy = []string{}
	}
	panes := map[string]any{
		"a3j": map[string]any{
			"datasource": "pyroscope",
			"queries": []map[string]any{{
				"groupBy":           groupBy,
				"includeExemplars":  false,
				"labelSelector":     labelSelector,
				"profileIdSelector": []string{},
				"spanSelector":      []string{},
				"queryType":         "both",
				"refId":             "A",
				"datasource":        map[string]string{"type": "grafana-pyroscope-datasource", "uid": "pyroscope"},
				"profileTypeId":     profileTypeID,
			}},
			"range":   map[string]string{"from": "now-1h", "to": "now"},
			"compact": false,
		},
	}
	b, _ := json.Marshal(panes)
	return fmt.Sprintf("%s/explore?schemaVersion=1&panes=%s&orgId=1", baseURL, url.QueryEscape(string(b)))
}

func thisRunLink(testName string) string {
	sel := fmt.Sprintf(`{service_name="kubernetes-monitor-memory-test", test=%q}`, testName)
	return grafanaProfileLink(memTestGrafanaURL, testName, "memory:inuse_space:bytes:space:bytes", nil, sel)
}

func perTestComparisonLink() string {
	sel := `{service_name="kubernetes-monitor-memory-test"}`
	return grafanaProfileLink(memTestGrafanaURL, "", "memory:inuse_space:bytes:space:bytes", []string{"test"}, sel)
}

func tryStartPyroscope(t *testing.T, tags map[string]string) (*pyroscope.Profiler, bool) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, memTestPyroscopeURL+"/ready", nil)
	if err != nil {
		return nil, false
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, false
	}
	_ = resp.Body.Close()

	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(5)

	profiler, err := pyroscope.Start(pyroscope.Config{
		ApplicationName: "kubernetes-monitor-memory-test",
		ServerAddress:   memTestPyroscopeURL,
		Tags:            tags,
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
		},
	})
	if err != nil {
		t.Logf("pyroscope.Start failed: %v — falling back to pprof dumps", err)
		return nil, false
	}
	t.Logf("pyroscope active at %s with tags %v", memTestPyroscopeURL, tags)
	return profiler, true
}

func writeHeapDump(t *testing.T, label string) {
	t.Helper()
	if err := os.MkdirAll(memTestOutputDir, 0o755); err != nil {
		t.Fatalf("could not create output dir %s: %v", memTestOutputDir, err)
	}
	safeTestName := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	filename := filepath.Join(memTestOutputDir,
		fmt.Sprintf("heap-%s-%s-%s.pprof", safeTestName, label, time.Now().UTC().Format("20060102T150405Z")))
	f, err := os.Create(filename)
	if err != nil {
		t.Fatalf("could not create heap dump %s: %v", filename, err)
	}
	defer f.Close()

	runtime.GC()
	if err := pprof.Lookup("heap").WriteTo(f, 0); err != nil {
		t.Fatalf("heap profile write failed: %v", err)
	}
	t.Logf("heap dump written: %s", filename)
}
