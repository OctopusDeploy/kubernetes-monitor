package cluster

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/client-go/rest"
)

func newStubAPIServer(t *testing.T) *rest.Config {
	t.Helper()
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api":
			_, _ = w.Write(
				[]byte(
					`{"kind":"APIVersions","versions":["v1"],"serverAddressByClientCIDRs":[{"clientCIDR":"0.0.0.0/0","serverAddress":""}]}`,
				),
			)
		case "/apis":
			_, _ = w.Write([]byte(`{"kind":"APIGroupList","apiVersion":"v1","groups":[]}`))
		case "/api/v1":
			_, _ = w.Write(
				[]byte(
					`{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"v1","resources":[{"name":"pods","singularName":"pod","namespaced":true,"kind":"Pod","verbs":["get","list","watch"]}]}`,
				),
			)
		case "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews":
			_, _ = w.Write(
				[]byte(
					`{"kind":"SelfSubjectRulesReview","apiVersion":"authorization.k8s.io/v1","status":{"resourceRules":[{"verbs":["*"],"apiGroups":["*"],"resources":["*"]}],"nonResourceRules":[],"incomplete":false}}`,
				),
			)
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","code":404}`))
		}
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(server.Close)
	return &rest.Config{Host: server.URL}
}

func TestEnsureCluster_CreatesCluster(t *testing.T) {
	restConfig := newStubAPIServer(t)
	clusterList := NewClusterList(t.Context(), restConfig, discardLogger(), NoOpUpdater{}, nil, false)
	clusterId := ClusterId("cluster-id")

	cluster, err := clusterList.EnsureCluster(t.Context(), clusterId)
	if err != nil {
		t.Errorf("Failed to ensure cluster: %s", err.Error())
	}

	if diff := cmp.Diff(clusterId, cluster.ClusterId); diff != "" {
		t.Error(diff)
	}

	applicationInstanceCount, err := ask(t.Context(), cluster.mailbox, func(list *ApplicationInstanceList) int {
		return len(list.applicationInstances)
	})
	if err != nil {
		t.Fatalf("reading application instances: %s", err.Error())
	}
	if applicationInstanceCount != 0 {
		t.Errorf("Found %d application instances, expected none", applicationInstanceCount)
	}
}

func TestEnsureCluster_ReturnsExistingCluster(t *testing.T) {
	restConfig := newStubAPIServer(t)
	clusterList := NewClusterList(t.Context(), restConfig, discardLogger(), NoOpUpdater{}, nil, false)
	clusterId := ClusterId("cluster-id")

	existing, err := clusterList.EnsureCluster(t.Context(), clusterId)
	if err != nil {
		t.Fatalf("Failed to ensure cluster during test setup: %s", err.Error())
	}

	actual, err := clusterList.EnsureCluster(t.Context(), clusterId)
	if err != nil {
		t.Errorf("Failed to ensure cluster: %s", err.Error())
	}

	if diff := cmp.Diff(clusterId, actual.ClusterId); diff != "" {
		t.Error(diff)
	}

	if actual != existing {
		t.Error("expected the existing cluster to be returned")
	}
}

func TestEnsureCluster_DifferentIdsShareOneClusterConnection(t *testing.T) {
	restConfig := newStubAPIServer(t)
	clusterList := NewClusterList(t.Context(), restConfig, discardLogger(), NoOpUpdater{}, nil, false)

	first, err := clusterList.EnsureCluster(t.Context(), "first")
	if err != nil {
		t.Fatalf("Failed to ensure first cluster: %s", err.Error())
	}
	second, err := clusterList.EnsureCluster(t.Context(), "second")
	if err != nil {
		t.Fatalf("Failed to ensure second cluster: %s", err.Error())
	}

	if first == second {
		t.Error("expected a distinct target per cluster id")
	}
	if first.shared != second.shared {
		t.Error("expected every target to share one cluster cache")
	}
}
