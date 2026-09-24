package cluster

import (
	"context"
	"io"
	"log/slog"
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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	clusterList := NewClusterList(t.Context(), restConfig, logger, NoOpUpdater{}, nil, false)
	clusterId := ClusterId("cluster-id")

	cluster, err := clusterList.EnsureCluster(context.TODO(), clusterId)
	if err != nil {
		t.Errorf("Failed to ensure cluster: %s", err.Error())
	}

	if diff := cmp.Diff(clusterId, cluster.ClusterId); diff != "" {
		t.Error(diff)
	}

	emptyApplicationInstanceList := NewApplicationInstanceList()

	if diff := cmp.Diff(
		emptyApplicationInstanceList.applicationInstances.GetAsMap(),
		cluster.ApplicationInstances.applicationInstances.GetAsMap(),
	); diff != "" {
		t.Error(diff)
	}

	if cluster.onResourceUpdatedUnsubscribe == nil {
		t.Error("OnResourceUpdatedUnsubscribe should not be nil")
	}
}

func TestEnsureCluster_ReturnsExistingCluster(t *testing.T) {
	restConfig := newStubAPIServer(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	clusterList := NewClusterList(t.Context(), restConfig, logger, NoOpUpdater{}, nil, false)
	clusterId := ClusterId("cluster-id")

	_, err := clusterList.EnsureCluster(context.TODO(), clusterId)
	if err != nil {
		t.Fatalf("Failed to ensure cluster during test setup: %s", err.Error())
	}

	actual, err := clusterList.EnsureCluster(context.TODO(), clusterId)
	if err != nil {
		t.Errorf("Failed to ensure cluster: %s", err.Error())
	}

	if diff := cmp.Diff(clusterId, actual.ClusterId); diff != "" {
		t.Error(diff)
	}

	if len(clusterList.clusters) != 1 {
		t.Errorf("Found %d clusters, expected 1", len(clusterList.clusters))
	}
}
