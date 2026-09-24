package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes/fake"

	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakediscovery "k8s.io/client-go/discovery/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestPermissionResourceFilter_IsExcludedResource(t *testing.T) {
	tests := []struct {
		name     string
		allowed  map[schema.GroupKind]struct{}
		group    string
		kind     string
		expected bool
	}{
		{
			name:     "kind in allowed set is not excluded",
			allowed:  map[schema.GroupKind]struct{}{{Group: "", Kind: "Pod"}: {}},
			group:    "",
			kind:     "Pod",
			expected: false,
		},
		{
			name:     "kind not in allowed set is excluded",
			allowed:  map[schema.GroupKind]struct{}{{Group: "", Kind: "Pod"}: {}},
			group:    "apps",
			kind:     "Deployment",
			expected: true,
		},
		{
			name:     "empty allowed set excludes everything",
			allowed:  map[schema.GroupKind]struct{}{},
			group:    "",
			kind:     "Pod",
			expected: true,
		},
		{
			name:     "group must match exactly",
			allowed:  map[schema.GroupKind]struct{}{{Group: "apps", Kind: "Deployment"}: {}},
			group:    "extensions",
			kind:     "Deployment",
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &PermissionResourceFilter{allowed: tc.allowed}
			got := f.IsExcludedResource(tc.group, tc.kind, "")
			if got != tc.expected {
				t.Errorf("IsExcludedResource(%q, %q) = %v, want %v", tc.group, tc.kind, got, tc.expected)
			}
		})
	}
}

func TestNewPermissionResourceFilter_AllowsKindsWithAllThreeVerbs(t *testing.T) {
	clientset := fake.NewSimpleClientset()

	clientset.PrependReactor(
		"create",
		"selfsubjectrulesreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			return true, &authorizationv1.SelfSubjectRulesReview{
				Status: authorizationv1.SubjectRulesReviewStatus{
					ResourceRules: []authorizationv1.ResourceRule{
						{
							Verbs:     []string{"get", "list", "watch"},
							APIGroups: []string{""},
							Resources: []string{"pods", "nodes"},
						},
						{
							// ConfigMap missing "watch" — should be excluded.
							Verbs:     []string{"get", "list"},
							APIGroups: []string{""},
							Resources: []string{"configmaps"},
						},
					},
				},
			}, nil
		},
	)

	discoveryClient := &stubDiscoveryClient{
		FakeDiscovery: clientset.Discovery().(*fakediscovery.FakeDiscovery),
		preferred: []*metav1.APIResourceList{
			{
				GroupVersion: "v1",
				APIResources: []metav1.APIResource{
					{Name: "pods", Kind: "Pod", Namespaced: true, Group: "", Version: "v1"},
					{Name: "configmaps", Kind: "ConfigMap", Namespaced: true, Group: "", Version: "v1"},
					{Name: "nodes", Kind: "Node", Namespaced: false, Group: "", Version: "v1"},
				},
			},
		},
	}

	f, err := NewPermissionResourceFilter(
		t.Context(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		clientset,
		discoveryClient,
		[]string{"default"},
	)
	if err != nil {
		t.Fatalf("NewPermissionResourceFilter returned error: %v", err)
	}

	if f.IsExcludedResource("", "Pod", "") {
		t.Error("Pod should not be excluded (all three verbs allowed)")
	}
	if !f.IsExcludedResource("", "ConfigMap", "") {
		t.Error("ConfigMap should be excluded (watch denied)")
	}
	if f.IsExcludedResource("", "Node", "") {
		t.Error("Node should not be excluded (cluster-scoped, all verbs allowed)")
	}
}

var errFakeDiscovery = errors.New("fake discovery error")

type failingDiscoveryClient struct {
	discovery.DiscoveryInterface
}

func (c *failingDiscoveryClient) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return nil, errFakeDiscovery
}

type stubDiscoveryClient struct {
	*fakediscovery.FakeDiscovery
	preferred []*metav1.APIResourceList
}

func (c *stubDiscoveryClient) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return c.preferred, nil
}

func TestNewPermissionResourceFilter_ReturnsErrorOnDiscoveryFailure(t *testing.T) {
	clientset := fake.NewSimpleClientset()

	_, err := NewPermissionResourceFilter(
		t.Context(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		clientset,
		&failingDiscoveryClient{},
		nil,
	)
	if err == nil {
		t.Fatal("expected error when discovery fails, got nil")
	}
	if !errors.Is(err, errFakeDiscovery) {
		t.Errorf("expected wrapped errFakeDiscovery, got %v", err)
	}
}

type mutableDiscoveryClient struct {
	*fakediscovery.FakeDiscovery
	mu        sync.Mutex
	preferred []*metav1.APIResourceList
	calls     int
}

func (c *mutableDiscoveryClient) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return c.preferred, nil
}

func (c *mutableDiscoveryClient) setPreferred(p []*metav1.APIResourceList) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preferred = p
}

func (c *mutableDiscoveryClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestNewPermissionResourceFilter_RefreshesAllowedSet(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	clientset.PrependReactor(
		"create",
		"selfsubjectrulesreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			return true, &authorizationv1.SelfSubjectRulesReview{
				Status: authorizationv1.SubjectRulesReviewStatus{
					ResourceRules: []authorizationv1.ResourceRule{{
						Verbs:     []string{"*"},
						APIGroups: []string{"*"},
						Resources: []string{"*"},
					}},
				},
			}, nil
		},
	)

	discoveryClient := &mutableDiscoveryClient{
		FakeDiscovery: clientset.Discovery().(*fakediscovery.FakeDiscovery),
		preferred: []*metav1.APIResourceList{
			{
				GroupVersion: "v1",
				APIResources: []metav1.APIResource{
					{Name: "pods", Kind: "Pod", Namespaced: true, Group: "", Version: "v1"},
				},
			},
		},
	}

	f, err := NewPermissionResourceFilter(
		t.Context(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		clientset,
		discoveryClient,
		nil,
	)
	if err != nil {
		t.Fatalf("NewPermissionResourceFilter: %v", err)
	}

	if f.IsExcludedResource("", "Pod", "") {
		t.Fatal("Pod should not be excluded after initial build")
	}
	if !f.IsExcludedResource("", "ConfigMap", "") {
		t.Fatal("ConfigMap should be excluded before refresh")
	}

	go f.startRefreshLoop(t.Context(), 5*time.Millisecond)

	discoveryClient.setPreferred([]*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Kind: "Pod", Namespaced: true, Group: "", Version: "v1"},
				{Name: "configmaps", Kind: "ConfigMap", Namespaced: true, Group: "", Version: "v1"},
			},
		},
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !f.IsExcludedResource("", "ConfigMap", "") {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("ConfigMap should have become not-excluded after refresh, but stayed excluded for 2s")
}

type flakeyDiscoveryClient struct {
	*fakediscovery.FakeDiscovery
	mu        sync.Mutex
	preferred []*metav1.APIResourceList
	fail      bool
}

func (c *flakeyDiscoveryClient) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		return nil, errFakeDiscovery
	}
	return c.preferred, nil
}

func (c *flakeyDiscoveryClient) setFail(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fail = v
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestNewPermissionResourceFilter_RefreshErrorKeepsPriorAllowedSet(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	clientset.PrependReactor(
		"create",
		"selfsubjectrulesreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			return true, &authorizationv1.SelfSubjectRulesReview{
				Status: authorizationv1.SubjectRulesReviewStatus{
					ResourceRules: []authorizationv1.ResourceRule{{
						Verbs:     []string{"*"},
						APIGroups: []string{"*"},
						Resources: []string{"*"},
					}},
				},
			}, nil
		},
	)

	discoveryClient := &flakeyDiscoveryClient{
		FakeDiscovery: clientset.Discovery().(*fakediscovery.FakeDiscovery),
		preferred: []*metav1.APIResourceList{
			{
				GroupVersion: "v1",
				APIResources: []metav1.APIResource{
					{Name: "pods", Kind: "Pod", Namespaced: true, Group: "", Version: "v1"},
				},
			},
		},
	}

	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	f, err := NewPermissionResourceFilter(
		t.Context(),
		logger,
		clientset,
		discoveryClient,
		nil,
	)
	if err != nil {
		t.Fatalf("NewPermissionResourceFilter: %v", err)
	}

	if f.IsExcludedResource("", "Pod", "") {
		t.Fatal("Pod should not be excluded after initial build")
	}

	go f.startRefreshLoop(t.Context(), 5*time.Millisecond)

	discoveryClient.setFail(true)

	time.Sleep(50 * time.Millisecond)

	if f.IsExcludedResource("", "Pod", "") {
		t.Fatal("Pod should still be allowed after refresh failures (prior allowed set must be retained)")
	}

	if !strings.Contains(logBuf.String(), "Failed to refresh PermissionResourceFilter") {
		t.Fatalf("expected refresh failure warning in logs, got: %s", logBuf.String())
	}
}

func TestNewPermissionResourceFilter_RefreshLoopExitsOnContextCancel(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	clientset.PrependReactor(
		"create",
		"selfsubjectrulesreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			return true, &authorizationv1.SelfSubjectRulesReview{
				Status: authorizationv1.SubjectRulesReviewStatus{
					ResourceRules: []authorizationv1.ResourceRule{{
						Verbs:     []string{"*"},
						APIGroups: []string{"*"},
						Resources: []string{"*"},
					}},
				},
			}, nil
		},
	)

	discoveryClient := &mutableDiscoveryClient{
		FakeDiscovery: clientset.Discovery().(*fakediscovery.FakeDiscovery),
		preferred: []*metav1.APIResourceList{
			{GroupVersion: "v1", APIResources: []metav1.APIResource{
				{Name: "pods", Kind: "Pod", Namespaced: true, Group: "", Version: "v1"},
			}},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	f, err := NewPermissionResourceFilter(
		ctx,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		clientset,
		discoveryClient,
		nil,
	)
	if err != nil {
		t.Fatalf("NewPermissionResourceFilter: %v", err)
	}

	go f.startRefreshLoop(ctx, 5*time.Millisecond)

	time.Sleep(30 * time.Millisecond)
	cancel()

	time.Sleep(20 * time.Millisecond)
	before := discoveryClient.callCount()

	time.Sleep(50 * time.Millisecond)
	after := discoveryClient.callCount()

	if after != before {
		t.Fatalf("expected no further discovery calls after ctx cancel, got %d -> %d", before, after)
	}
}
