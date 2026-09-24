package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"

	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// defaultProbeNamespace is the SSRR probe namespace when no target namespaces are
// configured. Any non-empty namespace works: ClusterRole-granted rules surface in
// every namespace's SSRR result.
const defaultProbeNamespace = "default"

type PermissionResourceFilter struct {
	mu      sync.RWMutex
	allowed map[schema.GroupKind]struct{}

	logger           *slog.Logger
	clientset        kubernetes.Interface
	discoveryClient  discovery.DiscoveryInterface
	targetNamespaces []string
}

func (f *PermissionResourceFilter) IsExcludedResource(group, kind, cluster string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, ok := f.allowed[schema.GroupKind{Group: group, Kind: kind}]
	return !ok
}

func (f *PermissionResourceFilter) GetLabelSelector(group, kind, cluster string) string {
	return ""
}

// buildAllowedResourcesMap discovers API resources and runs a single
// SelfSubjectRulesReview against the probe namespace, returning the set of
// groupKinds the caller has full get/list/watch access to. It does NOT mutate
// the filter — callers must swap the result into f.allowed themselves.
//
// Assumes RBAC is uniform across target namespaces
func (f *PermissionResourceFilter) buildAllowedResourcesMap(
	ctx context.Context,
) (map[schema.GroupKind]struct{}, error) {
	apiResourceLists, err := f.discoveryClient.ServerPreferredResources()
	if err != nil {
		return nil, fmt.Errorf("discovering API resources: %w", err)
	}

	rules, err := fetchResourceRules(ctx, f.clientset, f.probeNamespace())
	if err != nil {
		return nil, fmt.Errorf("fetching rules: %w", err)
	}

	allowed := make(map[schema.GroupKind]struct{})
	for _, list := range apiResourceLists {
		group, _ := parseGroupVersion(list.GroupVersion)
		for _, res := range list.APIResources {
			if rulesPermitAllVerbs(rules, group, res.Name, requiredVerbs) {
				allowed[schema.GroupKind{Group: group, Kind: res.Kind}] = struct{}{}
			}
		}
	}

	return allowed, nil
}

var requiredVerbs = []string{"get", "list", "watch"}

// NewPermissionResourceFilter builds a filter against the current RBAC state.
// Use startRefreshLoop to refresh the allowed set in the background. If the
// initial build fails the error is returned.
func NewPermissionResourceFilter(
	ctx context.Context,
	logger *slog.Logger,
	clientset kubernetes.Interface,
	discoveryClient discovery.DiscoveryInterface,
	targetNamespaces []string,
) (*PermissionResourceFilter, error) {
	f := &PermissionResourceFilter{
		logger:           logger,
		clientset:        clientset,
		discoveryClient:  discoveryClient,
		targetNamespaces: targetNamespaces,
	}

	allowed, err := f.buildAllowedResourcesMap(ctx)
	if err != nil {
		return nil, err
	}
	f.allowed = allowed

	return f, nil
}

// startRefreshLoop rebuilds the allowed set every interval and exits when ctx
// is cancelled. Rebuild failures are logged and the prior allowed set is retained.
func (f *PermissionResourceFilter) startRefreshLoop(
	ctx context.Context, interval time.Duration,
) {
	f.logger.Info(
		"Starting PermissionResourceFilter refresh loop",
		slog.Duration("interval", interval),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			allowed, err := f.buildAllowedResourcesMap(ctx)
			if err != nil {
				f.logger.Warn(
					"Failed to refresh PermissionResourceFilter allowed set; keeping prior set",
					slog.Any("error", err),
				)
				continue
			}
			f.mu.Lock()
			f.allowed = allowed
			f.mu.Unlock()
		}
	}
}

func parseGroupVersion(gv string) (group, version string) {
	if i := strings.Index(gv, "/"); i >= 0 {
		return gv[:i], gv[i+1:]
	}
	return "", gv
}

func (f *PermissionResourceFilter) probeNamespace() string {
	if len(f.targetNamespaces) > 0 {
		return f.targetNamespaces[0]
	}

	if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		return strings.TrimSpace(string(data))
	}

	return defaultProbeNamespace
}

func fetchResourceRules(
	ctx context.Context, clientset kubernetes.Interface, namespace string,
) ([]authorizationv1.ResourceRule, error) {
	ssrr := &authorizationv1.SelfSubjectRulesReview{
		Spec: authorizationv1.SelfSubjectRulesReviewSpec{Namespace: namespace},
	}
	result, err := clientset.AuthorizationV1().
		SelfSubjectRulesReviews().
		Create(ctx, ssrr, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	return result.Status.ResourceRules, nil
}

func rulesPermitAllVerbs(
	rules []authorizationv1.ResourceRule, group, resource string, verbs []string,
) bool {
	for _, verb := range verbs {
		if !rulesPermitVerb(rules, group, resource, verb) {
			return false
		}
	}
	return true
}

func rulesPermitVerb(rules []authorizationv1.ResourceRule, group, resource, verb string) bool {
	for _, rule := range rules {
		// Name-restricted rules don't grant the unrestricted list/watch we need.
		if len(rule.ResourceNames) > 0 {
			continue
		}
		if !slices.Contains(rule.Verbs, verb) && !slices.Contains(rule.Verbs, "*") {
			continue
		}
		if !slices.Contains(rule.APIGroups, group) && !slices.Contains(rule.APIGroups, "*") {
			continue
		}
		if !slices.Contains(rule.Resources, resource) && !slices.Contains(rule.Resources, "*") {
			continue
		}
		return true
	}
	return false
}
