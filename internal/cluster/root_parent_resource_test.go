package cluster

import (
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"
)

func childResources(links ...[2]types.UID) map[kube.ResourceKey]*ChildMonitoredResource {
	resources := map[kube.ResourceKey]*ChildMonitoredResource{}
	for _, link := range links {
		resourceId, ownerId := link[0], link[1]
		resources[kube.ResourceKey{Name: string(resourceId)}] = &ChildMonitoredResource{
			ResourceId: resourceId,
			OwnerId:    ownerId,
		}
	}
	return resources
}

func TestGetRootParentResource(t *testing.T) {
	tests := []struct {
		name     string
		ownerId  types.UID
		children map[kube.ResourceKey]*ChildMonitoredResource
		expected types.UID
	}{
		{
			name:     "owner is not a child resource",
			ownerId:  "deployment",
			children: childResources(),
			expected: "deployment",
		},
		{
			name:    "walks the chain to the top level owner",
			ownerId: "pod",
			children: childResources(
				[2]types.UID{"pod", "replicaset"},
				[2]types.UID{"replicaset", "deployment"},
			),
			expected: "deployment",
		},
		{
			name:     "stops when a resource owns itself",
			ownerId:  "pod",
			children: childResources([2]types.UID{"pod", "pod"}),
			expected: "pod",
		},
		{
			name:    "stops when a resource lower in the chain owns itself",
			ownerId: "pod",
			children: childResources(
				[2]types.UID{"pod", "replicaset"},
				[2]types.UID{"replicaset", "replicaset"},
			),
			expected: "replicaset",
		},
		{
			name:    "stops when two resources own each other",
			ownerId: "a",
			children: childResources(
				[2]types.UID{"a", "b"},
				[2]types.UID{"b", "a"},
			),
			expected: "b",
		},
		{
			name:    "stops on a longer loop",
			ownerId: "a",
			children: childResources(
				[2]types.UID{"a", "b"},
				[2]types.UID{"b", "c"},
				[2]types.UID{"c", "a"},
			),
			expected: "c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, getRootParentResource(tt.ownerId, tt.children))
		})
	}
}
