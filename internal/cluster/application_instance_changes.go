package cluster

import "github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"

type ApplicationInstanceChanges struct {
	ApplicationInstanceId
	ClusterId
	PresentMonitoredResources []*PresentMonitoredResource
	ChildMonitoredResources   []*ChildMonitoredResource
	MissingMonitoredResources []*MissingMonitoredResource
	UnknownMonitoredResources []*UnknownMonitoredResource
	DeletedChildResourceKeys  []kube.ResourceKey
}

func (update *ApplicationInstanceChanges) GetAllResourceCount() int {
	return len(update.PresentMonitoredResources) +
		len(update.ChildMonitoredResources) +
		len(update.MissingMonitoredResources) +
		len(update.UnknownMonitoredResources)
}
