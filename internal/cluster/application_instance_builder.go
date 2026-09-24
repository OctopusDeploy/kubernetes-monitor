package cluster

import (
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"

	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
	"github.com/octopusdeploy/kubernetes-monitor/internal/utilities"
)

type ApplicationInstanceBuilder struct {
	applicationInstanceId     string
	clusterId                 string
	hashSalt                  crypto.HashSalt
	desiredResources          []*DesiredResource
	presentMonitoredResources []*PresentMonitoredResource
	childMonitoredResources   []*ChildMonitoredResource
	missingMonitoredResources []*MissingMonitoredResource
	unknownMonitoredResources []*UnknownMonitoredResource
}

func NewApplicationInstanceBuilder() *ApplicationInstanceBuilder {
	return &ApplicationInstanceBuilder{
		applicationInstanceId: "default-application-instance-id",
		clusterId:             "default-cluster-id",
		hashSalt:              "fake-salt",
		desiredResources:      nil,
	}
}

func (b *ApplicationInstanceBuilder) WithApplicationInstanceId(value string) *ApplicationInstanceBuilder {
	b.applicationInstanceId = value
	return b
}

func (b *ApplicationInstanceBuilder) WithHashSalt(value crypto.HashSalt) *ApplicationInstanceBuilder {
	b.hashSalt = value
	return b
}

func (b *ApplicationInstanceBuilder) WithClusterId(value string) *ApplicationInstanceBuilder {
	b.clusterId = value
	return b
}

func (b *ApplicationInstanceBuilder) WithDesiredResources(value []*DesiredResource) *ApplicationInstanceBuilder {
	b.desiredResources = value
	return b
}

func (b *ApplicationInstanceBuilder) WithPresentMonitoredResources(
	value []*PresentMonitoredResource,
) *ApplicationInstanceBuilder {
	b.presentMonitoredResources = value
	return b
}

func (b *ApplicationInstanceBuilder) WithChildMonitoredResources(
	value []*ChildMonitoredResource,
) *ApplicationInstanceBuilder {
	b.childMonitoredResources = value
	return b
}

func (b *ApplicationInstanceBuilder) WithMissingMonitoredResources(
	value []*MissingMonitoredResource,
) *ApplicationInstanceBuilder {
	b.missingMonitoredResources = value
	return b
}

func (b *ApplicationInstanceBuilder) WithUnknownMonitoredResources(
	value []*UnknownMonitoredResource,
) *ApplicationInstanceBuilder {
	b.unknownMonitoredResources = value
	return b
}

func (b *ApplicationInstanceBuilder) Build() ApplicationInstance {
	if b.desiredResources == nil {
		desiredResource := NewDesiredResourceBuilder().Build()
		b.WithDesiredResources([]*DesiredResource{&desiredResource})
	}

	trackedResourceKeys := map[kube.ResourceKey]bool{}

	desiredResources := map[kube.ResourceKey]*DesiredResource{}
	for _, val := range b.desiredResources {
		desiredResources[val.ResourceKey()] = val
	}

	presentMonitoredResources := map[DesiredResourceId]*PresentMonitoredResource{}
	for _, val := range b.presentMonitoredResources {
		presentMonitoredResources[val.DesiredResourceId] = val
		trackedResourceKeys[val.ResourceKey()] = true
	}

	childMonitoredResources := map[kube.ResourceKey]*ChildMonitoredResource{}
	for _, val := range b.childMonitoredResources {
		childMonitoredResources[val.ResourceKey()] = val
		trackedResourceKeys[val.ResourceKey()] = true
	}

	missingMonitoredResources := map[DesiredResourceId]*MissingMonitoredResource{}
	for _, val := range b.missingMonitoredResources {
		missingMonitoredResources[val.DesiredResourceId] = val
	}

	unknownMonitoredResources := map[DesiredResourceId]*UnknownMonitoredResource{}
	for _, val := range b.unknownMonitoredResources {
		unknownMonitoredResources[val.DesiredResourceId] = val
	}

	return ApplicationInstance{
		ApplicationInstanceId:     ApplicationInstanceId(b.applicationInstanceId),
		hashSalt:                  b.hashSalt,
		desiredResources:          utilities.NewConcurrentMapFromMap(desiredResources),
		presentMonitoredResources: utilities.NewConcurrentMapFromMap(presentMonitoredResources),
		childMonitoredResources:   utilities.NewConcurrentMapFromMap(childMonitoredResources),
		missingMonitoredResources: utilities.NewConcurrentMapFromMap(missingMonitoredResources),
		unknownMonitoredResources: utilities.NewConcurrentMapFromMap(unknownMonitoredResources),
		trackedResourceKeys:       utilities.NewConcurrentMapFromMap(trackedResourceKeys),
	}
}
