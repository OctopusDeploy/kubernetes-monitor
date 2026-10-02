package cluster

import (
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func SortDesiredResources(a, b *DesiredResource) bool {
	return string(a.Id) < string(b.Id)
}

func TestUpdateApplicationInstance_SavesNewDesiredResources(t *testing.T) {
	applicationInstances := NewApplicationInstanceList()

	expectedApplicationInstance := NewApplicationInstanceBuilder().Build()
	expected := expectedApplicationInstance.GetDesiredResources()

	applicationInstances.UpsertApplicationInstance(&expectedApplicationInstance)

	actualApplicationInstance, ok := applicationInstances.Get(expectedApplicationInstance.ApplicationInstanceId)
	actual := actualApplicationInstance.GetDesiredResources()

	if diff := cmp.Diff(expected, actual, cmpopts.SortSlices(SortDesiredResources)); !ok || diff != "" {
		t.Error(diff)
	}
}

func TestUpdateApplicationInstance_SavesDesiredResourcesForMultipleApplicationInstances(t *testing.T) {
	applicationInstances := NewApplicationInstanceList()

	existingResource := NewDesiredResourceBuilder().Build()
	existingApplicationInstance := NewApplicationInstanceBuilder().Build()

	applicationInstances.UpsertApplicationInstance(&existingApplicationInstance)

	newResource := NewDesiredResourceBuilder().
		WithName("updated").
		WithVersion("2").
		Build()

	expectedApplicationInstance := NewApplicationInstanceBuilder().
		WithApplicationInstanceId("second-application-instance-id").
		WithDesiredResources([]*DesiredResource{&existingResource, &newResource}).
		Build()

	expected := expectedApplicationInstance.GetDesiredResources()

	applicationInstances.UpsertApplicationInstance(&expectedApplicationInstance)

	actualApplicationInstance, ok := applicationInstances.Get(expectedApplicationInstance.ApplicationInstanceId)
	actual := actualApplicationInstance.GetDesiredResources()

	if diff := cmp.Diff(expected, actual, cmpopts.SortSlices(SortDesiredResources)); !ok || diff != "" {
		t.Error(diff)
	}
}

func TestUpdateApplicationInstance_MergesWithExistingDesiredResources(t *testing.T) {
	applicationInstances := NewApplicationInstanceList()

	existingResource := NewDesiredResourceBuilder().Build()
	existingApplicationInstance := NewApplicationInstanceBuilder().Build()

	applicationInstances.UpsertApplicationInstance(&existingApplicationInstance)

	newResource := NewDesiredResourceBuilder().
		WithName("updated").
		WithVersion("2").
		Build()

	expectedApplicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&existingResource, &newResource}).
		Build()

	expected := expectedApplicationInstance.GetDesiredResources()

	applicationInstances.UpsertApplicationInstance(&expectedApplicationInstance)

	actualApplicationInstance, ok := applicationInstances.Get(expectedApplicationInstance.ApplicationInstanceId)
	actual := actualApplicationInstance.GetDesiredResources()

	if diff := cmp.Diff(expected, actual, cmpopts.SortSlices(SortDesiredResources)); !ok || diff != "" {
		t.Error(diff)
	}
}

func TestUpdateApplicationInstance_ReplacesMatchingResourcesInExistingDesiredResources(t *testing.T) {
	applicationInstances := NewApplicationInstanceList()

	existingResource := NewDesiredResourceBuilder().Build()

	expectedApplicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&existingResource}).
		Build()

	expected := expectedApplicationInstance.GetDesiredResources()

	applicationInstances.UpsertApplicationInstance(&expectedApplicationInstance)

	newApplicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&existingResource, &existingResource}).
		Build()

	applicationInstances.UpsertApplicationInstance(&newApplicationInstance)

	actualApplicationInstance, ok := applicationInstances.Get(newApplicationInstance.ApplicationInstanceId)
	actual := actualApplicationInstance.GetDesiredResources()

	if diff := cmp.Diff(expected, actual, cmpopts.SortSlices(SortDesiredResources)); !ok || diff != "" {
		t.Error(diff)
	}
}

func TestResourceKeysOfInterest_CoversDesiredAndTrackedResourcesOfEveryApplicationInstance(t *testing.T) {
	firstDesired := NewDesiredResourceBuilder().WithId("first-desired").WithName("first-desired").Build()
	firstPresent := NewPresentMonitoredResourceBuilder().
		WithDesiredResourceId(firstDesired.Id).
		WithName("first-present").
		Build()
	firstChild := NewChildMonitoredResourceBuilder().
		WithPresentResourceOwner(firstPresent).
		WithName("first-child").
		WithKind("ReplicaSet").
		Build()
	first := NewApplicationInstanceBuilder().
		WithApplicationInstanceId("first").
		WithDesiredResources([]*DesiredResource{&firstDesired}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&firstPresent}).
		WithChildMonitoredResources([]*ChildMonitoredResource{&firstChild}).
		Build()

	secondDesired := NewDesiredResourceBuilder().WithId("second-desired").WithName("second-desired").Build()
	secondChild := NewChildMonitoredResourceBuilder().
		WithChildResourceOwner(firstChild).
		WithName("second-child").
		WithKind("Pod").
		Build()
	secondUnknown := NewUnknownMonitoredResourceBuilder().WithDesiredResourceId("not-desired").Build()
	second := NewApplicationInstanceBuilder().
		WithApplicationInstanceId("second").
		WithDesiredResources([]*DesiredResource{&secondDesired}).
		WithChildMonitoredResources([]*ChildMonitoredResource{&secondChild}).
		WithUnknownMonitoredResources([]*UnknownMonitoredResource{&secondUnknown}).
		Build()

	applicationInstances := NewApplicationInstanceList()
	applicationInstances.UpsertApplicationInstance(&first)
	applicationInstances.UpsertApplicationInstance(&second)

	expected := map[kube.ResourceKey]struct{}{
		firstDesired.ResourceKey():  {},
		firstPresent.ResourceKey():  {},
		firstChild.ResourceKey():    {},
		secondDesired.ResourceKey(): {},
		secondChild.ResourceKey():   {},
	}
	if diff := cmp.Diff(expected, applicationInstances.resourceKeysOfInterest()); diff != "" {
		t.Error(diff)
	}
}
