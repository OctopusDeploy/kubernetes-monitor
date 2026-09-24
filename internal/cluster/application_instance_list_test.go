package cluster

import (
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func SortDesiredResources(a, b *DesiredResource) bool {
	return string(a.Id) < string(b.Id)
}

func GenerateResourceKeys(count int) []kube.ResourceKey {
	resourceKeys := make([]kube.ResourceKey, count)
	for i := 0; i < count; i++ {
		unique := rand.Int()
		resourceKeys[i] = kube.ResourceKey{
			Group:     fmt.Sprintf("group-%v", unique),
			Kind:      fmt.Sprintf("kind-%v", unique),
			Namespace: fmt.Sprintf("namespace-%v", unique),
			Name:      fmt.Sprintf("name-%v", unique),
		}
	}
	return resourceKeys
}

func GetRandomSubset(count int, max int, source []kube.ResourceKey) []kube.ResourceKey {
	resourceKeys := make([]kube.ResourceKey, count)
	for i := 0; i < count; i++ {
		resourceKeys[i] = source[rand.Intn(max)]
	}
	return resourceKeys
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

func TestApplicationInstanceList_DataIsConsistent(t *testing.T) {
	totalNumber := 1000
	applicationInstances := NewApplicationInstanceList()
	resourceKeys := GenerateResourceKeys(totalNumber)
	keysToDelete := GetRandomSubset(totalNumber/10, totalNumber, resourceKeys)

	for _, key := range resourceKeys {
		applicationInstances.AddTrackedResourceKey(key)
	}
	for _, key := range keysToDelete {
		applicationInstances.RemoveTrackedResourceKey(key)
	}
	for _, key := range resourceKeys {
		if slices.Contains(keysToDelete, key) && applicationInstances.ResourceIsTracked(key) {
			t.Error("Resource should not be tracked", key)
		}
	}
}

func TestApplicationInstanceList_ConcurrentAccessDoesNotThrow(t *testing.T) {
	totalNumber := 10000
	applicationInstances := NewApplicationInstanceList()
	resourceKeys := GenerateResourceKeys(totalNumber)

	go func() {
		for {
			applicationInstances.ResourceIsTracked(resourceKeys[0])
		}
	}()
	for _, key := range resourceKeys {
		go applicationInstances.AddTrackedResourceKey(key)
		go applicationInstances.ResourceIsTracked(key)
	}

	// Wait for all keys to be added
	for len(applicationInstances.trackedResourceKeys.GetAll()) < totalNumber {
	}

	if len(applicationInstances.trackedResourceKeys.GetAll()) != totalNumber {
		t.Errorf("Expected %d keys, but got %d", totalNumber, len(applicationInstances.trackedResourceKeys.GetAll()))
	}
}

func BenchmarkApplicationInstanceList_ResourceIsTracked(b *testing.B) {
	totalNumber := 1000
	for i := 0; i < b.N; i++ {
		wg := sync.WaitGroup{}
		applicationInstances := NewApplicationInstanceList()
		resourceKeys := GenerateResourceKeys(totalNumber)
		keysToDelete := GetRandomSubset(totalNumber/10, totalNumber, resourceKeys)
		keysToCheck := append(
			GenerateResourceKeys(totalNumber/10),
			GetRandomSubset(totalNumber/10, totalNumber, resourceKeys)...)
		keysToCheck = append(keysToCheck, keysToDelete...)

		wg.Add(1)
		go func() {
			for _, key := range resourceKeys {
				applicationInstances.AddTrackedResourceKey(key)
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			for _, key := range keysToDelete {
				applicationInstances.RemoveTrackedResourceKey(key)
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			for _, key := range keysToCheck {
				applicationInstances.ResourceIsTracked(key)
			}
			wg.Done()
		}()
		wg.Wait()
	}
}
