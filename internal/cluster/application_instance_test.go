package cluster

import (
	"context"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/cache"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// cachedResolver returns res.Resource as-is — used when a test's fixtures
// already carry their manifests inline.
type cachedResolver struct{}

func (cachedResolver) ResolveManifest(_ context.Context, res *cache.Resource) (*unstructured.Unstructured, error) {
	if res == nil {
		return nil, nil
	}
	return res.Resource, nil
}

func TestGetChangesForUpdatedResource_CreatesNilChangeSetForNoChanges(t *testing.T) {
	applicationInstance := NewApplicationInstanceBuilder().Build()

	changes, err := applicationInstance.GetChangesForUpdatedResource(
		context.TODO(),
		ClusterId("cluster-id"),
		cachedResolver{},
		nil,
		nil,
	)

	if diff := cmp.Diff(nil, err); diff != "" {
		t.Error(diff)
	}

	if diff := cmp.Diff((*ApplicationInstanceChanges)(nil), changes); diff != "" {
		t.Error(diff)
	}
}

func TestGetChangesForUpdatedResource_UpdatesPresentResource(t *testing.T) {
	desiredResource := NewDesiredResourceBuilder().Build()
	presentMonitoredResource := NewPresentMonitoredResourceBuilder().ForDesiredResource(desiredResource).Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&presentMonitoredResource}).
		Build()

	oldRes := presentMonitoredResource.ToResource(false)
	newRes := presentMonitoredResource.ToResource(true)

	changes, err := applicationInstance.GetChangesForUpdatedResource(
		context.TODO(),
		ClusterId("cluster-id"),
		cachedResolver{},
		newRes,
		oldRes,
	)

	if diff := cmp.Diff(nil, err); diff != "" {
		t.Error(diff)
	}

	presentMonitoredResource.ResourceVersion = newRes.ResourceVersion

	expectedChanges := &ApplicationInstanceChanges{
		ApplicationInstanceId:     applicationInstance.ApplicationInstanceId,
		ClusterId:                 presentMonitoredResource.ClusterId,
		PresentMonitoredResources: []*PresentMonitoredResource{&presentMonitoredResource},
	}

	if diff := cmp.Diff(expectedChanges, changes); diff != "" {
		t.Error(diff)
	}

	expectedApplicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&presentMonitoredResource}).
		Build()

	assertApplicationInstancesEqual(t, &expectedApplicationInstance, &applicationInstance)
}

func TestGetChangesForUpdatedResource_ConvertsPresentResourceToMissingResource(t *testing.T) {
	desiredResource := NewDesiredResourceBuilder().Build()
	presentMonitoredResource := NewPresentMonitoredResourceBuilder().ForDesiredResource(desiredResource).Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&presentMonitoredResource}).
		Build()

	oldRes := presentMonitoredResource.ToResource(false)

	changes, err := applicationInstance.GetChangesForUpdatedResource(
		context.TODO(),
		ClusterId("cluster-id"),
		cachedResolver{},
		nil,
		oldRes,
	)

	if diff := cmp.Diff(nil, err); diff != "" {
		t.Error(diff)
	}

	expectedMissingMonitoredResource := newMissingMonitoredResourceFromDesiredResource(desiredResource)

	expectedChanges := &ApplicationInstanceChanges{
		ApplicationInstanceId:     applicationInstance.ApplicationInstanceId,
		ClusterId:                 presentMonitoredResource.ClusterId,
		MissingMonitoredResources: []*MissingMonitoredResource{&expectedMissingMonitoredResource},
	}

	if diff := cmp.Diff(expectedChanges, changes); diff != "" {
		t.Error(diff)
	}

	expectedApplicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithMissingMonitoredResources([]*MissingMonitoredResource{&expectedMissingMonitoredResource}).
		Build()

	assertApplicationInstancesEqual(t, &expectedApplicationInstance, &applicationInstance)
}

func TestGetChangesForUpdatedResource_ConvertsMissingResourceToPresentResource(t *testing.T) {
	desiredResource := NewDesiredResourceBuilder().Build()
	missingMonitoredResource := newMissingMonitoredResourceFromDesiredResource(desiredResource)

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithMissingMonitoredResources([]*MissingMonitoredResource{&missingMonitoredResource}).
		Build()

	expectedPresentMonitoredResource := NewPresentMonitoredResourceBuilder().ForDesiredResource(desiredResource).Build()

	newRes := expectedPresentMonitoredResource.ToResource(false)

	changes, err := applicationInstance.GetChangesForUpdatedResource(
		context.TODO(),
		ClusterId("cluster-id"),
		cachedResolver{},
		newRes,
		nil,
	)

	if diff := cmp.Diff(nil, err); diff != "" {
		t.Error(diff)
	}

	expectedChanges := &ApplicationInstanceChanges{
		ApplicationInstanceId:     applicationInstance.ApplicationInstanceId,
		ClusterId:                 missingMonitoredResource.ClusterId,
		PresentMonitoredResources: []*PresentMonitoredResource{&expectedPresentMonitoredResource},
	}

	if diff := cmp.Diff(expectedChanges, changes); diff != "" {
		t.Error(diff)
	}

	expectedApplicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&expectedPresentMonitoredResource}).
		Build()

	assertApplicationInstancesEqual(t, &expectedApplicationInstance, &applicationInstance)
}

func TestGetChangesForUpdatedResource_IgnoresUnknownResources(t *testing.T) {
	desiredResource := NewDesiredResourceBuilder().Build()
	unknownMonitoredResource := newUnknownMonitoredResourceFromDesiredResource(desiredResource)

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithUnknownMonitoredResources([]*UnknownMonitoredResource{&unknownMonitoredResource}).
		Build()

	expectedPresentMonitoredResource := NewPresentMonitoredResourceBuilder().ForDesiredResource(desiredResource).Build()

	newRes := expectedPresentMonitoredResource.ToResource(false)

	changes, err := applicationInstance.GetChangesForUpdatedResource(
		context.TODO(),
		ClusterId("cluster-id"),
		cachedResolver{},
		newRes,
		nil,
	)

	if diff := cmp.Diff(nil, err); diff != "" {
		t.Error(diff)
	}

	if diff := cmp.Diff((*ApplicationInstanceChanges)(nil), changes); diff != "" {
		t.Error(diff)
	}

	expectedApplicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithUnknownMonitoredResources([]*UnknownMonitoredResource{&unknownMonitoredResource}).
		Build()

	assertApplicationInstancesEqual(t, &expectedApplicationInstance, &applicationInstance)
}

func TestGetChangesForUpdatedResource_AddsChildOfParentResource(t *testing.T) {
	desiredResource := NewDesiredResourceBuilder().Build()
	parentResource := NewPresentMonitoredResourceBuilder().ForDesiredResource(desiredResource).Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&parentResource}).
		Build()

	expectedChildMonitoredResource := NewChildMonitoredResourceBuilder().
		WithPresentResourceOwner(parentResource).
		Build()

	newRes := expectedChildMonitoredResource.ToResource(false)

	changes, err := applicationInstance.GetChangesForUpdatedResource(
		context.TODO(),
		ClusterId("cluster-id"),
		cachedResolver{},
		newRes,
		nil,
	)

	if diff := cmp.Diff(nil, err); diff != "" {
		t.Error(diff)
	}

	expectedChanges := &ApplicationInstanceChanges{
		ApplicationInstanceId:   applicationInstance.ApplicationInstanceId,
		ClusterId:               expectedChildMonitoredResource.ClusterId,
		ChildMonitoredResources: []*ChildMonitoredResource{&expectedChildMonitoredResource},
	}

	if diff := cmp.Diff(expectedChanges, changes); diff != "" {
		t.Error(diff)
	}

	expectedApplicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&parentResource}).
		WithChildMonitoredResources([]*ChildMonitoredResource{&expectedChildMonitoredResource}).
		Build()

	assertApplicationInstancesEqual(t, &expectedApplicationInstance, &applicationInstance)
}

func TestGetChangesForUpdatedResource_AddsChildOfChildResource(t *testing.T) {
	desiredResource := NewDesiredResourceBuilder().Build()
	parentResource := NewPresentMonitoredResourceBuilder().ForDesiredResource(desiredResource).Build()

	childMonitoredResource := NewChildMonitoredResourceBuilder().
		WithPresentResourceOwner(parentResource).
		Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&parentResource}).
		WithChildMonitoredResources([]*ChildMonitoredResource{&childMonitoredResource}).
		Build()

	expectedChildMonitoredResource := NewChildMonitoredResourceBuilder().
		WithChildResourceOwner(childMonitoredResource).
		Build()

	newRes := expectedChildMonitoredResource.ToResource(false)

	changes, err := applicationInstance.GetChangesForUpdatedResource(
		context.TODO(),
		ClusterId("cluster-id"),
		cachedResolver{},
		newRes,
		nil,
	)

	if diff := cmp.Diff(nil, err); diff != "" {
		t.Error(diff)
	}

	expectedChanges := &ApplicationInstanceChanges{
		ApplicationInstanceId:   applicationInstance.ApplicationInstanceId,
		ClusterId:               expectedChildMonitoredResource.ClusterId,
		ChildMonitoredResources: []*ChildMonitoredResource{&expectedChildMonitoredResource},
	}

	if diff := cmp.Diff(expectedChanges, changes); diff != "" {
		t.Error(diff)
	}

	expectedApplicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&parentResource}).
		WithChildMonitoredResources([]*ChildMonitoredResource{
			&childMonitoredResource, &expectedChildMonitoredResource,
		}).
		Build()

	assertApplicationInstancesEqual(t, &expectedApplicationInstance, &applicationInstance)
}

func TestGetChangesForUpdatedResource_UpdatesChildResource(t *testing.T) {
	desiredResource := NewDesiredResourceBuilder().Build()
	parentResource := NewPresentMonitoredResourceBuilder().ForDesiredResource(desiredResource).Build()

	childMonitoredResource := NewChildMonitoredResourceBuilder().
		WithPresentResourceOwner(parentResource).
		Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&parentResource}).
		WithChildMonitoredResources([]*ChildMonitoredResource{&childMonitoredResource}).
		Build()

	oldRes := childMonitoredResource.ToResource(false)
	newRes := childMonitoredResource.ToResource(true)

	changes, err := applicationInstance.GetChangesForUpdatedResource(
		context.TODO(),
		ClusterId("cluster-id"),
		cachedResolver{},
		newRes,
		oldRes,
	)

	if diff := cmp.Diff(nil, err); diff != "" {
		t.Error(diff)
	}

	childMonitoredResource.ResourceVersion = newRes.ResourceVersion

	expectedChanges := &ApplicationInstanceChanges{
		ApplicationInstanceId:   applicationInstance.ApplicationInstanceId,
		ClusterId:               childMonitoredResource.ClusterId,
		ChildMonitoredResources: []*ChildMonitoredResource{&childMonitoredResource},
	}

	if diff := cmp.Diff(expectedChanges, changes); diff != "" {
		t.Error(diff)
	}

	expectedApplicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&parentResource}).
		WithChildMonitoredResources([]*ChildMonitoredResource{&childMonitoredResource}).
		Build()

	assertApplicationInstancesEqual(t, &expectedApplicationInstance, &applicationInstance)
}

func TestGetChangesForUpdatedResource_RemovesChildResource(t *testing.T) {
	desiredResource := NewDesiredResourceBuilder().Build()
	parentResource := NewPresentMonitoredResourceBuilder().ForDesiredResource(desiredResource).Build()

	childMonitoredResource := NewChildMonitoredResourceBuilder().
		WithPresentResourceOwner(parentResource).
		Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&parentResource}).
		WithChildMonitoredResources([]*ChildMonitoredResource{&childMonitoredResource}).
		Build()

	oldRes := childMonitoredResource.ToResource(false)

	changes, err := applicationInstance.GetChangesForUpdatedResource(
		context.TODO(),
		ClusterId("cluster-id"),
		cachedResolver{},
		nil,
		oldRes,
	)

	if diff := cmp.Diff(nil, err); diff != "" {
		t.Error(diff)
	}

	expectedChanges := &ApplicationInstanceChanges{
		ApplicationInstanceId:    applicationInstance.ApplicationInstanceId,
		ClusterId:                childMonitoredResource.ClusterId,
		DeletedChildResourceKeys: []kube.ResourceKey{childMonitoredResource.ResourceKey()},
	}

	if diff := cmp.Diff(expectedChanges, changes); diff != "" {
		t.Error(diff)
	}

	expectedApplicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&parentResource}).
		Build()

	assertApplicationInstancesEqual(t, &expectedApplicationInstance, &applicationInstance)
}

func TestGetParentResourceId_ReturnsOwnerRefId(t *testing.T) {
	applicationInstance := NewApplicationInstanceBuilder().Build()
	expected := types.UID("test-deployment-uid")

	actual := applicationInstance.GetParentResourceId(ResourceInfo{
		ResourceKey: kube.ResourceKey{},
		OwnerRefs: []v1.OwnerReference{
			{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "test-deployment",
				UID:        expected,
			},
		},
	})

	if actual != expected {
		t.Errorf("Expected %s, but got %s", expected, actual)
	}
}

func TestGetParentResourceId_ReturnsPresentResourceId(t *testing.T) {
	expected := types.UID("test-deployment-uid")

	desiredResource := NewDesiredResourceBuilder().Build()
	presentResource := NewPresentMonitoredResourceBuilder().
		WithDesiredResourceId(desiredResource.Id).
		WithUID(expected).
		Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&presentResource}).
		Build()

	actual := applicationInstance.GetParentResourceId(ResourceInfo{
		ResourceKey: kube.ResourceKey{},
		OwnerRefs: []v1.OwnerReference{
			{
				APIVersion: desiredResource.Details.ManifestType.APIVersion,
				Kind:       desiredResource.Details.ManifestType.Kind,
				Name:       desiredResource.Details.Name,
				// No UID here
			},
		},
	})

	if actual != expected {
		t.Errorf("Expected %s, but got %s", expected, actual)
	}
}

func TestGetParentResourceId_ReturnsChildResourceId(t *testing.T) {
	expected := types.UID("test-deployment-uid")

	desiredResource := NewDesiredResourceBuilder().Build()
	presentResource := NewPresentMonitoredResourceBuilder().
		WithDesiredResourceId(desiredResource.Id).
		Build()

	// This is the parent
	childResource := NewChildMonitoredResourceBuilder().
		WithPresentResourceOwner(presentResource).
		WithName(presentResource.Name).
		WithNamespace(presentResource.Namespace).
		WithGroup(presentResource.GroupVersionKind.Group).
		WithKind(presentResource.GroupVersionKind.Kind).
		WithVersion(presentResource.GroupVersionKind.Version).
		WithUID(expected).
		Build()

	grandchildResource := NewChildMonitoredResourceBuilder().
		WithNamespace(childResource.Namespace). // Relations must share the same namespace
		WithChildResourceOwner(childResource).
		Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&desiredResource}).
		WithPresentMonitoredResources([]*PresentMonitoredResource{&presentResource}).
		WithChildMonitoredResources([]*ChildMonitoredResource{&childResource}).
		Build()

	actual := applicationInstance.GetParentResourceId(ResourceInfo{
		ResourceKey: kube.ResourceKey{
			Namespace: grandchildResource.Namespace,
		},
		OwnerRefs: []v1.OwnerReference{
			{
				APIVersion: childResource.GroupVersionKind.Group + "/" + childResource.GroupVersionKind.Version,
				Kind:       childResource.ResourceKey().Kind,
				Name:       childResource.Name,
				// No UID here
			},
		},
	})

	if actual != expected {
		t.Errorf("Expected %s, but got %s", expected, actual)
	}
}

func TestDeleteDesiredResources_OnlyRemovesListedResources(t *testing.T) {
	r1 := NewDesiredResourceBuilder().WithName("resource-1").Build()
	r2 := NewDesiredResourceBuilder().WithName("resource-2").Build()
	r3 := NewDesiredResourceBuilder().WithName("resource-3").Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&r1, &r2, &r3}).
		Build()

	applicationInstance.DeleteDesiredResources([]DesiredResourceId{r1.Id, r3.Id})

	expected := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&r2}).
		Build()

	if diff := cmp.Diff(
		expected.desiredResources.GetAsMap(),
		applicationInstance.desiredResources.GetAsMap(),
	); diff != "" {
		t.Error(diff)
	}
}

func TestDeleteDesiredResourcesExceptForVersion_KeepsResourcesWithNoVersion(t *testing.T) {
	unversioned := NewDesiredResourceBuilder().
		WithName("from-a-complete-list").
		WithoutVersion().
		Build()
	oldVersion := NewDesiredResourceBuilder().WithName("old").WithVersion("old-version").Build()
	keptVersion := NewDesiredResourceBuilder().WithName("kept").WithVersion("kept-version").Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&unversioned, &oldVersion, &keptVersion}).
		Build()

	applicationInstance.DeleteDesiredResourcesExceptForVersion("kept-version")

	expected := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&unversioned, &keptVersion}).
		Build()

	if diff := cmp.Diff(
		expected.desiredResources.GetAsMap(),
		applicationInstance.desiredResources.GetAsMap(),
	); diff != "" {
		t.Error(diff)
	}
}

func TestDeleteDesiredResources_UnknownIdIsIgnored(t *testing.T) {
	r1 := NewDesiredResourceBuilder().WithName("resource-1").Build()
	r2 := NewDesiredResourceBuilder().WithName("resource-2").Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&r1}).
		WithDesiredResources([]*DesiredResource{&r2}).
		Build()

	applicationInstance.DeleteDesiredResources([]DesiredResourceId{"does-not-exist"})

	expected := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&r1}).
		WithDesiredResources([]*DesiredResource{&r2}).
		Build()

	if diff := cmp.Diff(
		expected.desiredResources.GetAsMap(),
		applicationInstance.desiredResources.GetAsMap(),
	); diff != "" {
		t.Error(diff)
	}
}

func TestDeleteDesiredResources_EmptyListRemovesNothing(t *testing.T) {
	r1 := NewDesiredResourceBuilder().WithName("resource-1").Build()
	r2 := NewDesiredResourceBuilder().WithName("resource-2").Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&r1, &r2}).
		Build()

	applicationInstance.DeleteDesiredResources([]DesiredResourceId{})

	expected := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&r1, &r2}).
		Build()

	if diff := cmp.Diff(
		expected.desiredResources.GetAsMap(),
		applicationInstance.desiredResources.GetAsMap(),
	); diff != "" {
		t.Error(diff)
	}
}

func newMissingMonitoredResourceFromDesiredResource(desiredResource DesiredResource) MissingMonitoredResource {
	return NewMissingMonitoredResourceBuilder().
		WithDesiredResourceId(desiredResource.Id).
		WithName(desiredResource.Details.Name).
		WithNamespace(desiredResource.Details.Namespace).
		WithGroup(desiredResource.Details.ManifestType.GroupVersionKind().Group).
		WithKind(desiredResource.Details.ManifestType.Kind).
		WithVersion(desiredResource.Details.ManifestType.GroupVersionKind().Version).
		Build()
}

func newUnknownMonitoredResourceFromDesiredResource(desiredResource DesiredResource) UnknownMonitoredResource {
	return NewUnknownMonitoredResourceBuilder().
		WithDesiredResourceId(desiredResource.Id).
		Build()
}

func assertApplicationInstancesEqual(t *testing.T, expected *ApplicationInstance, actual *ApplicationInstance) {
	if diff := cmp.Diff(expected.desiredResources.GetAsMap(), actual.desiredResources.GetAsMap()); diff != "" {
		t.Error(diff)
	}

	if diff := cmp.Diff(
		expected.presentMonitoredResources.GetAsMap(),
		actual.presentMonitoredResources.GetAsMap(),
	); diff != "" {
		t.Error(diff)
	}

	if diff := cmp.Diff(
		expected.childMonitoredResources.GetAsMap(),
		actual.childMonitoredResources.GetAsMap(),
	); diff != "" {
		t.Error(diff)
	}

	if diff := cmp.Diff(
		expected.missingMonitoredResources.GetAsMap(),
		actual.missingMonitoredResources.GetAsMap(),
	); diff != "" {
		t.Error(diff)
	}

	if diff := cmp.Diff(
		expected.unknownMonitoredResources.GetAsMap(),
		actual.unknownMonitoredResources.GetAsMap(),
	); diff != "" {
		t.Error(diff)
	}
}

func TestReplaceDesiredResources_DropsResourcesAbsentFromTheList(t *testing.T) {
	kept := NewDesiredResourceBuilder().WithName("kept").Build()
	dropped := NewDesiredResourceBuilder().WithName("dropped").Build()

	applicationInstance := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&kept, &dropped}).
		Build()

	applicationInstance.ReplaceDesiredResources(context.TODO(), "cluster-id", map[kube.ResourceKey]*DesiredResource{
		kept.ResourceKey(): &kept,
	})

	// The status of a resource that is no longer desired must go too, otherwise it keeps being
	// reported to Server as part of the application instance.
	expected := NewApplicationInstanceBuilder().
		WithDesiredResources([]*DesiredResource{&kept}).
		WithUnknownMonitoredResources([]*UnknownMonitoredResource{
			NewUnknownMonitoredResource("cluster-id", &kept, ""),
		}).
		Build()

	assertApplicationInstancesEqual(t, &expected, &applicationInstance)
}
