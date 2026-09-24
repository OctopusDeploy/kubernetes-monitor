package protos

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"sigs.k8s.io/yaml"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

const name = "github.com/octopusdeploy/kubernetes-monitor/internal/protos"

var tracer = otel.Tracer(name)

func toPresentMonitoredResources(resources []*cluster.PresentMonitoredResource) ([]*PresentMonitoredResource, []error) {
	monitoredResources := make([]*PresentMonitoredResource, 0, len(resources))
	errors := make([]error, 0)
	for _, resource := range resources {
		presentResource, err := toPresentMonitoredResource(resource)
		if err != nil {
			errors = append(errors, err)
			continue
		}

		monitoredResources = append(monitoredResources, presentResource)
	}
	return monitoredResources, errors
}

func toPresentMonitoredResource(stateResource *cluster.PresentMonitoredResource) (*PresentMonitoredResource, error) {
	if stateResource == nil {
		return nil, nil
	}

	gvk := stateResource.GroupVersionKind

	if stateResource.ResourceId == "" {
		return nil, fmt.Errorf(
			"ResourceId is empty for %s/%s %s: %s in %s",
			gvk.Group,
			gvk.Version,
			gvk.Kind,
			stateResource.Name,
			stateResource.Namespace,
		)
	}

	if stateResource.Manifest == nil {
		return nil, fmt.Errorf(
			"Manifest is nil for %s/%s %s: %s in %s",
			gvk.Group,
			gvk.Version,
			gvk.Kind,
			stateResource.Name,
			stateResource.Namespace,
		)
	}

	rawManifest, err := yaml.Marshal(stateResource.Manifest.Object)
	if err != nil {
		return nil, err
	}

	return &PresentMonitoredResource{
		ResourceUuid: ToUUID(stateResource.ResourceId),
		ResourceDetails: &ResourceDetails{
			Name:                stateResource.Name,
			KubernetesNamespace: stateResource.Namespace,
			GroupVersionKind: &GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    gvk.Kind,
			},
		},
		ResourceStatus:     ToResourceStatus(stateResource.Status),
		ResourceSyncStatus: ToSyncStatus(*stateResource.SyncStatus),
		ClusterId:          ToClusterId(stateResource.ClusterId),
		DesiredResourceId:  ToDesiredResourceId(stateResource.DesiredResourceId),
		Manifest:           &YamlManifest{Value: string(rawManifest[:])},
	}, nil
}

func toChildMonitoredResources(resources []*cluster.ChildMonitoredResource) ([]*ChildMonitoredResource, []error) {
	childResources := make([]*ChildMonitoredResource, 0, len(resources))
	errors := make([]error, 0)
	for _, resource := range resources {
		childResource, err := toChildMonitoredResource(resource)
		if err != nil {
			errors = append(errors, err)
			continue
		}

		childResources = append(childResources, childResource)
	}
	return childResources, errors
}

func toChildMonitoredResource(stateResource *cluster.ChildMonitoredResource) (*ChildMonitoredResource, error) {
	if stateResource == nil {
		return nil, nil
	}

	gvk := stateResource.GroupVersionKind

	if stateResource.ResourceId == "" {
		return nil, fmt.Errorf(
			"ResourceId is empty for %s/%s %s: %s in %s",
			gvk.Group,
			gvk.Version,
			gvk.Kind,
			stateResource.Name,
			stateResource.Namespace,
		)
	}

	if stateResource.OwnerId == "" {
		return nil, fmt.Errorf(
			"OwnerId is empty for %s/%s %s: %s in %s",
			gvk.Group,
			gvk.Version,
			gvk.Kind,
			stateResource.Name,
			stateResource.Namespace,
		)
	}

	if stateResource.RootOwnerId == "" {
		return nil, fmt.Errorf(
			"RootOwnerId is empty for %s/%s %s: %s in %s",
			gvk.Group,
			gvk.Version,
			gvk.Kind,
			stateResource.Name,
			stateResource.Namespace,
		)
	}

	if stateResource.Manifest == nil {
		return nil, fmt.Errorf(
			"Manifest is nil for %s/%s %s: %s in %s",
			gvk.Group,
			gvk.Version,
			gvk.Kind,
			stateResource.Name,
			stateResource.Namespace,
		)
	}

	rawManifest, err := yaml.Marshal(stateResource.Manifest.Object)
	if err != nil {
		return nil, err
	}

	return &ChildMonitoredResource{
		ResourceUuid:           ToUUID(stateResource.ResourceId),
		ParentResourceUuid:     ToUUID(stateResource.OwnerId),
		RootParentResourceUuid: ToUUID(stateResource.RootOwnerId),
		ResourceDetails: &ResourceDetails{
			Name:                stateResource.Name,
			KubernetesNamespace: stateResource.Namespace,
			GroupVersionKind: &GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    gvk.Kind,
			},
		},
		ResourceStatus: ToResourceStatus(stateResource.Status),
		ClusterId:      ToClusterId(stateResource.ClusterId),
		Manifest:       &YamlManifest{Value: string(rawManifest[:])},
	}, nil
}

func toMissingMonitoredResources(resources []*cluster.MissingMonitoredResource) []*MissingMonitoredResource {
	missingResources := make([]*MissingMonitoredResource, 0, len(resources))
	for _, resource := range resources {
		missingResources = append(missingResources, toMissingResource(resource))
	}
	return missingResources
}

func toMissingResource(stateResource *cluster.MissingMonitoredResource) *MissingMonitoredResource {
	if stateResource == nil {
		return nil
	}

	gvk := stateResource.GroupVersionKind

	return &MissingMonitoredResource{
		ResourceDetails: &ResourceDetails{
			Name:                stateResource.Name,
			KubernetesNamespace: stateResource.Namespace,
			GroupVersionKind: &GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    gvk.Kind,
			},
		},
		ResourceStatus:     ToResourceStatus(stateResource.Status),
		ResourceSyncStatus: ToSyncStatus(*stateResource.SyncStatus),
		ClusterId:          ToClusterId(stateResource.ClusterId),
		DesiredResourceId:  ToDesiredResourceId(stateResource.DesiredResourceId),
	}
}

func toUnknownMonitoredResources(resources []*cluster.UnknownMonitoredResource) []*UnknownMonitoredResource {
	unknownResources := make([]*UnknownMonitoredResource, 0, len(resources))
	for _, resource := range resources {
		unknownResources = append(unknownResources, toUnknownResource(resource))
	}
	return unknownResources
}

func toUnknownResource(stateResource *cluster.UnknownMonitoredResource) *UnknownMonitoredResource {
	if stateResource == nil {
		return nil
	}

	return &UnknownMonitoredResource{
		ClusterId:          ToClusterId(stateResource.ClusterId),
		DesiredResourceId:  ToDesiredResourceId(stateResource.DesiredResourceId),
		ResourceStatus:     ToResourceStatus(stateResource.Status),
		ResourceSyncStatus: ToSyncStatus(*stateResource.SyncStatus),
	}
}

func ToUpdateMonitoredResourceRequest(
	ctx context.Context, update cluster.ApplicationInstanceChanges,
) (*UpdateMonitoredResourcesRequest, *DeleteChildMonitoredResourcesRequest, []error) {
	var request *UpdateMonitoredResourcesRequest
	_, span := tracer.Start(ctx, "ToUpdateMonitoredResourceRequest")
	defer span.End()

	span.AddEvent("calculating present resources")
	presentResources, presentErrors := toPresentMonitoredResources(update.PresentMonitoredResources)
	span.AddEvent("calculating child resources")
	childResources, childErrors := toChildMonitoredResources(update.ChildMonitoredResources)
	span.AddEvent("finished calculating resources")
	errors := append(presentErrors, childErrors...)

	if update.GetAllResourceCount() > 0 {
		request = &UpdateMonitoredResourcesRequest{
			ApplicationInstanceId:     ToApplicationInstanceId(update.ApplicationInstanceId),
			ClusterId:                 ToClusterId(update.ClusterId),
			PresentMonitoredResources: presentResources,
			ChildMonitoredResources:   childResources,
			MissingMonitoredResources: toMissingMonitoredResources(update.MissingMonitoredResources),
			UnknownMonitoredResources: toUnknownMonitoredResources(update.UnknownMonitoredResources),
		}
	}

	return request, toDeleteRequest(update), errors
}

// ToReplaceMonitoredResourceRequest builds a complete replacement of the state Server holds for the application instance.
func ToReplaceMonitoredResourceRequest(
	update cluster.ApplicationInstanceChanges,
) (*ReplaceMonitoredResourcesRequest, []error) {
	presentResources, presentErrors := toPresentMonitoredResources(update.PresentMonitoredResources)
	childResources, childErrors := toChildMonitoredResources(update.ChildMonitoredResources)
	errors := append(presentErrors, childErrors...)

	request := &ReplaceMonitoredResourcesRequest{
		ApplicationInstanceId:     ToApplicationInstanceId(update.ApplicationInstanceId),
		ClusterId:                 ToClusterId(update.ClusterId),
		PresentMonitoredResources: presentResources,
		ChildMonitoredResources:   childResources,
		MissingMonitoredResources: toMissingMonitoredResources(update.MissingMonitoredResources),
		UnknownMonitoredResources: toUnknownMonitoredResources(update.UnknownMonitoredResources),
	}

	return request, errors
}

func toDeleteRequest(update cluster.ApplicationInstanceChanges) *DeleteChildMonitoredResourcesRequest {
	var deleteRequest *DeleteChildMonitoredResourcesRequest

	if len(update.DeletedChildResourceKeys) > 0 {
		deleteRequest = &DeleteChildMonitoredResourcesRequest{
			ApplicationInstanceId: ToApplicationInstanceId(update.ApplicationInstanceId),
			ClusterId:             ToClusterId(update.ClusterId),
			ResourceKeys:          ToResourceKeys(update.DeletedChildResourceKeys),
		}
	}

	return deleteRequest
}
