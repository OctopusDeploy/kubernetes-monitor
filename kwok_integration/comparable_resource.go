package kwok_integration

import (
	"regexp"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

func mapPresentMonitoredResourceToComparableResource(update *cluster.PresentMonitoredResource) ComparableResource {
	// Remove random char from the end of resource names so we can compare to known values
	re := regexp.MustCompile(`(.*)?\-[a-z1-9]{10}(\-[a-z1-9]{5})?$`)
	trimmedName := re.ReplaceAllString(update.Name, `$1`)

	return ComparableResource{
		Name:      trimmedName,
		Namespace: update.Namespace,
		Kind:      update.GroupVersionKind.Kind,
	}
}

func mapChildMonitoredResourceToComparableResource(update *cluster.ChildMonitoredResource) ComparableResource {
	// Remove random char from the end of resource names so we can compare to known values
	re := regexp.MustCompile(`(.*)?\-[a-z1-9]{10}(\-[a-z1-9]{5})?$`)
	trimmedName := re.ReplaceAllString(update.Name, `$1`)

	return ComparableResource{
		Name:      trimmedName,
		Namespace: update.Namespace,
		Kind:      update.GroupVersionKind.Kind,
	}
}

func mapMissingMonitoredResourceToComparableResource(update *cluster.MissingMonitoredResource) ComparableResource {
	// Remove random char from the end of resource names so we can compare to known values
	re := regexp.MustCompile(`(.*)?\-[a-z1-9]{10}(\-[a-z1-9]{5})?$`)
	trimmedName := re.ReplaceAllString(update.Name, `$1`)

	return ComparableResource{
		Name:      trimmedName,
		Namespace: update.Namespace,
		Kind:      update.GroupVersionKind.Kind,
	}
}

type ComparableResource struct {
	Name      string // Full name, or a prefix to compare to a name
	Namespace string
	Kind      string
}

func SortComparableResource(a, b ComparableResource) bool {
	return a.Kind+a.Namespace+a.Name < b.Kind+b.Namespace+b.Name
}
