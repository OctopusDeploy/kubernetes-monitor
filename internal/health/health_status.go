package health

import (
	"fmt"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	argoLua "github.com/argoproj/argo-cd/v3/util/lua"
	appsv1 "k8s.io/api/apps/v1"
	k8sSchema "k8s.io/apimachinery/pkg/runtime/schema"
)

func GetHealthStatus(manifest *unstructured.Unstructured) *health.HealthStatus {
	if manifest == nil {
		return &health.HealthStatus{
			Status:  health.HealthStatusUnknown,
			Message: "Missing resource manifest",
		}
	}

	resourceHealth, err := health.GetResourceHealth(manifest, &CustomHealthChecks{})
	if err != nil {
		return &health.HealthStatus{
			Status:  health.HealthStatusUnknown,
			Message: err.Error(),
		}
	}
	return resourceHealth
}

type CustomHealthChecks struct{}

// GetResourceHealth is a HealthOverride used by the gitops-engine to provide custom health checks for resources
// health.GetResourceHealth() will call this method first. If it returns (nil, nil) the default health check will be used instead.
func (*CustomHealthChecks) GetResourceHealth(obj *unstructured.Unstructured) (*health.HealthStatus, error) {
	gvk := obj.GroupVersionKind()
	switch gvk {

	case k8sSchema.GroupVersionKind{Group: "argoproj.io", Version: "v1alpha1", Kind: "Rollout"}:
		vm := argoLua.VM{}
		script, useOpenLibs, err := vm.GetHealthScript(obj)
		if err != nil {
			return nil, err
		}
		if script != "" {
			vm.UseOpenLibs = useOpenLibs
			return vm.ExecuteHealthLua(obj, script)
		}
		return nil, nil

	case appsv1.SchemeGroupVersion.WithKind(kube.DeploymentKind):
		var deployment appsv1.Deployment
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &deployment)
		if err != nil {
			return nil, fmt.Errorf("failed to convert unstructured Deployment to typed: %v", err)
		}
		return getAppsv1DeploymentHealth(&deployment)

	case appsv1.SchemeGroupVersion.WithKind(kube.ReplicaSetKind):
		var replicaSet appsv1.ReplicaSet
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &replicaSet)
		if err != nil {
			return nil, fmt.Errorf("failed to convert unstructured ReplicaSet to typed: %v", err)
		}
		return getAppsv1ReplicaSetHealth(&replicaSet)

	default:
		return nil, nil
	}
}
