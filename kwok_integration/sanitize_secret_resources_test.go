package kwok_integration

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
)

func TestSanitizeSecrets(t *testing.T) {
	secretName := "mysecret"
	secretDataName := "secret-password"
	secretDataValue := "purple-monkey-dishwasher"

	deploymentFeature := features.New("corev1/secret").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			secretSpec := corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: cfg.Namespace()},
				Data: map[string][]byte{
					secretDataName: []byte(secretDataValue),
				},
				Type: "Opaque",
			}

			createdSecret := createSecret(ctx, cfg, t, secretSpec)
			gvk, err := getGroupVersionKind(cfg, createdSecret)
			if err != nil {
				t.Fatal(err)
				return nil
			}

			testCluster := createTestCluster(t, cfg)
			setupApplicationInstance(createdSecret, gvk, testCluster)
			return context.WithValue(ctx, testContextKey("testCluster"), testCluster)
		}).
		Assess("data hashed", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			resource := getPresentMonitoredResource(ctx, t)
			if resource.SyncStatus.Status != cluster.SyncStatusInSync {
				t.Errorf("Sync status is not InSync: %s", resource.SyncStatus.Status)
			}

			secretReported := parseSecret(resource, t)
			secretActual := getActualSecret(ctx, t, cfg, secretName)

			validateDataHashed(secretReported, secretActual, secretDataName, secretDataValue, t)
			return ctx
		}).
		Feature()

	testenv.Test(t, deploymentFeature)
}

func getActualSecret(ctx context.Context, t *testing.T, cfg *envconf.Config, secretName string) corev1.Secret {
	var secretActual corev1.Secret
	if err := cfg.Client().Resources().Get(ctx, secretName, cfg.Namespace(), &secretActual); err != nil {
		t.Fatal(err)
	}
	return secretActual
}

func setupApplicationInstance(createdSecret corev1.Secret, gvk *schema.GroupVersionKind, testCluster *cluster.Cluster) {
	salt := crypto.HashSalt("Projects-123/Environments-45/Tenants-6")

	// The Secret object holds the value in a byte[]
	// We need to convert that to a base64 string (like in the YAML) before hashing it
	hashedData := map[string]string{}
	for key, binaryValue := range createdSecret.Data {
		base64Encoded := base64.StdEncoding.EncodeToString(binaryValue)
		hashString := crypto.HashString(salt, base64Encoded)
		hashedData[key] = hashString
	}

	apiVersion := getApiVersion(*gvk)
	desiredResource := cluster.NewDesiredResourceBuilder().
		WithName(createdSecret.Name).
		WithNamespace(createdSecret.Namespace).
		WithKind(gvk.Kind).
		WithApiVersion(apiVersion).
		WithManifest(&unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": apiVersion,
				"kind":       gvk.Kind,
				"metadata": map[string]interface{}{
					"name":        createdSecret.Name,
					"namespace":   createdSecret.Namespace,
					"annotations": createdSecret.Annotations,
				},
				"data": hashedData,
			},
		}).Build()

	applicationInstance := cluster.NewApplicationInstanceBuilder().
		WithHashSalt(salt).
		WithDesiredResources([]*cluster.DesiredResource{&desiredResource}).
		Build()

	testCluster.ApplicationInstances.UpsertApplicationInstance(&applicationInstance)
}

func getPresentMonitoredResource(ctx context.Context, t *testing.T) *cluster.PresentMonitoredResource {
	testCluster := ctx.Value(testContextKey("testCluster")).(*cluster.Cluster)

	_ = testCluster.Sync()

	var applicationInstanceUpdates []cluster.ApplicationInstanceChanges
	for applicationInstanceUpdate := range testCluster.GetApplicationInstanceUpdates(context.TODO()) {
		applicationInstanceUpdates = append(applicationInstanceUpdates, *applicationInstanceUpdate)
	}

	if len(applicationInstanceUpdates) != 1 {
		t.Fatalf("Expected 1 ApplicationInstanceUpdate, found %d", len(applicationInstanceUpdates))
	}

	return applicationInstanceUpdates[0].PresentMonitoredResources[0]
}

func parseSecret(liveSecret *cluster.PresentMonitoredResource, t *testing.T) corev1.Secret {
	var secretReported corev1.Secret
	err := runtime.DefaultUnstructuredConverter.
		FromUnstructured((*unstructured.Unstructured)(liveSecret.Manifest).UnstructuredContent(), &secretReported)
	if err != nil {
		t.Fatal(err)
	}
	return secretReported
}

func validateDataHashed(
	secretReported corev1.Secret, secretActual corev1.Secret, dataName string, dataValue string, t *testing.T,
) {
	if string(secretReported.Data[dataName]) == string(secretActual.Data[dataName]) {
		t.Errorf("Not expecting actual data")
	}

	if string(secretReported.Data[dataName]) == dataValue {
		t.Errorf("Expected sync status to be")
	}
}

func getGroupVersionKind(cfg *envconf.Config, createdSecret corev1.Secret) (*schema.GroupVersionKind, error) {
	gvks, boolean, err := cfg.Client().Resources().GetScheme().ObjectKinds(&createdSecret)
	if err != nil {
		return nil, err
	}

	if len(gvks) == 0 || len(gvks) > 1 || boolean {
		return nil, errors.New(fmt.Sprintf("unable to find resource kind of %s", createdSecret.Kind))
	}

	return &gvks[0], nil
}

func getApiVersion(gvk schema.GroupVersionKind) string {
	return strings.TrimLeft(fmt.Sprintf("%s/%s", gvk.Group, gvk.Version), "/")
}

func createSecret(ctx context.Context, cfg *envconf.Config, t *testing.T, secretSpec corev1.Secret) corev1.Secret {
	t.Log("Creating secret", secretSpec.Name)
	if err := cfg.Client().Resources().Create(ctx, &secretSpec); err != nil {
		t.Fatal(err)
	}

	var createdSecret corev1.Secret
	if err := cfg.Client().Resources().Get(ctx, secretSpec.Name, cfg.Namespace(), &createdSecret); err != nil {
		t.Fatal(err)
	}

	return createdSecret
}
