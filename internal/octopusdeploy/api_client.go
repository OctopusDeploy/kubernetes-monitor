package octopusdeploy

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/OctopusDeploy/go-octopusdeploy/v2/pkg/client"
	"github.com/OctopusDeploy/go-octopusdeploy/v2/pkg/machines"

	"github.com/octopusdeploy/kubernetes-monitor/internal/certificate"
)

// OctopusClient Wrapper around the Octopus Go Client to handle few edge cases before we update the go library
// This will be removed after the go client is updated to support the endpoints we need
type OctopusClient struct {
	Client *client.Client
}

func NewOctopusApiClient(
	serverAccessToken string,
	serverUrl *url.URL,
	caCertificatePath string,
) (*OctopusClient, error) {
	credentials, err := NewOctopusCredential(serverAccessToken)
	if err != nil {
		return nil, err
	}

	// If we have a custom CA cert, we'll create a HTTP client to use it - otherwise nil to use default
	var httpClient *http.Client
	if caCertificatePath != "" {
		httpClient, _ = getHttpClient(caCertificatePath)
	}

	c, err := client.NewClientWithCredentials(httpClient, serverUrl, credentials, "", "OKM")
	if err != nil {
		return nil, err
	}

	return &OctopusClient{
		Client: c,
	}, nil
}

func NewOctopusCredential(credential string) (client.ICredential, error) {
	if client.IsAPIKey(credential) {
		return client.NewApiKey(credential)
	} else {
		return client.NewAccessToken(credential)
	}
}

type RegisterKubernetesMonitorCommand struct {
	InstallationId string
	MachineId      string
	Thumbprint     string
}

type RegisterKubernetesMonitorResponse struct {
	Resource              KubernetesMonitorResource
	AuthenticationToken   string
	CertificateThumbprint string
}

type KubernetesMonitorResource struct {
	Id             string
	MachineId      string
	InstallationId string
	SpaceId        string
}

func (oc *OctopusClient) RegisterKubernetesMonitor(
	installationId, spaceId, machineName string,
) (RegisterKubernetesMonitorResponse, error) {
	var machineId string
	maxAttempts := 10
	currentAttempts := 0

	for machineId == "" && currentAttempts < maxAttempts {
		machinesResponse, err := machines.Get(oc.Client, spaceId, machines.MachinesQuery{
			Name: machineName,
			Take: 1,
		})
		if err != nil {
			return RegisterKubernetesMonitorResponse{}, err
		}

		if machinesResponse != nil && len(machinesResponse.Items) > 0 {
			machineId = machinesResponse.Items[0].ID
			break
		}

		currentAttempts += 1
		time.Sleep(5 * time.Second)
	}

	if machineId == "" {
		return RegisterKubernetesMonitorResponse{}, fmt.Errorf(
			"failed to find a machine with the name %s after 10 attempts",
			machineName,
		)
	}

	path := fmt.Sprintf("%s/observability/kubernetes-monitors", spaceId)
	kubernetesMonitorCreateCommand := RegisterKubernetesMonitorCommand{
		InstallationId: installationId,
		MachineId:      machineId,
	}

	var kubernetesMonitorResponse RegisterKubernetesMonitorResponse
	resp, err := oc.Client.Sling().
		New().
		Post(path).
		BodyJSON(kubernetesMonitorCreateCommand).
		ReceiveSuccess(&kubernetesMonitorResponse)

	if resp == nil || resp.StatusCode != 200 {
		return RegisterKubernetesMonitorResponse{}, fmt.Errorf("error registering kubernetes monitor: %s", resp.Status)
	}

	if err != nil {
		return RegisterKubernetesMonitorResponse{}, err
	}

	// This indicates that the monitor wasn't unmarshalled properly
	// If there was actually an issue, we would have received a non-200 status code
	if kubernetesMonitorResponse.Resource.Id == "" {
		return RegisterKubernetesMonitorResponse{}, ErrorMonitorIdMissing
	}

	return kubernetesMonitorResponse, nil
}

func getHttpClient(caCertificatePath string) (*http.Client, error) {
	rootCertificates, err := certificate.GetRootCertificatePool(caCertificatePath)
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{
		RootCAs: rootCertificates,
	}
	tr := &http.Transport{TLSClientConfig: tlsConfig}
	return &http.Client{Transport: tr}, nil
}
