# Kubernetes Monitor

The Kubernetes Monitor runs inside your Kubernetes cluster and reports resource state back to [Octopus Deploy](https://octopus.com). It maintains a persistent gRPC connection to the Octopus server, receives commands describing which resources to track, and continuously compares the desired state against the actual state in the cluster. It will also serve on-demand container logs and Kubernetes events back to the server for troubleshooting.

See our documentation on [octopus.com](https://octopus.com/docs/kubernetes/targets/kubernetes-agent/kubernetes-monitor) for more information.

There is more detailed Helm chart documentation in this repo [charts/kubernetes-monitor/README.md](charts/kubernetes-monitor/README.md).

## CI/CD

### Building

This project utilizes Github Actions to build, test and release the application and Helm chart.

Additionally, we make use of [Depot](https://depot.dev/) for faster builds than what Github Actions provides.

You can find the workflows in [.github/workflows](.github/workflows).

### Releasing

For a step by step guide to getting your changes into production, see the [releasing cheat sheet](docs/releasing-cheat-sheet.md)

We use [release please](https://github.com/googleapis/release-please/blob/main/README.md) for our releases, which relies on conventional commits to determine changelogs and thus how the version increments according to semver.

#### Application Image

Once changes are merged via a PR into `main`, release please will create a PR for you to review and merge. 

This PR will update the Helm chart with the new application version, but will not create a new helm release.

Once merged, Github actions will build the release and push the artifact to a staging repository, after which an Octopus project will pick it up and release that to Dockerhub.

#### Helm Chart

This follows the same process as the application, however only changes to the `charts/kubernetes-monitor` directory will trigger a new release PR.

The Kubernetes monitor Helm chart is released to customers as a subchart of the [Kubernetes agent's Helm chart](https://github.com/OctopusDeploy/helm-charts/tree/main/charts/kubernetes-agent)

## Development

### Pre-requisites

- Go
- A [Kubernetes agent](https://octopus.com/docs/kubernetes/targets/kubernetes-agent/) deployment target should be active.
- Your active kubeconfig should be pointing to the same cluster as the kubernetes agent.

### Running it

There are run configurations for Goland and VSCode with sensible defaults pre-configured as command line arguments, running one of these should get you where you need to go.

When running the Kubernetes monitor for the first time, you will need to register the monitor with Octopus server. This will create configuration files for you that can be re-used.

Pick the `register` launch configuration and run it.

For standard registration, you'll be prompted to set:
- Machine name
  - This is the name of the Kubernetes agent deployment target
- Space ID
  - The space where the Kubernetes agent was created

There is also a configuration for saving the configuration in Kubernetes, which replicates how it works when installing via Helm.

> ℹ️ The default configuration is setup to connect to an Octopus server running on your localhost, but this can be changed to point to a cloud instance if required

Once registered, the `run` launch configuration will help you pick which existing configuration you want to use and run the monitor in debug.  

If you're replacing a monitor that usually runs in-cluster, ensure it's stopped prior to debugging using the monitor, as we currently only support a single instance of the monitor connecting to server at a time.

### Debugging
If you are running the Kubernetes monitor in a dev environment, you can use the `--debug` flag to enable debug logging. If you need more logs around gRPC communications you can add the following environment variables:

> ⚠️ **This will log out all the data being sent and received, including any sensitive information. Use with caution.**

- `GRPC_GO_LOG_SEVERITY_LEVEL=info` - This will make gRPC log messages at the `info` level and above.
- `GRPC_GO_LOG_VERBOSITY_LEVEL=99` - This will allow gRPC to log messages around the actual gRPC calls.
- `GODEBUG=http2debug=2` - This will make Go log out the http2 frames that are being sent and received. 