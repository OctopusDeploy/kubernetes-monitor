# Releasing Cheat Sheet

Step by step guide to get your changes into customers hands

## Kubernetes Monitor
1. Make your changes, create a PR in this repo, ensuring it's titled according to [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/)
   1. When merging, ensure you use a squash merge and the resulting commit is named as per [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/)
2. Let the `release-please` workflow run and create a release PR for the application
   1. Approve and merge this PR
   2. NB This only happens if you have made changes to the application
3. Let the `release-please` workflow run again and create a release PR for the Helm chart
   1. Approve and merge this PR
4. Your changes will be available once Octopus has promoted the images to Dockerhub

## Kubernetes Agent

1. Update the version of the Kubernetes monitor sub-chart in the [Kubernetes agent chart](https://github.com/OctopusDeploy/helm-charts/blob/main/charts/kubernetes-agent/Chart.yaml#L13)
2. The [Helm charts repo](https://github.com/OctopusDeploy/helm-charts/tree/main) uses `changesets` for releasing - see [the readme](https://github.com/OctopusDeploy/helm-charts/blob/main/README.md) for details on how to release any changes made```

> ℹ️ Because the repo for the Kubernetes is not public (yet), we need to ensure to duplicate the [Helm readme](charts/kubernetes-monitor/README.md) file from this repo to the helm charts repo whenever there are changes.