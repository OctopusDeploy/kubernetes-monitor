# Changelog

## [0.43.0](https://github.com/OctopusDeploy/kubernetes-monitor/compare/kubernetes-monitor-chart-v0.42.0...kubernetes-monitor-chart-v0.43.0) (2026-09-24)


### Features

* **main:** release 0.31.2 ([#3](https://github.com/OctopusDeploy/kubernetes-monitor/issues/3)) ([340f5a8](https://github.com/OctopusDeploy/kubernetes-monitor/commit/340f5a8e87e256e0f638b60032ae13874d44020e))

## Earlier releases

These entries preserve the release history before the public repository was created.

## 0.42.0 (2026-09-24)


### Features

* **main:** release 0.31.1


### Bug Fixes

* default kubernetes monitor nodeSelector to Linux nodes

## 0.41.1 (2026-09-21)


### Bug Fixes

* use correct service account names in monitor helpers

## 0.41.0 (2026-09-07)


### Features

* **main:** release 0.31.0

## 0.40.0 (2026-09-01)


### Features

* **main:** release 0.30.0

## 0.39.0 (2026-08-28)


### Features

* **main:** release 0.29.2

## 0.38.0 (2026-08-23)


### Features

* **main:** release 0.29.1

## 0.37.0 (2026-08-18)


### Features

* **main:** release 0.29.0

## 0.36.0 (2026-08-07)


### ⚠ BREAKING CHANGES

* Enable compression over gRPC by default (Set gzip compression speed to fastest)


### Features

* **main:** release 0.28.0


### Bug Fixes

* int values aren't written correctly to the file-based store (local dev)
* Set gzip compression speed to fastest

## 0.35.0 (2026-08-05)


### Features

* **main:** release 0.27.3


### Bug Fixes

* updates are dropped when the resource snapshot exceeds the max gRPC message size

## 0.34.0 (2026-08-04)


### Features

* **main:** release 0.27.2

## 0.33.0 (2026-08-03)


### Features

* **main:** release 0.27.1

## 0.32.0 (2026-07-27)


### Features

* Add more debug logs
* **main:** release 0.27.0

## 0.31.0 (2026-06-22)


### Features

* **chart:** 'monitor.rules' for setting custom list of permitted resources for monitoring
* **main:** release 0.26.0

## 0.30.0 (2026-06-18)


### Features

* **main:** release 0.25.0

## 0.29.0 (2026-06-09)


### Features

* **main:** release 0.24.1

## 0.28.0 (2026-05-07)


### Features

* **main:** release 0.24.0
* security context to comply restricted-v2 openshift SCC by default

## 0.27.0 (2026-04-27)


### Features

* **main:** release 0.21.0
* **main:** release 0.22.0
* **main:** release 0.23.0

## 0.26.0 (2026-03-19)


### Features

* **main:** release 0.20.1

## 0.25.0 (2026-03-04)


### Features

* **main:** release 0.20.0
* namespace-scoped monitoring for restricted RBAC environments

## 0.24.0 (2026-01-14)


### Features

* add custom spec and env values
* **main:** release 0.19.2
* **main:** release 0.19.3

## 0.23.0 (2025-12-09)


### Features

* allow PVM for serviceAccountTokens

## 0.22.0 (2025-11-24)


### Features

* Add namespaced role functionality
* fix custom CA in monitor deployment

## 0.21.0 (2025-11-18)


### Features

* **main:** release 0.19.1

## 0.20.0 (2025-10-09)


### Features

* allow server access token to be specified with an existing secret

## 0.19.1 (2025-09-23)


### Bug Fixes

* add namespace to helm templates
* align secret name with kubernetes agent chart

## 0.19.0 (2025-09-22)


### Features

* **main:** release 0.19.0
* pass custom CA through to registration pod

## 0.18.0 (2025-07-14)


### Features

* **main:** release 0.18.1


### Bug Fixes

* allow registration to complete with expired credentials

## 0.17.0 (2025-07-09)


### Features

* **main:** release 0.18.0

## 0.16.0 (2025-07-01)


### Features

* add custom CA support
* **main:** release 0.17.0

## 0.15.1 (2025-07-01)


### Bug Fixes

* update the chart config store

## 0.15.0 (2025-07-01)


### Features

* **main:** release 0.16.0
* Refactor to use Viper for configuration management

## 0.14.0 (2025-06-18)


### Features

* **main:** release 0.15.2

## 0.13.0 (2025-05-22)


### Features

* **main:** release 0.15.1

## 0.12.0 (2025-05-12)


### Features

* **main:** release 0.15.0

## 0.11.0 (2025-04-08)


### Features

* **main:** release 0.14.0

## 0.10.0 (2025-04-03)


### Features

* **main:** release 0.13.0

## 0.9.0 (2025-03-28)


### Features

* **main:** release 0.12.0

## 0.8.0 (2025-03-20)


### Features

* **main:** release 0.11.0

## 0.7.0 (2025-03-17)


### Features

* **main:** release 0.10.1

## 0.6.0 (2025-03-14)


### Features

* **main:** release 0.10.0


### Bug Fixes

* Make chart compatible with uppercase alias

## 0.5.0 (2025-03-12)


### Features

* **main:** release 0.9.0

## 0.4.1 (2025-03-03)


### Bug Fixes

* Run hooks on install as well as upgrade

## 0.4.0 (2025-02-20)


### Features

* **main:** release 0.8.1


### Bug Fixes

* fix issues with pre-installation

## 0.3.0 (2025-02-20)


### Features

* Clean up helm chart and use pre-install hook
* **main:** release 0.8.0
* publish helm chart


### Bug Fixes

* helm chart errors
* update chart description
* update secret resource to be non-capitalised
* Update snapshot tests


### Miscellaneous Chores

* release 0.1.0

## 0.1.1 (2024-12-05)


### Bug Fixes

* update secret resource to be non-capitalised

## 0.1.0 (2024-12-03)


### Features

* Clean up helm chart and use pre-install hook
* publish helm chart


### Bug Fixes

* helm chart errors
* Update snapshot tests


### Miscellaneous Chores

* release 0.1.0

## 0.1.0 (2024-11-25)


### Features

* publish helm chart


### Bug Fixes

* helm chart errors


### Miscellaneous Chores

* release 0.1.0
