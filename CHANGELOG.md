# Changelog

## Earlier releases

These entries preserve the release history before the public repository was created.
Links to private commits and pull requests have been omitted; version numbers are unchanged.

## 0.31.1 (2026-09-24)


### Bug Fixes

* **deps:** update module go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp to v1.45.0 [security]
* **deps:** update module google.golang.org/grpc to v1.83.2 [security]

## 0.31.0 (2026-09-07)


### Features

* let the monitor recover after a cloud instance move
* rebuild the connection when Octopus Server stops answering

## 0.30.0 (2026-09-01)


### Features

* send replace command when rehydrating desired resources

## 0.29.2 (2026-08-28)


### Bug Fixes

* Prevent locks from hierarchy iteration

## 0.29.1 (2026-08-23)


### Bug Fixes

* Resolve high memory usage

## 0.29.0 (2026-08-18)


### Features

* Report orphaned status

## 0.28.0 (2026-08-07)


### ⚠ BREAKING CHANGES

* Enable compression over gRPC by default (Set gzip compression speed to fastest)

### Bug Fixes

* int values aren't written correctly to the file-based store (local dev)
* Set gzip compression speed to fastest

## 0.27.3 (2026-08-05)


### Bug Fixes

* updates are dropped when the resource snapshot exceeds the max gRPC message size

## 0.27.2 (2026-08-04)


### Bug Fixes

* treat RST_STREAM resets as recoverable with exponential backoff

## 0.27.1 (2026-08-03)


### Bug Fixes

* Deleted CRD-based resource is stuck in orphaned state when CRD is removed when monitor is offline

## 0.27.0 (2026-07-27)


### Features

* Add more debug logs

## 0.26.0 (2026-06-22)


### Features

* **monitor:** work with limited permissions (custom list of permitted resources)


### Bug Fixes

* **deps:** resolve Renovate status check URL issue

## 0.25.0 (2026-06-18)


### Features

* rollout health status


### Bug Fixes

* **deps:** update module go.opentelemetry.io/otel to v1.41.0 [security]
* **deps:** update module go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp to v1.43.0 [security]
* **deps:** update module go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp to v1.43.0 [security]
* **deps:** bump golang.org/x/net to v0.55.0 to fix CVEs

## 0.24.1 (2026-05-26)


### Bug Fixes

* cve-2026-33186

## 0.24.0 (2026-05-06)


### Features

* **Dockerfile:** non-root user by default

## 0.23.0 (2026-04-27)


### Features

* add new message DeleteDesiredResourcesCommand

## 0.22.0 (2026-04-23)


### Features

* add tests for memory usage and cache ordering


### Bug Fixes

* do not cache every unstructured

## 0.21.0 (2026-04-22)


### Features

* allow us to specify Pyroscope server


### Bug Fixes

* stop failing entirely when permissions are constrained


### Reverts

* Remove SBOM from pipeline

## 0.20.1 (2026-03-19)


### Bug Fixes

* Use exponential retry for errors that occur during server outages

## 0.20.0 (2026-03-04)


### Features

* namespace-scoped monitoring for restricted RBAC environments

## 0.19.3 (2026-01-08)


### Bug Fixes

* implement read only store if the config dir can't be created

## 0.19.2 (2025-12-15)


### Bug Fixes

* properly wrap config errors

## 0.19.1 (2025-11-18)


### Bug Fixes

* better startup logs

## 0.19.0 (2025-09-22)


### Features

* pass custom CA through to registration pod

## 0.18.1 (2025-07-14)


### Bug Fixes

* allow registration to complete with expired credentials

## 0.18.0 (2025-07-09)


### Features

* update registration endpoint

## 0.17.0 (2025-07-01)


### Features

* add custom CA support

## 0.16.0 (2025-06-27)


### Features

* add thumbprint as chained authentication
* Refactor to use Viper for configuration management

## 0.15.2 (2025-06-18)


### Bug Fixes

* stop pushing extra image and fix flaky test

## 0.15.1 (2025-05-22)


### Bug Fixes

* invalidate discovery client

## 0.15.0 (2025-05-12)


### Features

* add pyroscope
* add tracing
* parallelise
* send JSON Patches to Octopus Server


### Bug Fixes

* fix bug using config-path flag
* handle EOFs when they aren't thrown by the IO package

## 0.14.0 (2025-04-08)


### Features

* reduce max gRPC window size to 1MB


### Bug Fixes

* Fix concurrent map reads and writes
* remove last applied annotation

## 0.13.0 (2025-04-03)


### Features

* Retries and Resiliency


### Bug Fixes

* load owner ID on the fly
* remove last occurance of unnecessary slice init

## 0.12.0 (2025-03-28)


### Features

* custom health checks for degraded deployments
* Send failure to get logs or events error to Octopus 

## 0.11.0 (2025-03-20)


### Features

* Implement event fetching


### Bug Fixes

* fix special case when events are singular

## 0.10.1 (2025-03-17)


### Bug Fixes

* allow okm to send updates on cluster changes
* concurrency fixes
* fix missing unstructured data

## 0.10.0 (2025-03-13)


### Features

* Add root parent resource ID to child resource

## 0.9.0 (2025-03-12)


### Features

* initial pod log implementation


### Bug Fixes

* check for existing authentication secret

## 0.8.1 (2025-02-20)


### Bug Fixes

* fix issues with pre-installation

## 0.8.0 (2025-02-20)


### Features

* change default resource monitor period
* register OnResourceUpdated during cluster creation
* send monitored resource updates as we receive desired resources
* Send yaml manifest for live resources


### Bug Fixes

* keep helm chart and okm versions the same
* Remove grpc:// protocol statements from incoming connection values
* simplify mutex locking

## 0.7.0 (2025-01-22)


### ⚠ BREAKING CHANGES

* update grpc contracts to use separate monitored resources

### Features

* Handle resources that don't support health checking
* Sanitize Secret resources
* update grpc contracts to use separate monitored resources


### Bug Fixes

* add tests for GetChangesForUpdatedResource
* bad comparison function causing flakey tests

## 0.6.0 (2024-12-03)


### ⚠ BREAKING CHANGES

* Don't crash when monitoring cluster scoped resources

### Features

* Clean up helm chart and use pre-install hook


### Bug Fixes

* Don't crash when monitoring cluster scoped resources

## 0.5.0 (2024-11-25)


### Features

* Add correlation ID to outgoing gRPC context
* Detect missing resources
* publish helm chart
* support configurations without persistent storage


### Bug Fixes

* fixes to registration process
* helm chart errors

## 0.4.1 (2024-11-08)


### Bug Fixes

* Short term workaround to get configmaps showing as healthy

## 0.4.0 (2024-11-06)


### ⚠ BREAKING CHANGES

* Configuration changes
* update grpc contracts
* replace discovery loop with gitops-engine

### Features

* add cluster_id to ReplaceLiveResourcesRequest
* Add helm chart
* Add initial installation process
* Add initial machine association during registration
* Cache manifests only from desired resources
* Send health status
* Send live status
* Split auth and encryption
* update and delete live resources from k8s events


### Bug Fixes

* change logging to only log when actually sending update
* integration tests build
* Support implicit namespace matching workaround


### Miscellaneous Chores

* cleanup config!


### Code Refactoring

* replace discovery loop with gitops-engine
* update grpc contracts

## 0.3.1 (2024-07-01)


### Bug Fixes

* allow use of environment variables to set server URL

## 0.3.0 (2024-06-28)


### Features

* Add resource tree
* Add simple control via CRD
* Create project structure and split into packages
* gRPC comms with Server
* send children resources to Server

## 0.2.0 (2024-06-12)


### Features

* just list all our deployments for now


### Bug Fixes

* remove accidental quotation

## 0.1.1 (2024-06-12)


### Bug Fixes

* use version properly

## 0.1.0 (2024-06-12)


### Features

* add initial build


### Miscellaneous Chores

* release 0.1.0
