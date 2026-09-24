# Memory profile test

Gated behind the `memory_profile` build tag so it doesn't run with the
default `kwok_integration` suite.

Run a single scenario:

```sh
go test -tags=memory_profile -timeout=30m \
    -run TestCacheMemoryProfile_1PercentDesired \
    ./kwok_integration/
```

Run all scenarios:

```sh
go test -tags=memory_profile -timeout=60m \
    -run TestCacheMemoryProfile ./kwok_integration/
```

Each scenario creates deployments in a KWOK cluster, seeds some of them as
desired resources, and calls `cluster.Cluster.Sync()` to exercise the real
gitops-engine cluster cache.

If Pyroscope is reachable at `memTestPyroscopeURL` (currently
`http://localhost:5053`), heap samples are shipped there, tagged with the
test name and scenario parameters. Otherwise, pprof heap dumps go to
`testdata/memory-profiles/`.

When Pyroscope is up, each test logs a Grafana Explore link
(`http://localhost:5054`) pre-filtered to that run's `test` label. The
profile is also tagged with `deployments`, `replicas`, `desired_pct`, and
`phase` for ad-hoc filtering.

To add a new scenario, copy one of the existing `TestCacheMemoryProfile_*`
functions and change the `memTestConfig` values.

## Pre-populated etcd snapshots

Snapshots are **not committed** (they're too large) and **must be generated
locally before the first profile run**. Run the matching generator under the
`snapshot_gen` build tag:

```sh
go test -tags=snapshot_gen -timeout=30m \
    -run TestGenerateSnapshot_3kDeployments15Replicas \
    ./kwok_integration/
```

This writes:

- `testdata/snapshots/mem-d<deployments>-r<replicas>.db` — the etcd snapshot.
- `testdata/snapshots/mem-d<deployments>-r<replicas>.json` — a sidecar that
  records the namespace and namePrefix so the profile test knows where to
  look for resources after restoring.

The profile test then restores the matching snapshot via `kwokctl snapshot
restore --format etcd` (effectively instantaneous). If no matching snapshot
is present, it falls back to creating resources via the API — slow, but
works for a one-off run.

The filename only depends on `deployments` and `replicas`, since those are
the only fields that affect etcd contents. Scenarios that differ only by
`desiredPct` (runtime-only) share a snapshot, so one generation covers both
the 1% and 10% desired scenarios.

Regenerate whenever the shape of the test resources changes (e.g. you added
a new field to the Deployment/ReplicaSet/Pod templates in `createStack`).
