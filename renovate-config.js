// Self-hosted Renovate configuration for kubernetes-monitor.
//
// This is run by the "Renovate update dependencies" GitHub Actions workflow
// (.github/workflows/update-dependencies.yml) using renovatebot/github-action.
//
// It manages three things in this repo, separated into clearly-labelled
// packageRules sections below:
//   1. Go modules        (go.mod / go.sum)        -> manager "gomod"
//   2. Docker base images (Dockerfile)            -> manager "dockerfile"
//   3. GitHub Actions     (.github/workflows/*)   -> manager "github-actions"

const preCannedPrNotes = {
  greenMeansGo: [
    "Green means go. Any issues in this PR should be caught as part of our tests and/or builds.",
  ],
  reviewBreakingChanges: [
    "Please review breaking changes and check whether we are impacted based on our usages.",
  ],
};

module.exports = {
  timezone: "Australia/Brisbane",
  schedule: ["* * * * 1-5"],
  requireConfig: "optional",
  onboarding: false,

  platform: "github",
  repositories: ["OctopusDeploy/kubernetes-monitor"],
  reviewers: ["team:team-swordfish"],
  labels: ["dependencies"],
  branchPrefix: "renovate/",

  // Commit as our own bot account instead of Renovate's Mend-owned default.
  gitAuthor: "team-yosemite-bot <teamyosemitebot@octopus.com>",

  enabledManagers: ["gomod", "dockerfile", "github-actions"],

  // Run `go mod tidy` after a gomod update so go.sum and the indirect
  // requirements in go.mod are reconciled within the same PR, instead of
  // leaving the bumped go.mod inconsistent. No-op for the dockerfile and
  // github-actions managers.
  // https://docs.renovatebot.com/configuration-options/#postupdateoptions
  postUpdateOptions: ["gomodTidy"],

  // Renovate will create a new issue in the repository with a "dashboard" that
  // gives an overview of the status of all updates.
  // https://docs.renovatebot.com/key-concepts/dashboard/
  dependencyDashboard: true,
  dependencyDashboardTitle: "Dependency Dashboard",

  // Limit the amount of PRs created at once.
  prConcurrentLimit: 10,
  prHourlyLimit: 5,

  // This repo uses Conventional Commits (release-please generates the changelog
  // from them), so keep Renovate's commit messages semantic e.g.
  // "chore(deps): update ...".
  // https://docs.renovatebot.com/semantic-commits/
  semanticCommits: "enabled",

  // Security (vulnerability) updates: emit `fix(deps): ...` instead of the
  // default `chore(deps): ...`. release-please hides `chore` from the changelog
  // but surfaces `fix` under "Bug Fixes" (and it drives a patch release), so
  // this ensures security bumps are recorded in the release notes. These
  // options apply only to vulnerability-remediation branches (the
  // `renovate/*-vulnerability` branches with `[security]` in the PR title);
  // routine dependency updates stay `chore(deps): ...` and remain hidden.
  // https://docs.renovatebot.com/configuration-options/#vulnerabilityalerts
  vulnerabilityAlerts: {
    semanticCommitType: "fix",
    // `addLabels` appends to the base `labels: ["dependencies"]`, so security
    // PRs are tagged both "dependencies" and "security".
    addLabels: ["security"],
  },

  // Per-package rules.
  // Renovate evaluates all packageRules and does not stop at the first match,
  // so order them in ascending order of importance: later rules override
  // earlier ones.
  // https://docs.renovatebot.com/configuration-options/#packagerules
  packageRules: [
    // ************************************************************************
    // GO MODULES (manager: gomod)
    // ************************************************************************
    {
      // The `go` directive / toolchain bump in go.mod. Moving Go major/minor
      // versions can have real behavioural impact and is worth a deliberate
      // look, so don't lump it in with library updates.
      matchManagers: ["gomod"],
      matchDepNames: ["go"],
      addLabels: ["go"],
      prBodyNotes: [
        ...preCannedPrNotes.reviewBreakingChanges,
        "This bumps the Go version in go.mod. Confirm the Dockerfile build image (golang:x.y-alpine) and CI Go version are aligned.",
      ],
    },
    {
      // golang.org/x/* repos move together; grouping reduces PR noise.
      groupName: "golang.org/x packages",
      matchManagers: ["gomod"],
      matchPackageNames: ["golang.org/x/**"],
      addLabels: ["go"],
      prBodyNotes: [...preCannedPrNotes.greenMeansGo],
    },
    {
      // OpenTelemetry Go libraries are versioned and released together; a
      // partial update tends to break compilation, so update them as a group.
      groupName: "opentelemetry-go",
      matchManagers: ["gomod"],
      matchSourceUrls: [
        "https://github.com/open-telemetry/opentelemetry-go",
        "https://github.com/open-telemetry/opentelemetry-go-contrib",
      ],
      addLabels: ["go"],
      prBodyNotes: [...preCannedPrNotes.greenMeansGo],
    },
    {
      // k8s.io/* and sigs.k8s.io/* are pinned by argo-cd, which forces exact
      // Kubernetes versions via the `replace` block in go.mod. Bumping them on
      // their own breaks the build against argo-cd, so never update them
      // standalone -- the matching versions are bumped by hand as part of the
      // argo-cd update (see the argo-cd rule below).
      //
      // sigs.k8s.io/e2e-framework and sigs.k8s.io/yaml are excluded: they're
      // versioned independently of the Kubernetes release train, so let them
      // keep updating on their own.
      matchManagers: ["gomod"],
      matchPackageNames: [
        "k8s.io/**",
        "sigs.k8s.io/**",
        "!sigs.k8s.io/e2e-framework",
        "!sigs.k8s.io/yaml",
      ],
      enabled: false,
    },
    {
      // argo-cd and its nested gitops-engine/v3 module are tagged together on
      // the same commit, and argo-cd's util/lua hands us gitops-engine types,
      // so a gitops-engine bump on its own could leave argo-cd on an API it
      // wasn't built against. Group them so both move in one PR.
      //
      // argo-cd also drives the Kubernetes stack (frozen above), so its update
      // PR has to carry those bumps by hand.
      groupName: "argo-cd",
      matchManagers: ["gomod"],
      matchPackageNames: ["github.com/argoproj/argo-cd/**"],
      addLabels: ["go"],
      prBodyNotes: [
        ...preCannedPrNotes.reviewBreakingChanges,
        "argo-cd pins the Kubernetes (k8s.io/*, sigs.k8s.io/*) versions, and Renovate is configured not to bump those on their own. Update the `require` and `replace` entries in go.mod by hand in this branch to match argo-cd's go.mod, and raise the `go` directive (plus the Dockerfile golang image and the golangci-lint pins) if argo-cd's is higher. Then run `go mod tidy`.",
      ],
    },
    {
      // spf13's CLI/config libraries (cobra, pflag, viper) are versioned
      // independently but routinely bumped together and are low-risk; group
      // them into one PR to cut review noise.
      groupName: "spf13 packages",
      matchManagers: ["gomod"],
      matchPackageNames: ["github.com/spf13/**"],
      addLabels: ["go"],
      prBodyNotes: [...preCannedPrNotes.greenMeansGo],
    },
    {
      // Catch-all label for any remaining Go module update.
      matchManagers: ["gomod"],
      addLabels: ["go"],
      prBodyNotes: [...preCannedPrNotes.greenMeansGo],
    },

    // ************************************************************************
    // DOCKER (manager: dockerfile)
    // ************************************************************************
    {
      // The golang build image. Pin to major.minor (e.g. 1.24) and keep it in
      // step with the `go` directive in go.mod rather than chasing every patch.
      matchManagers: ["dockerfile"],
      matchDepNames: ["golang"],
      // Match a major.minor docker tag like "1.24-alpine"; ignore patch bumps.
      versioning: "regex:^(?<major>\\d+)\\.(?<minor>\\d+)(-(?<compatibility>.*))?$",
      addLabels: ["docker"],
      prBodyNotes: [
        "Keep the build image Go version aligned with the `go` directive in go.mod.",
      ],
    },
    {
      // gcr.io/distroless/static is published as a rolling :latest tag with no
      // semver, so pin and update by digest to get reproducible, auditable
      // builds. https://docs.renovatebot.com/docker/#digest-pinning
      matchManagers: ["dockerfile"],
      matchDepNames: ["gcr.io/distroless/static"],
      addLabels: ["docker"],
      pinDigests: true,
      prBodyNotes: [
        ...preCannedPrNotes.greenMeansGo,
        "distroless/static updates by digest. A green build is sufficient verification.",
      ],
    },
    {
      // Catch-all label for any remaining Dockerfile image update.
      matchManagers: ["dockerfile"],
      addLabels: ["docker"],
      prBodyNotes: [...preCannedPrNotes.greenMeansGo],
    },

    // ************************************************************************
    // GITHUB ACTIONS (manager: github-actions)
    // ************************************************************************
    {
      // For security, pin every action (official actions/** included) to an
      // immutable commit SHA rather than a mutable tag. Renovate keeps the
      // "# vX.Y.Z" comment updated.
      // https://docs.renovatebot.com/modules/manager/github-actions/#digest-pinning-and-updating
      matchManagers: ["github-actions"],
      // Only pin real `uses:` action references (depType "action"). The
      // github-actions manager also extracts tool versions from `with:` inputs
      // (e.g. azure/setup-helm's `version:` becomes dep "helm", depType
      // "uses-with", datasource github-releases). Trying to pin a digest onto a
      // plain version string can't find a target and fails the whole grouped
      // pin-dependencies branch, so restrict pinning to actual actions.
      matchDepTypes: ["action"],
      addLabels: ["github-actions"],
      pinDigests: true,
      // Track the full vX.Y.Z release tag so the pinned SHA gets a `# v4.2.1`
      // comment instead of the bare `# v4` from a `@v4` reference. Same as
      // helpers:pinGitHubActionDigestsToSemver, inlined so it sits alongside
      // the rest of the github-actions rules.
      // https://docs.renovatebot.com/presets-helpers/#helperspingithubactiondigeststosemver
      extractVersion: "^(?<version>v?\\d+\\.\\d+\\.\\d+)$",
      versioning: "regex:^v?(?<major>\\d+)(\\.(?<minor>\\d+)\\.(?<patch>\\d+))?$",
    },
    {
      // Batch all `uses:` action updates into a
      // single PR to cut review noise; they're independent and bump frequently.
      // Restricted to depType "action" so `uses-with` tool versions (e.g. the
      // helm version from azure/setup-helm) stay in their own PRs.
      matchManagers: ["github-actions"],
      matchDepTypes: ["action"],
      groupName: "github-actions",
    },

    // ************************************************************************
    // BASE RULE (applies to everything)
    // ************************************************************************
    {
      matchPackageNames: ["*"],
      // Give third-party releases a couple of days to surface issues before we
      // pick them up, without waiting too long.
      minimumReleaseAge: "2 days",
    },
  ],
};
