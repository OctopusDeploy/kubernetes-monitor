//go:build memory_profile || snapshot_gen

package kwok_integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// Fixed so restored snapshots are portable across runs (env.Namespace() is random).
const snapshotNamespace = "mem-profile"

const snapshotDir = "testdata/snapshots"

type snapshotManifest struct {
	Namespace   string `json:"namespace"`
	NamePrefix  string `json:"namePrefix"`
	Deployments int    `json:"deployments"`
	Replicas    int32  `json:"replicas"`
}

// Only deployments/replicas affect etcd contents; other memTestConfig fields are runtime-only.
func snapshotBasename(cfg memTestConfig) string {
	return fmt.Sprintf("mem-d%d-r%d", cfg.deployments, cfg.replicas)
}

func snapshotPaths(cfg memTestConfig) (dbPath, manifestPath string) {
	base := filepath.Join(snapshotDir, snapshotBasename(cfg))
	return base + ".db", base + ".json"
}

func writeSnapshotManifest(path string, m snapshotManifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func readSnapshotManifest(path string) (*snapshotManifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m snapshotManifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &m, nil
}

func snapshotExists(cfg memTestConfig) bool {
	dbPath, manifestPath := snapshotPaths(cfg)
	if _, err := os.Stat(dbPath); err != nil {
		return false
	}
	if _, err := os.Stat(manifestPath); err != nil {
		return false
	}
	return true
}

func runKwokctl(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "kwokctl", args...)
	return cmd.CombinedOutput()
}

func snapshotSave(ctx context.Context, clusterName, dbPath string) error {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return err
	}
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return err
	}
	out, err := runKwokctl(ctx, "snapshot", "save",
		"--name", clusterName,
		"--path", abs,
		"--format", "etcd",
	)
	if err != nil {
		return fmt.Errorf("kwokctl snapshot save: %w\n%s", err, string(out))
	}
	return nil
}

func snapshotRestore(ctx context.Context, clusterName, dbPath string) error {
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return err
	}
	out, err := runKwokctl(ctx, "snapshot", "restore",
		"--name", clusterName,
		"--path", abs,
		"--format", "etcd",
	)
	if err != nil {
		return fmt.Errorf("kwokctl snapshot restore: %w\n%s", err, string(out))
	}
	return nil
}
