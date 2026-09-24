package kwok_integration

import (
	"context"
	"testing"

	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/support"
	"sigs.k8s.io/e2e-framework/support/kwok"
)

type testContextKey string

var (
	testenv         env.Environment
	kwokClusterName string
)

// This setup is used for all tests in this package so it should be kept as minimal as possible
func TestMain(m *testing.M) {
	testenv = env.New()
	kwokClusterName = envconf.RandomName("kwok-cluster", 16)
	namespace := envconf.RandomName("kwok-ns", 16)
	nodeName := "node-1"

	testenv.Setup(
		envfuncs.CreateClusterWithConfig(customKwokProvider(), kwokClusterName, "kwok-config.yaml"),
		envfuncs.CreateNamespace(namespace),
		CreateNode(nodeName),
	)

	testenv.Finish(
		envfuncs.ExportClusterLogs(kwokClusterName, "./test-cluster-logs"),
		envfuncs.DestroyCluster(kwokClusterName),
	)

	testenv.Run(m)
}

func CreateNode(name string) env.Func {
	return func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
		return ctx, createNode(ctx, cfg, name)
	}
}

// The default kwok provider doesn't correctly pass through the required wait command
// This simple function results in passing a wait duration through as is probably intended
func customKwokProvider() support.E2EClusterProvider {
	return kwok.NewCluster("")
}
