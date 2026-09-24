package kwok_integration

import (
	"context"
	"fmt"
	"testing"

	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

func TestGetLogs(t *testing.T) {
	podNames := []string{"long-logs", "short-logs"}

	logsFeature := features.New("corev1/logs").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			for _, podName := range podNames {
				createBasicPod(ctx, cfg, t, podName)
			}
			testCluster := createTestCluster(t, cfg)
			return context.WithValue(ctx, testContextKey("testCluster"), testCluster)
		}).
		Assess("truncate long logs", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			podName := "long-logs"
			testCluster := ctx.Value(testContextKey("testCluster")).(*cluster.Cluster)

			returnedLogs, err := testCluster.GetContainerLogs(
				cfg.Namespace(),
				podName,
				fmt.Sprintf("%s-container", podName),
				false,
				ctx,
			)
			if err != nil {
				t.Fatal(err)
			}

			if len(returnedLogs) != 149796 {
				t.Error("Expected 149796 log lines to be returned, got", len(returnedLogs))
			}

			if returnedLogs[0].Message != "1 179265: Fri Mar  7 05:45:09 UTC 2025" {
				t.Error(
					"Expected first log line to be '1 179265: Fri Mar  7 05:45:09 UTC 2025', got",
					returnedLogs[0].Message,
				)
			}
			return ctx
		}).
		Assess("do not truncate short logs", func(
			ctx context.Context, t *testing.T, cfg *envconf.Config,
		) context.Context {
			podName := "short-logs"
			testCluster := ctx.Value(testContextKey("testCluster")).(*cluster.Cluster)

			returnedLogs, err := testCluster.GetContainerLogs(
				cfg.Namespace(),
				podName,
				fmt.Sprintf("%s-container", podName),
				false,
				ctx,
			)
			if err != nil {
				t.Fatal(err)
			}

			if len(returnedLogs) != 100 {
				t.Error("Expected 100 log lines to be returned, got", len(returnedLogs))
			}

			if returnedLogs[0].Message != "1 0: Fri Mar  7 05:44:19 UTC 2025" {
				t.Error(
					"Expected first log line to be '1 0: Fri Mar  7 05:44:19 UTC 2025', got",
					returnedLogs[0].Message,
				)
			}
			return ctx
		}).
		Feature()

	testenv.Test(t, logsFeature)
}
