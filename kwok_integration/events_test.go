package kwok_integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
)

func TestGetEvents(t *testing.T) {
	testPodName := "events-test"

	eventsFeature := features.New("corev1/events").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			createBasicPod(ctx, cfg, t, testPodName)
			testCluster := createTestCluster(t, cfg)
			return context.WithValue(ctx, testContextKey("testCluster"), testCluster)
		}).
		Assess("get events", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			testCluster := ctx.Value(testContextKey("testCluster")).(*cluster.Cluster)

			returnedEvents, err := testCluster.GetEvents(cfg.Namespace(), testPodName, "Pod", ctx)
			if err != nil {
				t.Fatal(err)
			}

			if len(returnedEvents) != 1 {
				t.Error("Expected 1 event to be returned, got", len(returnedEvents))
			}

			expectedEvent := cluster.Event{
				FirstObservedTime:   time.Time{},
				LastObservedTime:    time.Time{},
				Count:               1,
				Action:              "Binding",
				Reason:              "Scheduled",
				Note:                "",
				ReportingController: "default-scheduler",
				ReportingInstance:   "",
				Type:                "Normal",
				Manifest:            "",
			}
			event := returnedEvents[0]
			// There's no way to reliably get the timestamps from the event in an integration test, so we ignore them and will test in unit tests instead
			if diff := cmp.Diff(
				expectedEvent,
				event,
				cmpopts.IgnoreFields(
					cluster.Event{},
					"Note",
					"ReportingInstance",
					"Manifest",
					"FirstObservedTime",
					"LastObservedTime",
				),
			); diff != "" {
				t.Error(diff)
			}

			return ctx
		}).
		Feature()

	testenv.Test(t, eventsFeature)
}
