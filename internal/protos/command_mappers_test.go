package protos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestToClusterDesiredResource_NilManifest_DoesNotPanic(t *testing.T) {
	in := &DesiredResource{
		DesiredResourceId: &DesiredResourceId{Value: "desired-1"},
		ResourceDetails: &DesiredResourceDetails{
			AssumedNamespace: "ns",
			GroupVersionKind: &GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
			Name:             "cm",
		},
		Manifest: nil,
	}

	out := in.ToClusterDesiredResource()

	assert.NotNil(t, out)
	assert.Nil(t, out.Manifest, "nil proto Manifest should map to nil cluster Manifest")
	assert.Equal(t, "cm", out.Details.Name)
	assert.Equal(t, "ConfigMap", out.Details.ManifestType.Kind)
	assert.Nil(t, out.Version)
}

func TestToClusterDesiredResourceForVersion_NilVersion_DoesNotPanic(t *testing.T) {
	in := &DesiredResource{
		DesiredResourceId: &DesiredResourceId{Value: "desired-1"},
		ResourceDetails: &DesiredResourceDetails{
			AssumedNamespace: "ns",
			GroupVersionKind: &GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
			Name:             "cm",
		},
		Manifest: nil,
	}

	out := in.ToClusterDesiredResourceForVersion(nil)

	assert.NotNil(t, out)
	assert.Nil(t, out.Version)
}

func desiredResourceProto(orphanedAt *timestamppb.Timestamp) *DesiredResource {
	return &DesiredResource{
		DesiredResourceId: &DesiredResourceId{Value: "desired-1"},
		ResourceDetails: &DesiredResourceDetails{
			GroupVersionKind: &GroupVersionKind{Version: "v1", Kind: "ConfigMap"},
			Name:             "cm",
		},
		OrphanedAt: orphanedAt,
	}
}

func TestToClusterDesiredResource_OrphanedAtIsCarriedAcross(t *testing.T) {
	orphanedAt := time.Date(2026, time.August, 6, 9, 30, 0, 0, time.UTC)

	out := desiredResourceProto(timestamppb.New(orphanedAt)).ToClusterDesiredResource()

	assert.True(t, out.IsOrphaned())
	assert.NotNil(t, out.OrphanedAt)
	assert.True(t, orphanedAt.Equal(*out.OrphanedAt))
}

// An older server never sets the timestamp, and an unset timestamp must stay nil rather than becoming
// the Unix epoch, which would read as an orphan
func TestToClusterDesiredResource_OrphanedAtAbsent_IsNotOrphan(t *testing.T) {
	out := desiredResourceProto(nil).ToClusterDesiredResource()

	assert.Nil(t, out.OrphanedAt)
	assert.False(t, out.IsOrphaned())
}
