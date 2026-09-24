package octopusdeploy

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

// RpcAuth is the Octopus auth scheme this monitor still speaks: an installation
// id and a bare authentication token. Octopus Server reads it through the same
// handler as client-id/Bearer and resolves both to the same client, but treats
// these two headers as legacy. It stays here rather than in octopus-grpc so that
// the shared library only carries the scheme new clients should use.
type RpcAuth struct {
	InstallationId      string
	AuthenticationToken string
}

func (r RpcAuth) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{
		"installation-id": r.InstallationId,
		"authentication":  r.AuthenticationToken,
	}, nil
}

func (r RpcAuth) RequireTransportSecurity() bool {
	return true
}

func AppendCorrelationId(ctx context.Context) (context.Context, string) {
	correlationId := uuid.New().String()
	ctx = metadata.AppendToOutgoingContext(ctx, "X-Correlation-ID", correlationId)
	return ctx, correlationId
}
