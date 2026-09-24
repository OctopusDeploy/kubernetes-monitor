package commands

import (
	"context"
	"log/slog"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	clusterPkg "github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
	"github.com/octopusdeploy/kubernetes-monitor/internal/crypto"
	pb "github.com/octopusdeploy/kubernetes-monitor/internal/protos"
)

const name = "github.com/octopusdeploy/kubernetes-monitor/internal/commands"

var tracer = otel.Tracer(name)

func (c *CommandHandler) handleUpdateDesiredResourcesCommand(command *pb.UpdateDesiredResourcesCommand) error {
	ctx, span := tracer.Start(context.TODO(), "CommandHandler.handleUpdateDesiredResourcesCommand")
	defer span.End()

	c.Logger.
		With(slog.Any("ApplicationInstanceId", command.ApplicationInstanceId)).
		With(slog.Int("DesiredResourceCount", len(command.DesiredResources))).
		Debug("Received update to desired resources")

	cluster, err := c.Clusters.EnsureCluster(ctx, command.ClusterId.FromProto())
	if err != nil {
		return err
	}

	span.AddEvent(
		"Updating desired resources",
		trace.WithAttributes(attribute.String("applicationInstanceId", command.GetApplicationInstanceId().GetValue())),
	)
	span.SetAttributes(attribute.Int("desiredResourceCount", len(command.DesiredResources)))
	applicationInstanceId := command.ApplicationInstanceId.FromProto()
	desiredResources := map[kube.ResourceKey]*clusterPkg.DesiredResource{}
	for _, updatedResource := range command.DesiredResources {
		newDesiredResource := updatedResource.ToClusterDesiredResourceForVersion(command.Version)
		resourceKey := newDesiredResource.ResourceKey()
		desiredResources[resourceKey] = newDesiredResource
	}

	return cluster.MergeDesiredResources(
		ctx,
		applicationInstanceId,
		desiredResources,
		crypto.HashSalt(command.HashSalt),
	)
}

func (c *CommandHandler) handleReplaceDesiredResourcesCommand(command *pb.ReplaceDesiredResourcesCommand) error {
	ctx, span := tracer.Start(context.TODO(), "CommandHandler.handleReplaceDesiredResourcesCommand")
	defer span.End()

	c.Logger.
		With(slog.Any("ApplicationInstanceId", command.ApplicationInstanceId)).
		With(slog.Int("DesiredResourceCount", len(command.DesiredResources))).
		Debug("Received complete desired resource list")

	cluster, err := c.Clusters.EnsureCluster(ctx, command.ClusterId.FromProto())
	if err != nil {
		return err
	}

	span.AddEvent(
		"Replacing desired resources",
		trace.WithAttributes(attribute.String("applicationInstanceId", command.GetApplicationInstanceId().GetValue())),
	)
	span.SetAttributes(attribute.Int("desiredResourceCount", len(command.DesiredResources)))
	applicationInstanceId := command.ApplicationInstanceId.FromProto()
	desiredResources := map[kube.ResourceKey]*clusterPkg.DesiredResource{}
	for _, updatedResource := range command.DesiredResources {
		newDesiredResource := updatedResource.ToClusterDesiredResource()
		resourceKey := newDesiredResource.ResourceKey()
		desiredResources[resourceKey] = newDesiredResource
	}

	return cluster.ReplaceDesiredResources(
		ctx,
		applicationInstanceId,
		desiredResources,
		crypto.HashSalt(command.HashSalt),
	)
}

func (c *CommandHandler) handlePruneOtherVersionsCommand(command *pb.PruneOtherVersionsCommand) error {
	c.Logger.With(slog.Any("ApplicationInstanceId", command.ApplicationInstanceId)).
		Debug("Received request to prune desired resources")

	cluster, err := c.Clusters.GetCluster(command.ClusterId.FromProto())
	if err != nil {
		return err
	}

	cluster.DeleteDesiredResourcesExceptForVersion(
		command.ApplicationInstanceId.FromProto(),
		command.Version.FromProto())

	return nil
}

func (c *CommandHandler) handleDeleteDesiredResourcesCommand(command *pb.DeleteDesiredResourcesCommand) error {
	c.Logger.With(slog.Any("ApplicationInstanceId", command.ApplicationInstanceId)).
		With(slog.Int("ResourceCount", len(command.ResourceIds))).
		Debug("Received request to prune specific desired resources")

	cluster, err := c.Clusters.GetCluster(command.ClusterId.FromProto())
	if err != nil {
		return err
	}

	resourceIds := make([]clusterPkg.DesiredResourceId, len(command.ResourceIds))
	for i, id := range command.ResourceIds {
		resourceIds[i] = id.FromProto()
	}

	cluster.DeleteDesiredResources(command.ApplicationInstanceId.FromProto(), resourceIds)
	return nil
}
