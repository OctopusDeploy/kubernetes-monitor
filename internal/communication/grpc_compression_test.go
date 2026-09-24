package communication

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/octopusdeploy/kubernetes-monitor/internal/protos"
)

const (
	messageSizeLimit   = 16 * 1024
	effectivelyNoLimit = messageSizeLimit * 100
)

type stubLiveStatusServer struct {
	pb.UnimplementedLiveStatusServiceServer
}

func (s *stubLiveStatusServer) ReplaceMonitoredResources(
	_ context.Context, _ *pb.ReplaceMonitoredResourcesRequest,
) (*pb.ReplaceMonitoredResourcesResponse, error) {
	return &pb.ReplaceMonitoredResourcesResponse{}, nil
}

// startStubServer starts an in-process LiveStatusService that accepts arbitrarily large
// messages, so that a failed send can only be caused by the client's own limits.
func startStubServer(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer(grpc.MaxRecvMsgSize(effectivelyNoLimit))
	pb.RegisterLiveStatusServiceServer(server, &stubLiveStatusServer{})

	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	return listener.Addr().String()
}

// newLargeCompressibleRequest builds a request exceeding messageSizeLimit out of repetitive,
// YAML-like manifests, so it mirrors a real cluster snapshot but compresses well.
func newLargeCompressibleRequest(t *testing.T) *pb.ReplaceMonitoredResourcesRequest {
	t.Helper()

	const resourceCount = 100
	manifest := strings.Repeat(
		"apiVersion: v1\nkind: Pod\nmetadata:\n  name: some-pod\n  namespace: some-namespace\n",
		20,
	)

	resources := make([]*pb.PresentMonitoredResource, 0, resourceCount)
	for i := 0; i < resourceCount; i++ {
		resources = append(resources, &pb.PresentMonitoredResource{
			Manifest: &pb.YamlManifest{Value: manifest},
		})
	}

	request := &pb.ReplaceMonitoredResourcesRequest{PresentMonitoredResources: resources}

	encoded, err := proto.Marshal(request)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}
	if len(encoded) <= messageSizeLimit {
		t.Fatalf(
			"test payload is %d bytes, which does not exceed the %d byte limit it is meant to exercise",
			len(encoded), messageSizeLimit,
		)
	}

	return request
}

// measureRequestSizes reports the marshalled and gzipped sizes of request, using the same
// registered compressor grpc-go uses when enforcing MaxCallSendMsgSize.
func measureRequestSizes(t *testing.T, request *pb.ReplaceMonitoredResourcesRequest) (uncompressed, compressed int) {
	t.Helper()

	encoded, err := proto.Marshal(request)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	compressor := encoding.GetCompressor(gzip.Name)
	if compressor == nil {
		t.Fatalf("the %q compressor is not registered, so GetGrpcCallOptions cannot work", gzip.Name)
	}

	var buffer bytes.Buffer
	writer, err := compressor.Compress(&buffer)
	if err != nil {
		t.Fatalf("failed to open a gzip writer: %v", err)
	}
	if _, err := writer.Write(encoded); err != nil {
		t.Fatalf("failed to compress the payload: %v", err)
	}
	// The gzip footer is only written on Close, so the length is not final until after it.
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to finish compressing the payload: %v", err)
	}

	return len(encoded), buffer.Len()
}

func dialStubServer(t *testing.T, address string, callOptions ...grpc.CallOption) pb.LiveStatusServiceClient {
	t.Helper()

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(callOptions...),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return pb.NewLiveStatusServiceClient(conn)
}

func sendLimitBetween(t *testing.T, compressedSize, uncompressedSize int) int {
	t.Helper()

	sendLimit := (compressedSize + uncompressedSize) / 2
	if !(compressedSize < sendLimit && sendLimit < uncompressedSize) {
		t.Fatalf(
			"test is vacuous: a send limit of %d bytes does not sit strictly between the compressed "+
				"size (%d) and the uncompressed size (%d)",
			sendLimit, compressedSize, uncompressedSize,
		)
	}

	return sendLimit
}

func TestGrpcCompression(t *testing.T) {
	request := newLargeCompressibleRequest(t)
	uncompressedSize, compressedSize := measureRequestSizes(t, request)
	sendLimit := sendLimitBetween(t, compressedSize, uncompressedSize)

	t.Run("compression enabled keeps the snapshot under the send limit", func(t *testing.T) {
		address := startStubServer(t)
		callOptions := GetGrpcCallOptions(sendLimit, effectivelyNoLimit, true)

		_, err := dialStubServer(t, address, callOptions...).
			ReplaceMonitoredResources(context.Background(), request)
		if err != nil {
			t.Fatalf(
				"a %d byte snapshot that gzips to %d bytes was refused by a %d byte send limit: %v. Is the send limit being enforced on the uncompressed payload? https://github.com/grpc/grpc-go/blob/master/stream.go#L1071 (comment: TODO(dfawley): should we be checking len(data) instead?)",
				uncompressedSize,
				compressedSize,
				sendLimit,
				err,
			)
		}
	})

	t.Run("compression disabled sends the snapshot uncompressed", func(t *testing.T) {
		address := startStubServer(t)
		callOptions := GetGrpcCallOptions(sendLimit, effectivelyNoLimit, false)

		_, err := dialStubServer(t, address, callOptions...).
			ReplaceMonitoredResources(context.Background(), request)
		if err == nil {
			t.Fatalf(
				"a %d byte snapshot was accepted under a %d byte send limit with compression disabled, "+
					"so it was still compressed and the setting has no effect on the wire",
				uncompressedSize, sendLimit,
			)
		}
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("expected ResourceExhausted from the client's own send limit, got: %v", err)
		}
	})
}
