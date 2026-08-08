package otlp

import (
	"context"
	"io"
	"net"
	"net/http"

	"github.com/nickpatt/traceforge/internal/model"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Sink receives flattened spans decoded from OTLP. It must return an error when
// it cannot accept (e.g. Kafka buffer full) so the server can signal
// backpressure (gRPC RESOURCE_EXHAUSTED / HTTP 429) to OTLP clients.
type Sink func(ctx context.Context, spans []*model.Span) error

// GRPCServer implements the OTLP TraceService.
type GRPCServer struct {
	coltracepb.UnimplementedTraceServiceServer
	sink Sink
}

// Export handles an OTLP export request.
func (g *GRPCServer) Export(
	ctx context.Context, req *coltracepb.ExportTraceServiceRequest,
) (*coltracepb.ExportTraceServiceResponse, error) {
	spans := FlattenRequest(req.GetResourceSpans())
	if err := g.sink(ctx, spans); err != nil {
		return nil, err
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// ServeGRPC starts the OTLP gRPC server on addr (default :4317). Blocks.
func ServeGRPC(addr string, sink Sink) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, &GRPCServer{sink: sink})
	return srv.Serve(lis)
}

// ServeHTTP starts the OTLP HTTP server on addr (default :4318). Handles
// POST /v1/traces with protobuf bodies. Blocks.
func ServeHTTP(addr string, sink Sink) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req coltracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			http.Error(w, "invalid protobuf", http.StatusBadRequest)
			return
		}
		spans := FlattenRequest(req.GetResourceSpans())
		if err := sink(r.Context(), spans); err != nil {
			http.Error(w, "backpressure", http.StatusTooManyRequests)
			return
		}
		resp, _ := proto.Marshal(&coltracepb.ExportTraceServiceResponse{})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(resp)
	})
	return http.ListenAndServe(addr, mux)
}
