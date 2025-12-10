package main

import (
	"context"
	"fmt"

	pb "github.com/Yulian302/lfusys-services-commons/api"
	"google.golang.org/grpc"
)

type grpcHandler struct {
	pb.UnimplementedGreeterServer
	tracing bool
}

func NewGrpcHandler(grpcServer *grpc.Server, tracing bool) {
	handler := &grpcHandler{
		tracing: tracing,
	}
	pb.RegisterGreeterServer(grpcServer, handler)
}

func (h *grpcHandler) SayHello(ctx context.Context, req *pb.HelloReq) (*pb.HelloReply, error) {
	return &pb.HelloReply{Msg: fmt.Sprintf("Hello, %s", req.Name)}, nil
}