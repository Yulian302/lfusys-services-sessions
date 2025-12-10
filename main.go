package main

import (
	"context"
	"log"
	"net"

	common "github.com/Yulian302/lfusys-services-commons"
	_ "github.com/joho/godotenv/autoload"
	"google.golang.org/grpc"
)

func main() {
	cfg := common.LoadConfig()

	store := NewStore()

	grpcServer := grpc.NewServer()

	l, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("grpc error: failed to listen to %v", cfg.GRPCAddr)
	}
	defer l.Close()

	svc := NewService(store)
	svc.Create(context.Background())
	NewGrpcHandler(grpcServer, cfg.Tracing)

	log.Printf("Grpc server started at %v", cfg.GRPCAddr)

	if err := grpcServer.Serve(l); err != nil {
		log.Fatal("cannot start grpc server: ", err.Error())
	}
}
