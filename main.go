package main

import (
	"context"
	"log"
	"net"

	common "github.com/Yulian302/lfusys-services-commons"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	_ "github.com/joho/godotenv/autoload"
	"google.golang.org/grpc"
)

func main() {
	cfg := common.LoadConfig()

	// verify aws credentials
	if cfg.AWS_ACCESS_KEY_ID == "" || cfg.AWS_SECRET_ACCESS_KEY == "" {
		log.Fatal("aws security credentials were not found")
	}

	// db client
	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(cfg.AWS_REGION))
	if err != nil {
		log.Fatalf("failed to load aws config: %v", err)
	}
	client := dynamodb.NewFromConfig(awsCfg)
	store := NewStore(client, "uploads")

	grpcServer := grpc.NewServer()

	l, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("grpc error: failed to listen to %v", cfg.GRPCAddr)
	}
	defer l.Close()

	svc := NewService(store)
	svc.Create(context.Background())
	NewGrpcHandler(grpcServer, cfg.Tracing, store, &cfg)

	log.Printf("Grpc server started at %v", cfg.GRPCAddr)

	if err := grpcServer.Serve(l); err != nil {
		log.Fatal("cannot start grpc server: ", err.Error())
	}
}
