package main

import (
	"context"
	"log"
	"net"

	common "github.com/Yulian302/lfusys-services-commons"
	pb "github.com/Yulian302/lfusys-services-commons/api"
	"github.com/Yulian302/lfusys-services-sessions/handlers"
	"github.com/Yulian302/lfusys-services-sessions/services"
	"github.com/Yulian302/lfusys-services-sessions/store"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	_ "github.com/joho/godotenv/autoload"
	"google.golang.org/grpc"
)

func main() {
	cfg := common.LoadConfig()

	if err := cfg.AWSConfig.ValidateSecrets(); err != nil {
		log.Fatal("aws security credentials were not found")
	}

	// db client
	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(cfg.AWSConfig.Region))
	if err != nil {
		log.Fatalf("failed to load aws config: %v", err)
	}
	client := dynamodb.NewFromConfig(awsCfg)
	store := store.NewUploadsStoreImpl(client, cfg.DynamoDBConfig.UploadsTableName)

	sessionService := services.NewSessionServiceImpl(store)
	grpcHandler := handlers.NewGrpcHandler(sessionService, cfg.ServiceConfig.UploadsURL)

	grpcServer := grpc.NewServer()
	pb.RegisterUploaderServer(grpcServer, grpcHandler)

	l, err := net.Listen("tcp", cfg.ServiceConfig.SessionGRPCAddr)
	if err != nil {
		log.Fatalf("grpc error: failed to listen to %v", cfg.ServiceConfig.SessionGRPCAddr)
	}
	defer l.Close()

	log.Printf("Grpc server started at %v", cfg.ServiceConfig.SessionGRPCAddr)

	if err := grpcServer.Serve(l); err != nil {
		log.Fatal("cannot start grpc server: ", err.Error())
	}
}
