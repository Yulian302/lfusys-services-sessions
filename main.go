package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	common "github.com/Yulian302/lfusys-services-commons"
	pb "github.com/Yulian302/lfusys-services-commons/api"
	"github.com/Yulian302/lfusys-services-sessions/handlers"
	"github.com/Yulian302/lfusys-services-sessions/queues"
	"github.com/Yulian302/lfusys-services-sessions/services"
	"github.com/Yulian302/lfusys-services-sessions/store"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	_ "github.com/joho/godotenv/autoload"
	"google.golang.org/grpc"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := common.LoadConfig()

	if err := cfg.AWSConfig.ValidateSecrets(); err != nil {
		log.Fatal("aws security credentials were not found")
	}

	// db client
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.AWSConfig.Region))
	if err != nil {
		log.Fatalf("failed to load aws config: %v", err)
	}
	client := dynamodb.NewFromConfig(awsCfg)
	fileStore := store.NewDynamoDbFileStoreImpl(client, cfg.DynamoDBConfig.FilesTableName)
	sessionStore := store.NewSessionStoreImpl(client, cfg.DynamoDBConfig.UploadsTableName)

	sqsClient := sqs.NewFromConfig(awsCfg)
	queueUrl := fmt.Sprintf("https://sqs.%s.amazonaws.com/%s/%s.fifo", cfg.AWSConfig.Region, cfg.AWSConfig.AccountID, cfg.ServiceConfig.UploadsNotificationsQueueName)
	uploadsReceiver := queues.NewUploadsNotifyReceiveImpl(sqsClient, fileStore, sessionStore, queueUrl)

	go func() {
		if err := uploadsReceiver.Poll(ctx); err != nil {
			log.Printf("poller exited: %v", err)
		}
	}()

	sessionService := services.NewSessionServiceImpl(sessionStore)
	fileService := services.NewFileServiceImpl(fileStore)
	grpcHandler := handlers.NewGrpcHandler(sessionService, fileService, cfg.ServiceConfig.UploadsURL)

	grpcServer := grpc.NewServer()
	pb.RegisterUploaderServer(grpcServer, grpcHandler)

	l, err := net.Listen("tcp", cfg.ServiceConfig.SessionGRPCAddr)
	if err != nil {
		log.Fatalf("grpc error: failed to listen to %v", cfg.ServiceConfig.SessionGRPCAddr)
	}
	defer l.Close()

	log.Printf("Grpc server started at %v", cfg.ServiceConfig.SessionGRPCAddr)

	go func() {
		if err := grpcServer.Serve(l); err != nil {
			log.Fatal("cannot start grpc server: ", err.Error())
		}
	}()

	sig := make(chan os.Signal, 1)
	<-sig
	log.Println("shutdown signal received")

	cancel()
	grpcServer.GracefulStop()
}
