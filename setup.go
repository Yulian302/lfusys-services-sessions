package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"

	pb "github.com/Yulian302/lfusys-services-commons/api"
	"github.com/Yulian302/lfusys-services-commons/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
)

type App struct {
	Server *grpc.Server

	DynamoDB *dynamodb.Client
	Redis    *redis.Client
	Sqs      *sqs.Client

	Config    config.Config
	AwsConfig aws.Config

	Services       *Services
	TracerProvider *trace.TracerProvider
}

func SetupApp() (*App, error) {
	cfg := config.LoadConfig()

	if err := cfg.AWSConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	awsCfg, err := initAWS(*cfg.AWSConfig)
	if err != nil {
		return nil, err
	}

	db := initDynamo(awsCfg)
	if db == nil {
		return nil, errors.New("could not init dynamodb")
	}

	rdb := initRedis(*cfg.RedisConfig)
	if rdb == nil {
		return nil, errors.New("could not init redis")
	}

	sqs := initSqs(awsCfg)
	if sqs == nil {
		return nil, errors.New("could not init sqs")
	}

	app := &App{
		DynamoDB: db,
		Redis:    rdb,
		Sqs:      sqs,

		Config:    cfg,
		AwsConfig: awsCfg,
	}

	app.Services = BuildServices(app)

	return app, nil
}

func (a *App) Run() error {
	a.Server = grpc.NewServer()

	l, err := net.Listen("tcp", a.Config.ServiceConfig.SessionGRPCAddr)
	if err != nil {
		return err
	}

	a.RegisterHandlers()

	return a.Server.Serve(l)
}

func initAWS(cfg config.AWSConfig) (aws.Config, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion(cfg.Region),
	)
	if err != nil {
		return aws.Config{}, fmt.Errorf("load aws config: %w", err)
	}
	return awsCfg, nil
}

func initDynamo(cfg aws.Config) *dynamodb.Client {
	return dynamodb.NewFromConfig(cfg)
}

func initRedis(cfg config.RedisConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     cfg.HOST,
		Password: "",
		DB:       0,
	})
}

func initSqs(cfg aws.Config) *sqs.Client {
	return sqs.NewFromConfig(cfg)
}

func (a *App) Shutdown(ctx context.Context) error {
	log.Println("starting graceful shutdown")

	if a.Server != nil {
		done := make(chan struct{})
		go func() {
			a.Server.GracefulStop()
			close(done)
		}()

		select {
		case <-done:
		case <-ctx.Done():
			a.Server.Stop() // force
		}
	}

	if a.Services != nil {
		if err := a.Services.Shutdown(ctx); err != nil {
			log.Printf("services shutdown error: %v", err)
		}
	}

	if a.Redis != nil {
		if err := a.Redis.Close(); err != nil {
			log.Printf("redis close error: %v", err)
		}
	}

	if a.TracerProvider != nil {
		if err := a.TracerProvider.Shutdown(ctx); err != nil {
			log.Printf("tracer shutdown error: %v", err)
		}
	}

	log.Println("graceful shutdown complete")
	return nil
}

func (a *App) RegisterHandlers() {
	pb.RegisterUploaderServer(a.Server, a.Services.UploadHandler)
}
