package main

import (
	"context"
	"fmt"
	"log"

	pb "github.com/Yulian302/lfusys-services-commons/api"
	"github.com/Yulian302/lfusys-services-commons/caching"
	"github.com/Yulian302/lfusys-services-sessions/handlers"
	"github.com/Yulian302/lfusys-services-sessions/queues"
	"github.com/Yulian302/lfusys-services-sessions/services"
	"github.com/Yulian302/lfusys-services-sessions/store"
)

type Stores struct {
	files    store.FileStore
	sessions store.SessionStore
}

type Services struct {
	Sessions services.SessionService
	Files    services.FileService
	Uploads  queues.UploadsNotifyReceiver

	Stores *Stores

	UploadHandler pb.UploaderServer
}

type Shutdowner interface {
	Shutdown(context.Context) error
}

func BuildServices(app *App) *Services {

	fileStore := store.NewDynamoDbFileStoreImpl(app.DynamoDB, app.Config.DynamoDBConfig.FilesTableName)
	sessStore := store.NewSessionStoreImpl(app.DynamoDB, app.Config.UploadsTableName)

	var cachingSvc caching.CachingService
	cachingSvc = caching.NewRedisCachingService(app.Redis)
	if app.Redis == nil {
		cachingSvc = caching.NewNullCachingService()
	}
	sessSvc := services.NewSessionServiceImpl(sessStore)
	fileSvc := services.NewFileServiceImpl(fileStore, cachingSvc)

	queueUrl := fmt.Sprintf("https://sqs.%s.amazonaws.com/%s/%s.fifo", app.Config.AWSConfig.Region, app.Config.AWSConfig.AccountID, app.Config.ServiceConfig.UploadsNotificationsQueueName)
	uploadsReceiver := queues.NewUploadsNotifyReceiveImpl(context.Background(), app.Sqs, fileStore, sessStore, cachingSvc, queueUrl)
	go uploadsReceiver.Start()

	handler := handlers.NewGrpcHandler(sessSvc, fileSvc, app.Config.ServiceConfig.UploadsURL)

	return &Services{
		Sessions: sessSvc,
		Files:    fileSvc,
		Uploads:  uploadsReceiver,

		Stores: &Stores{
			files:    fileStore,
			sessions: sessStore,
		},

		UploadHandler: handler,
	}
}

func (s *Services) Shutdown(ctx context.Context) error {
	log.Println("shutting down services")

	if s.Stores != nil {
		if err := s.Stores.Shutdown(ctx); err != nil {
			log.Printf("stores shutdown error: %v", err)
		}
	}

	if s.Uploads != nil {
		if sh, ok := s.Uploads.(Shutdowner); ok {
			if err := sh.Shutdown(ctx); err != nil {
				log.Printf("uploads receiver shutdown error: %v", err)
			}
		}
	}

	log.Println("services shutdown complete")
	return nil
}

func (s *Stores) Shutdown(ctx context.Context) error {
	log.Println("shutting down stores")

	shutdownIfPossible := func(name string, v any) {
		if sh, ok := v.(Shutdowner); ok {
			if err := sh.Shutdown(ctx); err != nil {
				log.Printf("%s store shutdown error: %v", name, err)
			}
		}
	}

	shutdownIfPossible("files", s.files)
	shutdownIfPossible("sessions", s.sessions)

	log.Println("stores shutdown complete")
	return nil
}
