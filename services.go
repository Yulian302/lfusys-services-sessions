package main

import (
	"context"
	"fmt"

	pb "github.com/Yulian302/lfusys-services-commons/api/uploader/v1"
	"github.com/Yulian302/lfusys-services-commons/caching"
	logger "github.com/Yulian302/lfusys-services-commons/logging"
	"github.com/Yulian302/lfusys-services-sessions/handlers"
	"github.com/Yulian302/lfusys-services-sessions/queues"
	"github.com/Yulian302/lfusys-services-sessions/services"
	"github.com/Yulian302/lfusys-services-sessions/store"
)

type Stores struct {
	files    store.FileStore
	sessions store.SessionStore

	logger logger.Logger
}

type Services struct {
	Sessions services.SessionService
	Files    services.FileService
	Uploads  queues.UploadsNotifyReceiver

	Stores *Stores

	UploadHandler pb.UploaderServer
	logger        logger.Logger
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
	sessSvc := services.NewSessionServiceImpl(sessStore, app.Logger)
	fileSvc := services.NewFileServiceImpl(fileStore, cachingSvc, app.Logger)

	queueUrl := fmt.Sprintf("https://sqs.%s.amazonaws.com/%s/%s.fifo", app.Config.AWSConfig.Region, app.Config.AWSConfig.AccountID, app.Config.ServiceConfig.UploadsNotificationsQueueName)
	uploadsReceiver := queues.NewUploadsNotifyReceiveImpl(context.Background(), app.Sqs, fileStore, sessStore, cachingSvc, queueUrl, logger.NullLogger{})
	go uploadsReceiver.Start()

	handler := handlers.NewGrpcHandler(sessSvc, fileSvc, app.Config.ServiceConfig.UploadsURL, app.Logger)

	return &Services{
		Sessions: sessSvc,
		Files:    fileSvc,
		Uploads:  uploadsReceiver,

		Stores: &Stores{
			files:    fileStore,
			sessions: sessStore,
		},

		UploadHandler: handler,
		logger:        app.Logger,
	}
}

func (s *Services) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down services")

	if s.Stores != nil {
		if err := s.Stores.Shutdown(ctx); err != nil {
			s.logger.Error("stores shutdown error", "err", err.Error())
		}
	}

	if s.Uploads != nil {
		if sh, ok := s.Uploads.(Shutdowner); ok {
			if err := sh.Shutdown(ctx); err != nil {
				s.logger.Error("uploads receiver shutdown error", "err", err.Error())
			}
		}
	}

	s.logger.Info("services shutdown complete")
	return nil
}

func (s *Stores) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down stores")

	shutdownIfPossible := func(name string, v any) {
		if sh, ok := v.(Shutdowner); ok {
			if err := sh.Shutdown(ctx); err != nil {
				s.logger.Error(fmt.Sprintf("%s store shutdown error", name), "err", err.Error())
			}
		}
	}

	shutdownIfPossible("files", s.files)
	shutdownIfPossible("sessions", s.sessions)

	s.logger.Info("stores shutdown complete")
	return nil
}
