package services

import (
	"context"

	logger "github.com/Yulian302/lfusys-services-commons/logging"
	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/Yulian302/lfusys-services-sessions/store"
)

type SessionService interface {
	CreateUpload(ctx context.Context, uploadSession models.UploadSession) error
	GetUploadStatus(ctx context.Context, uploadID string) (*models.UploadStatusResponse, error)
}

type SessionServiceImpl struct {
	sessionStore store.SessionStore

	logger logger.Logger
}

func NewSessionServiceImpl(sessionStore store.SessionStore, l logger.Logger) *SessionServiceImpl {
	return &SessionServiceImpl{
		sessionStore: sessionStore,
		logger:       l,
	}
}

func (svc *SessionServiceImpl) CreateUpload(ctx context.Context, uploadSession models.UploadSession) error {
	err := svc.sessionStore.CreateSession(ctx, uploadSession)
	if err != nil {
		svc.logger.Error("failed to create upload session",
			"upload_id", uploadSession.UploadId,
			"email", uploadSession.UserEmail,
			"file_name", uploadSession.FileName,
			"file_type", uploadSession.FileType,
			"file_size", uploadSession.FileSize,
			"error", err,
		)
		return err
	}

	svc.logger.Info("upload session created successfully",
		"upload_id", uploadSession.UploadId,
		"email", uploadSession.UserEmail,
		"file_name", uploadSession.FileName,
		"file_type", uploadSession.FileType,
		"file_size", uploadSession.FileSize,
		"total_chunks", uploadSession.TotalChunks,
	)
	return nil
}

func (svc *SessionServiceImpl) GetUploadStatus(ctx context.Context, uploadID string) (*models.UploadStatusResponse, error) {
	status, err := svc.sessionStore.GetStatus(ctx, uploadID)
	if err != nil {
		svc.logger.Error("failed to get upload status",
			"upload_id", uploadID,
			"error", err,
		)
		return nil, err
	}

	svc.logger.Debug("upload status retrieved",
		"upload_id", uploadID,
		"status", status.Status,
		"progress", status.Progress,
	)
	return status, nil
}
