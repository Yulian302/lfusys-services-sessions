package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Yulian302/lfusys-services-commons/caching"
	cerr "github.com/Yulian302/lfusys-services-commons/errors"
	logger "github.com/Yulian302/lfusys-services-commons/logging"
	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/Yulian302/lfusys-services-sessions/store"
	"github.com/google/uuid"
)

type UploadCompletionService interface {
	CompleteUpload(ctx context.Context, uploadID string) error
	GenerateDownloadUrl(ctx context.Context, key string, ttl time.Duration) (string, error)
}

type UploadCompletionServiceImpl struct {
	sessionStore store.SessionStore
	fileStore    store.FileStore
	fileStorage  store.FileStorage
	cachingSvc   caching.CachingService

	logger logger.Logger
}

func NewUploadCompletionServiceImpl(
	sessionStore store.SessionStore,
	fileStore store.FileStore,
	fileStorage store.FileStorage,
	cachingSvc caching.CachingService,
	l logger.Logger,
) *UploadCompletionServiceImpl {
	return &UploadCompletionServiceImpl{
		sessionStore: sessionStore,
		fileStore:    fileStore,
		fileStorage:  fileStorage,
		cachingSvc:   cachingSvc,
		logger:       l,
	}
}

func (svc *UploadCompletionServiceImpl) CompleteUpload(ctx context.Context, uploadID string) error {
	session, err := svc.sessionStore.GetSession(ctx, uploadID)
	if errors.Is(err, cerr.ErrSessionNotFound) {
		svc.logger.Info("upload session not found, possibly already processed", "upload_id", uploadID)
		return nil // already processed
	}
	if err != nil {
		svc.logger.Error("failed to get upload session", "upload_id", uploadID, "error", err)
		return err
	}

	file, err := svc.buildFileFromSession(*session)
	if err != nil {
		svc.logger.Error("failed to build file from session", "upload_id", uploadID, "error", err)
		return err
	}

	svc.logger.Info("upload finalization started", "upload_id", uploadID)
	fileKey := fmt.Sprintf("files/%s", file.FileId)
	err = svc.fileStorage.FinalizeUpload(ctx, uploadID, fileKey)
	if err != nil {
		svc.logger.Error("upload finalization failed", "upload_id", uploadID, "error", err)
		return err
	}

	if err := svc.fileStore.Create(ctx, file); err != nil {
		svc.logger.Error("failed to create file record", "upload_id", uploadID, "error", err)
		return err
	}

	// delete upload session
	err = svc.sessionStore.Delete(ctx, uploadID)
	if err != nil {
		svc.logger.Error("upload session deletion failed", "upload_id", uploadID, "error", err)
		// not returning error here as file is already created
	}

	// invalidate cache
	filesKey := fmt.Sprintf("user:files:%s", file.OwnerEmail)
	if err = svc.cachingSvc.Delete(ctx, filesKey); err != nil {
		svc.logger.Error("cached files invalidation failed", "upload_id", uploadID, "error", err)
		// not critical
	}

	svc.logger.Info("upload completed successfully", "upload_id", uploadID, "file_id", file.FileId)
	return nil
}

func (svc *UploadCompletionServiceImpl) GenerateDownloadUrl(ctx context.Context, key string, ttl time.Duration) (string, error) {
	return svc.fileStorage.GenerateDownloadUrl(ctx, key, ttl)
}

func (svc *UploadCompletionServiceImpl) buildFileFromSession(session models.UploadSession) (models.File, error) {
	if session.UploadId == "" {
		return models.File{}, errors.New("missing upload_id")
	}

	if session.FileSize <= 0 {
		return models.File{}, errors.New("invalid file size")
	}

	if session.Status != "completed" {
		return models.File{}, errors.New("upload not completed")
	}

	now := time.Now().UTC()

	file := models.File{
		FileId:      uuid.NewString(),
		UploadId:    session.UploadId,
		OwnerEmail:  session.UserEmail,
		Name:        session.FileName,
		Type:        session.FileType,
		Size:        session.FileSize,
		TotalChunks: session.TotalChunks,
		CreatedAt:   now,
	}

	return file, nil
}
