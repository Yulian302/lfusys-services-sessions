package services

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Yulian302/lfusys-services-commons/caching"
	logger "github.com/Yulian302/lfusys-services-commons/logging"
	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/Yulian302/lfusys-services-sessions/store"
)

type FileService interface {
	GetFiles(ctx context.Context, email string) (*models.FilesResponse, error)
	GetFileById(ctx context.Context, email, fileId string) (*models.File, error)
	Delete(ctx context.Context, fileId, ownerEmail string) error
}

type FileServiceImpl struct {
	fileStore  store.FileStore
	cachingSvc caching.CachingService

	logger logger.Logger
}

func NewFileServiceImpl(store store.FileStore, cachingSvc caching.CachingService, l logger.Logger) *FileServiceImpl {
	return &FileServiceImpl{
		fileStore:  store,
		cachingSvc: cachingSvc,
		logger:     l,
	}
}

func (svc *FileServiceImpl) GetFiles(ctx context.Context, email string) (*models.FilesResponse, error) {
	filesKey := fmt.Sprintf("user:files:%s", email)
	cached, err := svc.cachingSvc.Get(ctx, filesKey)
	if err == nil && cached != "" {
		var cachedFiles []models.File
		if err = json.Unmarshal([]byte(cached), &cachedFiles); err == nil {
			svc.logger.Debug("files retrieved from cache",
				"email", email,
				"file_count", len(cachedFiles),
			)
			return &models.FilesResponse{
				Files: cachedFiles,
			}, nil
		}
		svc.logger.Debug("could not unmarshal cached files data",
			"email", email,
		)
	}

	files, err := svc.fileStore.Read(ctx, email)
	if err != nil {
		svc.logger.Error("failed to read files from store",
			"email", email,
			"error", err,
		)
		return nil, err
	}

	b, err := json.Marshal(files)
	if err == nil {
		if err = svc.cachingSvc.Set(ctx, filesKey, string(b), 12*time.Hour); err != nil {
			svc.logger.Debug("could not save files data in cache",
				"email", email,
				"error", err,
			)
		}
	} else {
		svc.logger.Debug("could not save files data in cache",
			"email", email,
			"error", err,
		)
	}

	svc.logger.Info("files retrieved successfully",
		"email", email,
		"file_count", len(files),
	)

	return &models.FilesResponse{
		Files: files,
	}, nil
}

func (svc *FileServiceImpl) GetFileById(ctx context.Context, email, fileId string) (*models.File, error) {
	file, err := svc.fileStore.GetById(ctx, fileId)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (svc *FileServiceImpl) Delete(ctx context.Context, fileId, ownerEmail string) error {
	file, err := svc.fileStore.GetById(ctx, fileId)
	if err != nil {
		svc.logger.Error("failed to get file for deletion",
			"file_id", fileId,
			"error", err,
		)
		return err
	}
	err = svc.fileStore.Delete(ctx, fileId, ownerEmail)
	if err != nil {
		svc.logger.Error("failed to delete file",
			"file_id", fileId,
			"email", file.OwnerEmail,
			"error", err,
		)
		return err
	}

	filesKey := fmt.Sprintf("user:files:%s", file.OwnerEmail)
	svc.cachingSvc.Delete(ctx, filesKey)

	svc.logger.Info("file deleted successfully",
		"file_id", fileId,
		"email", file.OwnerEmail,
	)
	return nil
}
