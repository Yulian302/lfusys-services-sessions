package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Yulian302/lfusys-services-commons/caching"
	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/Yulian302/lfusys-services-sessions/store"
)

type FileService interface {
	GetFiles(ctx context.Context, email string) (*models.FilesResponse, error)
	Delete(ctx context.Context, fileId string) error
}

type FileServiceImpl struct {
	fileStore  store.FileStore
	cachingSvc caching.CachingService
}

func NewFileServiceImpl(store store.FileStore, cachingSvc caching.CachingService) *FileServiceImpl {
	return &FileServiceImpl{
		fileStore:  store,
		cachingSvc: cachingSvc,
	}
}

func (svc *FileServiceImpl) GetFiles(ctx context.Context, email string) (*models.FilesResponse, error) {
	filesKey := fmt.Sprintf("user:files:%s", email)
	cached, err := svc.cachingSvc.Get(ctx, filesKey)
	if err == nil && cached != "" {
		var cachedFiles []models.File
		if err = json.Unmarshal([]byte(cached), &cachedFiles); err == nil {
			return &models.FilesResponse{
				Files: cachedFiles,
			}, nil
		}
		fmt.Println("could not unmarshal cached files data")
	}

	files, err := svc.fileStore.Read(ctx, email)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(files)
	if err == nil {
		if err = svc.cachingSvc.Set(ctx, filesKey, string(b), 12*time.Hour); err != nil {
			log.Println("could not save files data in cache:", err)
		}
	} else {
		log.Println("could not save files data in cache:", err)
	}

	return &models.FilesResponse{
		Files: files,
	}, nil
}

func (svc *FileServiceImpl) Delete(ctx context.Context, fileId string) error {
	file, err := svc.fileStore.GetById(ctx, fileId)
	if err != nil {
		return err
	}
	err = svc.fileStore.Delete(ctx, fileId)
	if err != nil {
		return err
	}

	filesKey := fmt.Sprintf("user:files:%s", file.OwnerEmail)
	svc.cachingSvc.Delete(ctx, filesKey)
	return nil
}
