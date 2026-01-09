package services

import (
	"context"

	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/Yulian302/lfusys-services-sessions/store"
)

type FileService interface {
	GetFiles(ctx context.Context, email string) (*models.FilesResponse, error)
}

type FileServiceImpl struct {
	fileStore store.FileStore
}

func NewFileServiceImpl(store store.FileStore) *FileServiceImpl {
	return &FileServiceImpl{
		fileStore: store,
	}
}

func (svc *FileServiceImpl) GetFiles(ctx context.Context, email string) (*models.FilesResponse, error) {
	files, err := svc.fileStore.Read(ctx, email)
	if err != nil {
		return nil, err
	}

	return &models.FilesResponse{
		Files: files,
	}, nil
}
