package services

import (
	"context"

	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/Yulian302/lfusys-services-sessions/store"
)

type SessionService interface {
	CreateUpload(ctx context.Context, uploadSession models.UploadSession) error
	GetUploadStatus(ctx context.Context, uploadID string) (*models.UploadStatusResponse, error)
}

type SessionServiceImpl struct {
	sessionStore store.SessionStore
}

func NewSessionServiceImpl(sessionStore store.SessionStore) *SessionServiceImpl {
	return &SessionServiceImpl{
		sessionStore: sessionStore,
	}
}

func (svc *SessionServiceImpl) CreateUpload(ctx context.Context, uploadSession models.UploadSession) error {
	err := svc.sessionStore.CreateSession(ctx, uploadSession)
	return err
}

func (svc *SessionServiceImpl) GetUploadStatus(ctx context.Context, uploadID string) (*models.UploadStatusResponse, error) {
	status, err := svc.sessionStore.GetStatus(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	return status, nil
}
