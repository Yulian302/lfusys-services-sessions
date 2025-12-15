package main

import (
	"context"
)

type SessionService interface {
	CreateSession(context.Context) (*UploadSession, error)
}

type Store interface {
	Create(context.Context) error
	// Update(context.Context) error
	// Delete(context.Context) error
}
