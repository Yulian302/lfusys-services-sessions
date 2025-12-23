package store

import (
	"context"

	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type SessionStore interface {
	CreateSession(ctx context.Context, uploadSession models.UploadSession) error
}

type SessionStoreImpl struct {
	client    *dynamodb.Client
	tableName string
}

func NewUploadsStoreImpl(client *dynamodb.Client, tableName string) *SessionStoreImpl {
	return &SessionStoreImpl{
		client:    client,
		tableName: tableName,
	}
}

func (s *SessionStoreImpl) CreateSession(ctx context.Context, uploadSession models.UploadSession) error {
	uploadSessionItem, err := attributevalue.MarshalMap(uploadSession)
	if err != nil {
		return err
	}

	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item:      uploadSessionItem,
	})
	return err
}
