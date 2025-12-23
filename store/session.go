package store

import (
	"context"

	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type SessionStore interface {
	CreateSession(ctx context.Context, uploadSession models.UploadSession) error
	GetSession(ctx context.Context, uploadID string) (*models.UploadSession, error)
}

type SessionStoreImpl struct {
	client    *dynamodb.Client
	tableName string
}

func NewSessionStoreImpl(client *dynamodb.Client, tableName string) *SessionStoreImpl {
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

func (s *SessionStoreImpl) GetSession(ctx context.Context, uploadID string) (*models.UploadSession, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"upload_id": &types.AttributeValueMemberS{
				Value: uploadID,
			},
		},
	})
	if err != nil || out == nil {
		return nil, err
	}

	var session models.UploadSession

	if err := attributevalue.UnmarshalMap(out.Item, &session); err != nil {
		return nil, err
	}

	return &session, nil
}
