package store

import (
	"context"

	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type UploadFileStore interface {
	Create(ctx context.Context, file models.File) error
}

type DynamoDbFileStoreImpl struct {
	client    *dynamodb.Client
	tableName string
}

func NewDynamoDbFileStoreImpl(client *dynamodb.Client, tableName string) *DynamoDbFileStoreImpl {
	return &DynamoDbFileStoreImpl{
		client:    client,
		tableName: tableName,
	}
}

func (s *DynamoDbFileStoreImpl) Create(ctx context.Context, file models.File) error {

	fileItem, err := attributevalue.MarshalMap(file)
	if err != nil {
		return err
	}

	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item:      fileItem,
	})
	return err
}
