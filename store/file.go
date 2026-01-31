package store

import (
	"context"
	"time"

	"github.com/Yulian302/lfusys-services-commons/errors"
	"github.com/Yulian302/lfusys-services-commons/health"
	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type FileStore interface {
	Get(ctx context.Context, uploadId string) (*models.File, error)
	Create(ctx context.Context, file models.File) error
	Read(ctx context.Context, ownerEmail string) ([]models.File, error)

	health.ReadinessCheck
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

func (s *DynamoDbFileStoreImpl) IsReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	_, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(s.tableName),
	})

	return err
}

func (s *DynamoDbFileStoreImpl) Name() string {
	return "FileStore[files]"
}

func (s *DynamoDbFileStoreImpl) Get(ctx context.Context, uploadId string) (*models.File, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"upload_id": &types.AttributeValueMemberS{Value: uploadId},
		},
	})
	if err != nil {
		return nil, err
	}

	if out.Item == nil {
		return nil, errors.ErrSessionNotFound
	}

	var file models.File
	if err = attributevalue.UnmarshalMap(out.Item, &file); err != nil {
		return nil, err
	}

	return &file, nil
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

func (s *DynamoDbFileStoreImpl) Read(ctx context.Context, ownerEmail string) ([]models.File, error) {
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.tableName),
		IndexName:              aws.String("owner_email-index"),
		KeyConditionExpression: aws.String("owner_email = :e"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":e": &types.AttributeValueMemberS{
				Value: ownerEmail,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	var files []models.File
	if err = attributevalue.UnmarshalListOfMaps(out.Items, &files); err != nil {
		return nil, err
	}

	return files, nil
}
