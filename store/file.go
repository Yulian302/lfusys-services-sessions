package store

import (
	"context"
	"time"

	apperror "github.com/Yulian302/lfusys-services-commons/errors"
	"github.com/Yulian302/lfusys-services-commons/health"
	"github.com/Yulian302/lfusys-services-commons/retries"
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

	return retries.Retry(
		ctx,
		retries.HealthAttempts,
		retries.HealthBaseDelay,
		func() error {
			_, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
				TableName: aws.String(s.tableName),
			})

			return err
		},
		retries.IsRetriableDbError,
	)
}

func (s *DynamoDbFileStoreImpl) Name() string {
	return "FileStore[files]"
}

func (s *DynamoDbFileStoreImpl) Get(ctx context.Context, uploadId string) (*models.File, error) {
	var file models.File

	err := retries.Retry(
		ctx,
		retries.DefaultAttempts,
		retries.DefaultBaseDelay,
		func() error {
			out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
				TableName: aws.String(s.tableName),
				Key: map[string]types.AttributeValue{
					"upload_id": &types.AttributeValueMemberS{Value: uploadId},
				},
			})
			if err != nil {
				return err
			}

			if out.Item == nil {
				return apperror.ErrSessionNotFound
			}

			return attributevalue.UnmarshalMap(out.Item, &file)
		},
		retries.IsRetriableDbError,
	)

	if err != nil {
		return nil, err
	}

	return &file, nil
}

func (s *DynamoDbFileStoreImpl) Create(ctx context.Context, file models.File) error {
	fileItem, err := attributevalue.MarshalMap(file)
	if err != nil {
		return err
	}

	return retries.Retry(
		ctx,
		retries.DefaultAttempts,
		retries.DefaultBaseDelay,
		func() error {
			_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(s.tableName),
				Item:      fileItem,
			})
			return err
		},
		retries.IsRetriableDbError,
	)
}

func (s *DynamoDbFileStoreImpl) Read(ctx context.Context, ownerEmail string) ([]models.File, error) {
	var files []models.File

	err := retries.Retry(
		ctx,
		retries.DefaultAttempts,
		retries.DefaultBaseDelay,
		func() error {
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
				return err
			}
			return attributevalue.UnmarshalListOfMaps(out.Items, &files)
		},
		retries.IsRetriableDbError,
	)

	if err != nil {
		return nil, err
	}

	return files, nil
}
