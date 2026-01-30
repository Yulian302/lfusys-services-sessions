package store

import (
	"context"
	"time"

	"github.com/Yulian302/lfusys-services-commons/health"
	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type FileStore interface {
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
