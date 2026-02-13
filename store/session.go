package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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

type SessionStore interface {
	CreateSession(ctx context.Context, uploadSession models.UploadSession) error
	GetSession(ctx context.Context, uploadID string) (*models.UploadSession, error)
	GetStatus(ctx context.Context, uploadID string) (*models.UploadStatusResponse, error)
	Delete(ctx context.Context, uploadID string) error

	health.ReadinessCheck
}

type SessionStoreImpl struct {
	client    *dynamodb.Client
	tableName string

	health.ReadinessCheck
}

func NewSessionStoreImpl(client *dynamodb.Client, tableName string) *SessionStoreImpl {
	return &SessionStoreImpl{
		client:    client,
		tableName: tableName,
	}
}

func (s *SessionStoreImpl) IsReady(ctx context.Context) error {
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

func (s *SessionStoreImpl) Name() string {
	return "UploadsStore[sessions]"
}

func (s *SessionStoreImpl) CreateSession(ctx context.Context, uploadSession models.UploadSession) error {
	uploadSessionItem, err := attributevalue.MarshalMap(uploadSession)
	if err != nil {
		return err
	}

	return retries.Retry(
		ctx,
		retries.DefaultAttempts,
		retries.DefaultBaseDelay,
		func() error {
			_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName:           aws.String(s.tableName),
				Item:                uploadSessionItem,
				ConditionExpression: aws.String("attribute_not_exists(upload_id)"), // must be tested
			})
			return err
		},
		retries.IsRetriableDbError,
	)
}

func (s *SessionStoreImpl) GetSession(ctx context.Context, uploadID string) (*models.UploadSession, error) {
	var session models.UploadSession

	err := retries.Retry(
		ctx,
		retries.DefaultAttempts,
		retries.DefaultBaseDelay,
		func() error {
			out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
				TableName: aws.String(s.tableName),
				Key: map[string]types.AttributeValue{
					"upload_id": &types.AttributeValueMemberS{
						Value: uploadID,
					},
				},
			})
			if err != nil {
				return err
			}

			if out.Item == nil {
				return apperror.ErrSessionNotFound
			}

			return attributevalue.UnmarshalMap(out.Item, &session)
		},
		retries.IsRetriableDbError,
	)

	if err != nil {
		return nil, err
	}

	return &session, nil
}

func (s *SessionStoreImpl) GetStatus(ctx context.Context, uploadID string) (*models.UploadStatusResponse, error) {
	var uploadStatus models.UploadStatusResponse
	var item map[string]types.AttributeValue

	err := retries.Retry(
		ctx,
		retries.DefaultAttempts,
		retries.DefaultBaseDelay,
		func() error {
			out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
				TableName: aws.String(s.tableName),
				Key: map[string]types.AttributeValue{
					"upload_id": &types.AttributeValueMemberS{
						Value: uploadID,
					},
				},
				ProjectionExpression: aws.String("#st, total_chunks, uploaded_chunks"),
				ExpressionAttributeNames: map[string]string{
					"#st": "status",
				},
			})
			if err != nil {
				return err
			}

			if out.Item == nil {
				return apperror.ErrSessionNotFound
			}

			item = out.Item
			return nil
		},
		retries.IsRetriableDbError,
	)

	if err != nil {
		return nil, err
	}

	var status models.UploadStatus
	statusAttr, ok := item["status"].(*types.AttributeValueMemberS)
	if !ok {
		return nil, errors.New("could not parse status")
	}
	statusStr := statusAttr.Value
	status, err = models.ParseUploadStatus(statusStr)
	if err != nil {
		return nil, fmt.Errorf("database contains invalid status: %w", err)
	}

	var totalChunks uint32
	totalChunksAttr, ok := item["total_chunks"].(*types.AttributeValueMemberN)
	if !ok {
		return nil, errors.New("could not parse total_chunks")
	}

	totalChunksUint64, err := strconv.ParseUint(totalChunksAttr.Value, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid total_chunks value: %w", err)
	}
	totalChunks = uint32(totalChunksUint64)

	var nUploadedChunks int
	uploadedChunksAttr, ok := item["uploaded_chunks"].(*types.AttributeValueMemberNS)
	if ok {
		nUploadedChunks = len(uploadedChunksAttr.Value)
	}

	var progress uint8
	if totalChunks > 0 {
		progressFloat := (float64(nUploadedChunks) / float64(totalChunks)) * 100
		if progressFloat > 100 {
			progressFloat = 100
		}
		progress = uint8(progressFloat)
	}

	uploadStatus = models.UploadStatusResponse{
		Status:   status,
		Progress: progress,
		Message:  "",
	}

	return &uploadStatus, nil
}

func (s *SessionStoreImpl) Delete(ctx context.Context, uploadID string) error {
	return retries.Retry(
		ctx,
		retries.DefaultAttempts,
		retries.DefaultBaseDelay,
		func() error {
			_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(s.tableName),
				Key: map[string]types.AttributeValue{
					"upload_id": &types.AttributeValueMemberS{Value: uploadID},
				},
				ConditionExpression: aws.String("attribute_exists(upload_id)"),
			})
			return err
		},
		retries.IsRetriableDbError,
	)
}
