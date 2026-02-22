package test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Yulian302/lfusys-services-commons/caching"
	cerr "github.com/Yulian302/lfusys-services-commons/errors"
	logger "github.com/Yulian302/lfusys-services-commons/logging"
	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/Yulian302/lfusys-services-sessions/queues"
	"github.com/Yulian302/lfusys-services-sessions/services"
	"github.com/Yulian302/lfusys-services-sessions/store"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
)

const awsEndpoint = "http://localhost:4566"

type TestEnv struct {
	Dynamo   *dynamodb.Client
	Sqs      *sqs.Client
	S3       *s3.Client
	QueueURL string
}

func setupTestEnv(t *testing.T) *TestEnv {
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	require.NoError(t, err)

	db := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(awsEndpoint)
	})

	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(awsEndpoint)
	})

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(awsEndpoint)
	})

	createTable := func(name string) {
		_, err := db.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String(name),
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("upload_id"),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("upload_id"),
					KeyType:       types.KeyTypeHash,
				},
			},
			BillingMode: types.BillingModePayPerRequest,
		})

		var exists *types.ResourceInUseException
		if err != nil && !errors.As(err, &exists) {
			require.NoError(t, err)
		}
	}

	createTable("sessions")
	createTable("files")

	q, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("uploads"),
	})
	require.NoError(t, err)

	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("lfusysbucket"),
	})
	require.NoError(t, err)

	return &TestEnv{
		Dynamo:   db,
		Sqs:      sqsClient,
		S3:       s3Client,
		QueueURL: *q.QueueUrl,
	}
}

func TestUploadCompleted_DeletesSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	env := setupTestEnv(t)

	sessionStore := store.NewSessionStoreImpl(env.Dynamo, "sessions")
	fileStore := store.NewDynamoDbFileStoreImpl(env.Dynamo, "files")
	fileStorage := store.NewS3FileStorageImpl(env.S3, "lfusysbucket", logger.NullLogger{})
	cachingSvc := caching.NewNullCachingService()

	uploadCompletionSvc := services.NewUploadCompletionServiceImpl(sessionStore, fileStore, fileStorage, cachingSvc, logger.NullLogger{})

	receiver := queues.NewUploadsNotifyReceiveImpl(
		ctx,
		env.Sqs,
		uploadCompletionSvc,
		env.QueueURL, // MUST be real QueueURL
		logger.NullLogger{},
	)

	go receiver.Start()

	// allow poll loop to start
	time.Sleep(200 * time.Millisecond)

	require.NoError(t, sessionStore.CreateSession(ctx, models.UploadSession{
		UploadId:    "1",
		UserEmail:   "test@example.com",
		FileSize:    123,
		TotalChunks: 1,
		Status:      "completed",
	}))

	body, _ := json.Marshal(models.UploadCompletedEvent{
		UploadId: "1",
	})

	_, err := env.Sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(env.QueueURL), // EXACT same URL
		MessageBody: aws.String(string(body)),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, err := sessionStore.GetSession(ctx, "1")
		if !errors.Is(err, cerr.ErrSessionNotFound) {
			return false
		}

		_, err = fileStore.Get(ctx, "1")
		return err == nil
	}, 5*time.Second, 100*time.Millisecond)
}
