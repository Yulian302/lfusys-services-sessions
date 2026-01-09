package queues

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/Yulian302/lfusys-services-sessions/store"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

type UploadsNotifyReceiver interface {
	Poll(ctx context.Context) error
}

type UploadsNotifyReceiverImpl struct {
	client       *sqs.Client
	fileStore    store.FileStore
	sessionStore store.SessionStore
	queueUrl     string
}

func NewUploadsNotifyReceiveImpl(client *sqs.Client, fileStore store.FileStore, sessionStore store.SessionStore, queueUrl string) *UploadsNotifyReceiverImpl {
	return &UploadsNotifyReceiverImpl{
		client:       client,
		fileStore:    fileStore,
		sessionStore: sessionStore,
		queueUrl:     queueUrl,
	}
}

func (rc *UploadsNotifyReceiverImpl) Poll(ctx context.Context) error {

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		out, err := rc.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(rc.queueUrl),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     20,
			VisibilityTimeout:   30,
		})
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		for _, msg := range out.Messages {
			rc.handleMessage(ctx, msg)
		}
	}
}

func (rc *UploadsNotifyReceiverImpl) deleteMessage(ctx context.Context, msg types.Message) error {
	_, err := rc.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(rc.queueUrl),
		ReceiptHandle: msg.ReceiptHandle,
	})
	return err
}

func (rc *UploadsNotifyReceiverImpl) handleMessage(ctx context.Context, msg types.Message) {

	var evt models.UploadCompletedEvent
	if msg.Body == nil {
		rc.deleteMessage(ctx, msg)
		return
	}

	if err := json.Unmarshal([]byte(*msg.Body), &evt); err != nil {
		// poison message → delete or DLQ
		rc.deleteMessage(ctx, msg)
		return
	}

	session, err := rc.sessionStore.GetSession(ctx, evt.UploadId)
	if err != nil {
		return // retry
	}

	file, err := buildFileFromSession(*session)
	if err != nil {
		rc.deleteMessage(ctx, msg)
		return
	}

	if err := rc.fileStore.Create(ctx, file); err != nil {
		return // retry
	}

	rc.deleteMessage(ctx, msg)
}

func buildFileFromSession(session models.UploadSession) (models.File, error) {
	if session.UploadId == "" {
		return models.File{}, errors.New("missing upload_id")
	}

	if session.FileSize <= 0 {
		return models.File{}, errors.New("invalid file size")
	}

	if session.Status != "completed" {
		return models.File{}, errors.New("upload not completed")
	}

	now := time.Now().UTC()

	file := models.File{
		FileId:      uuid.NewString(),
		UploadId:    session.UploadId,
		OwnerEmail:  session.UserEmail,
		Size:        session.FileSize,
		TotalChunks: session.TotalChunks,
		CreatedAt:   now,
	}

	return file, nil

}
