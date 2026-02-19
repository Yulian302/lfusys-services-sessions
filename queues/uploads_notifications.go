package queues

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Yulian302/lfusys-services-commons/caching"
	cerr "github.com/Yulian302/lfusys-services-commons/errors"
	logger "github.com/Yulian302/lfusys-services-commons/logging"
	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/Yulian302/lfusys-services-sessions/store"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

type UploadsNotifyReceiver interface {
	pollLoop() error
}

type UploadsNotifyReceiverImpl struct {
	client       *sqs.Client
	fileStore    store.FileStore
	sessionStore store.SessionStore
	cachingSvc   caching.CachingService
	queueUrl     string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	logger logger.Logger
}

func NewUploadsNotifyReceiveImpl(
	parent context.Context,
	client *sqs.Client,
	fileStore store.FileStore,
	sessionStore store.SessionStore,
	cachingSvc caching.CachingService,
	queueUrl string,
	l logger.Logger,
) *UploadsNotifyReceiverImpl {

	ctx, cancel := context.WithCancel(parent)

	return &UploadsNotifyReceiverImpl{
		client:       client,
		fileStore:    fileStore,
		sessionStore: sessionStore,
		cachingSvc:   cachingSvc,
		queueUrl:     queueUrl,
		ctx:          ctx,
		cancel:       cancel,
		logger:       l,
	}
}

func (r *UploadsNotifyReceiverImpl) Start() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		_ = r.pollLoop()
	}()
}

func (r *UploadsNotifyReceiverImpl) pollLoop() error {
	for {
		select {
		case <-r.ctx.Done():
			r.logger.Debug("pollLoop timeout, context closed")
			return r.ctx.Err()
		default:
		}

		out, err := r.client.ReceiveMessage(r.ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(r.queueUrl),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     20, // long poll
			VisibilityTimeout:   30,
		})
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		for _, msg := range out.Messages {
			r.logger.Info("handling message: " + *msg.MessageId)
			r.handleMessage(r.ctx, msg)
		}
	}
}

func (r *UploadsNotifyReceiverImpl) deleteMessage(ctx context.Context, msg types.Message) error {
	_, err := r.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(r.queueUrl),
		ReceiptHandle: msg.ReceiptHandle,
	})
	r.logger.Info(fmt.Sprintf("message %s deleted successfully", *msg.MessageId))

	return err
}

func (r *UploadsNotifyReceiverImpl) handleMessage(ctx context.Context, msg types.Message) {

	var evt models.UploadCompletedEvent
	if msg.Body == nil {
		r.logger.Info("empty message body")
		r.deleteMessage(ctx, msg)
		return
	}

	if err := json.Unmarshal([]byte(*msg.Body), &evt); err != nil {
		// poison message → delete or DLQ
		r.logger.Info("wrong message structure")
		r.deleteMessage(ctx, msg)
		return
	}

	session, err := r.sessionStore.GetSession(ctx, evt.UploadId)
	if errors.Is(err, cerr.ErrSessionNotFound) {
		// already processed previously
		r.logger.Info("message already processed")
		r.deleteMessage(ctx, msg)
		return
	}
	if err != nil {
		return // retry
	}

	file, err := buildFileFromSession(*session)
	if err != nil {
		r.deleteMessage(ctx, msg)
		return
	}

	if err := r.fileStore.Create(ctx, file); err != nil {
		return // retry
	}

	// delete upload session
	err = r.sessionStore.Delete(ctx, evt.UploadId)
	if err != nil {
		r.logger.Error("upload session deletion failed", "err", err.Error())
	}

	// invalidate cache
	filesKey := fmt.Sprintf("user:files:%s", file.OwnerEmail)
	if err = r.cachingSvc.Delete(ctx, filesKey); err != nil {
		r.logger.Error("cached files invalidation failed", "err", err.Error())
	}

	r.deleteMessage(ctx, msg)
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

func (r *UploadsNotifyReceiverImpl) Shutdown(ctx context.Context) error {
	r.cancel()

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
