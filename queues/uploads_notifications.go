package queues

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	logger "github.com/Yulian302/lfusys-services-commons/logging"
	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/Yulian302/lfusys-services-sessions/services"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type UploadsNotifyReceiver interface {
	pollLoop() error
}

type UploadsNotifyReceiverImpl struct {
	client    *sqs.Client
	uploadSvc services.UploadCompletionService
	queueUrl  string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	logger logger.Logger
}

func NewUploadsNotifyReceiveImpl(
	parent context.Context,
	client *sqs.Client,
	uploadSvc services.UploadCompletionService,
	queueUrl string,
	l logger.Logger,
) *UploadsNotifyReceiverImpl {

	ctx, cancel := context.WithCancel(parent)

	return &UploadsNotifyReceiverImpl{
		client:    client,
		uploadSvc: uploadSvc,
		queueUrl:  queueUrl,
		ctx:       ctx,
		cancel:    cancel,
		logger:    l,
	}
}

func (r *UploadsNotifyReceiverImpl) Start() {
	r.logger.Info("starting uploads notification receiver",
		"queue_url", r.queueUrl,
	)
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

	err := r.uploadSvc.CompleteUpload(ctx, evt.UploadId)
	if err != nil {
		r.logger.Error("failed to complete upload", "upload_id", evt.UploadId, "error", err)
		return // retry
	}

	r.deleteMessage(ctx, msg)
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
