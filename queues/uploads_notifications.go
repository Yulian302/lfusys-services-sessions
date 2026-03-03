package queues

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
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
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20, // long poll
			VisibilityTimeout:   30,
		})
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		sem := make(chan struct{}, 20)

		for _, msg := range out.Messages {
			sem <- struct{}{}
			r.logger.Info("handling message: " + *msg.MessageId)
			go func(m types.Message) {
				defer func() { <-sem }()
				r.handleMessage(r.ctx, m)
			}(msg)
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
	if msg.Body == nil {
		r.logger.Warn("empty message body", "msgId", msg.MessageId)
		return
	}

	var s3evt models.S3Event
	if err := json.Unmarshal([]byte(*msg.Body), &s3evt); err != nil {
		r.logger.Error("failed to unmarhal s3 event", "error", err)
		return
	}



	for _, record := range s3evt.Records {
		key, err := url.QueryUnescape(record.S3.Object.Key)
		if err != nil {
			r.logger.Error("could not url decode key", "err", err)
			return
		}

		uploadID, chunkIdx, err := parseChunkKey(key)
		if err != nil {
			r.logger.Error("invalid object key", "key", key, "error", err)
			return
		}

		r.logger.Info("chunk complete event received",
			"upload_id", uploadID,
			"chunk_idx", chunkIdx,
		)

		err = r.uploadSvc.MarkChunkComplete(ctx, uploadID, uint32(chunkIdx))
		if err != nil {
			r.logger.Error("failed to mark chunk complete",
				"upload_id", uploadID,
				"chunk_idx", chunkIdx,
				"error", err,
			)
			return
		}
	}

	r.deleteMessage(ctx, msg)
}

func parseChunkKey(key string) (string, int, error) {
	parts := strings.Split(key, "/")
	if len(parts) != 3 {
		return "", 0, fmt.Errorf("invalid key format")
	}

	uploadID := parts[1]
	chunkPart := parts[2]
	if !strings.HasPrefix(chunkPart, "chunk_") {
		return "", 0, fmt.Errorf("invalid chunk format")
	}

	idxStr := strings.TrimPrefix(chunkPart, "chunk_")
	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		return "", 0, err
	}

	return uploadID, idx, nil
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
