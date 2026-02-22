package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	logger "github.com/Yulian302/lfusys-services-commons/logging"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type FileStorage interface {
	FinalizeUpload(ctx context.Context, uploadID string, finalKey string) error
	GenerateDownloadUrl(ctx context.Context, key string, ttl time.Duration) (string, error)
	multipartCopy(ctx context.Context, chunks []types.Object, finalKey string, chunkPrefix string) error
	streamMergeAndPut(ctx context.Context, chunks []types.Object, finalKey string, chunkPrefix string, totalSize int64) error
	listChunks(ctx context.Context, prefix string) ([]types.Object, error)
	fileExists(ctx context.Context, key string) (bool, error)
	abortStaleMultipartUploads(ctx context.Context, key string) error
	deletePrefix(ctx context.Context, prefix string) error
}

type S3FileStorageImpl struct {
	client             *s3.Client
	bucketName         string
	multipartThreshold int64 // Minimum size for multipart upload (default 5MB)

	logger logger.Logger
}

func NewS3FileStorageImpl(client *s3.Client, bucketName string, l logger.Logger) *S3FileStorageImpl {
	return &S3FileStorageImpl{
		client:             client,
		bucketName:         bucketName,
		multipartThreshold: 5 * 1024 * 1024, // 5MB default
		logger:             l,
	}
}

func NewS3FileStorageImplWithThreshold(client *s3.Client, bucketName string, multipartThreshold int64, l logger.Logger) *S3FileStorageImpl {
	return &S3FileStorageImpl{
		client:             client,
		bucketName:         bucketName,
		multipartThreshold: multipartThreshold,
		logger:             l,
	}
}

func (s *S3FileStorageImpl) FinalizeUpload(ctx context.Context, uploadID string, finalKey string) error {
	if uploadID == "" {
		return fmt.Errorf("uploadID cannot be empty")
	}
	if finalKey == "" {
		return fmt.Errorf("finalKey cannot be empty")
	}

	s.logger.Info("starting upload finalization", "upload_id", uploadID, "final_key", finalKey)

	exists, err := s.fileExists(ctx, finalKey)
	if err != nil {
		s.logger.Error("failed to check if final file exists", "upload_id", uploadID, "final_key", finalKey, "error", err)
		return fmt.Errorf("failed to check file existence: %w", err)
	}
	if exists {
		s.logger.Info("finalized file already exists, skipping", "upload_id", uploadID, "final_key", finalKey)
		return nil
	}

	chunkPrefix := fmt.Sprintf("uploads/%s/", uploadID)

	chunks, err := s.listChunks(ctx, chunkPrefix)
	if err != nil {
		s.logger.Error("failed to list chunks", "upload_id", uploadID, "prefix", chunkPrefix, "error", err)
		return fmt.Errorf("failed to list chunks: %w", err)
	}

	if len(chunks) == 0 {
		s.logger.Error("no chunks found for upload", "upload_id", uploadID, "prefix", chunkPrefix)
		return fmt.Errorf("no chunks found for upload %s", uploadID)
	}

	var totalSize int64
	for _, c := range chunks {
		totalSize += *c.Size
	}

	s.logger.Info("upload finalization details", "upload_id", uploadID, "chunk_count", len(chunks), "total_size", totalSize)

	if len(chunks) == 1 {
		s.logger.Info("single chunk upload, using copy operation", "upload_id", uploadID)
		return s.copySingleChunk(ctx, chunks[0], finalKey, chunkPrefix)
	}

	if totalSize < s.multipartThreshold {
		s.logger.Info("small file, using stream merge", "upload_id", uploadID, "total_size", totalSize)
		return s.streamMergeAndPut(ctx, chunks, finalKey, chunkPrefix, totalSize)
	}

	s.logger.Info("large file, using multipart upload", "upload_id", uploadID, "total_size", totalSize)
	return s.multipartCopy(ctx, chunks, finalKey, chunkPrefix)
}

func (s *S3FileStorageImpl) GenerateDownloadUrl(ctx context.Context, key string, ttl time.Duration) (string, error) {
	presigner := s3.NewPresignClient(s.client)

	presigned, err := presigner.PresignGetObject(
		ctx,
		&s3.GetObjectInput{
			Bucket: aws.String(s.bucketName),
			Key:    aws.String(key),
		},
		s3.WithPresignExpires(ttl),
	)
	if err != nil {
		return "", err
	}

	return presigned.URL, nil
}

func (s *S3FileStorageImpl) copySingleChunk(ctx context.Context, chunk types.Object, finalKey string, chunkPrefix string) error {
	src := s.bucketName + "/" + *chunk.Key

	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucketName),
		Key:        aws.String(finalKey),
		CopySource: aws.String(src),
	})
	if err != nil {
		s.logger.Error("failed to copy single chunk", "src", src, "dest", finalKey, "error", err)
		return fmt.Errorf("failed to copy object: %w", err)
	}

	s.logger.Info("successfully copied single chunk", "src", src, "dest", finalKey)

	err = s.deletePrefix(ctx, chunkPrefix)
	if err != nil {
		s.logger.Error("failed to delete chunks after copy", "prefix", chunkPrefix, "error", err)
		// Don't return error here as the main operation succeeded
	}

	return nil
}

func (s *S3FileStorageImpl) multipartCopy(
	ctx context.Context,
	chunks []types.Object,
	finalKey string,
	chunkPrefix string,
) error {
	s.logger.Info("starting multipart upload", "final_key", finalKey, "chunk_count", len(chunks))

	createOut, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(finalKey),
	})
	if err != nil {
		s.logger.Error("failed to create multipart upload", "final_key", finalKey, "error", err)
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}

	uploadID := *createOut.UploadId
	s.logger.Debug("created multipart upload", "upload_id", uploadID)

	defer func() {
		if err != nil {
			s.logger.Warn("aborting multipart upload due to error", "upload_id", uploadID, "final_key", finalKey)
			if abortErr := s.abortMultipartUpload(ctx, finalKey, uploadID); abortErr != nil {
				s.logger.Error("failed to abort multipart upload", "upload_id", uploadID, "error", abortErr)
			}
		}
	}()

	var completedParts []types.CompletedPart

	for i, obj := range chunks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		partNumber := int32(i + 1)
		src := s.bucketName + "/" + *obj.Key

		s.logger.Debug("copying part", "part_number", partNumber, "src", src)

		upOut, err := s.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:     aws.String(s.bucketName),
			Key:        aws.String(finalKey),
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int32(partNumber),
			CopySource: aws.String(src),
		})
		if err != nil {
			s.logger.Error("failed to upload part copy", "part_number", partNumber, "src", src, "error", err)
			return fmt.Errorf("failed to upload part %d: %w", partNumber, err)
		}

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       upOut.CopyPartResult.ETag,
			PartNumber: aws.Int32(partNumber),
		})
	}

	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.bucketName),
		Key:      aws.String(finalKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		s.logger.Error("failed to complete multipart upload", "upload_id", uploadID, "final_key", finalKey, "error", err)
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	s.logger.Info("successfully completed multipart upload", "upload_id", uploadID, "final_key", finalKey, "parts", len(completedParts))

	err = s.deletePrefix(ctx, chunkPrefix)
	if err != nil {
		s.logger.Error("failed to delete chunks after multipart upload", "prefix", chunkPrefix, "error", err)
		// Don't return error as the main operation succeeded
	}

	return nil
}

func (s *S3FileStorageImpl) abortMultipartUpload(ctx context.Context, key string, uploadID string) error {
	_, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.bucketName),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	return err
}

func (s *S3FileStorageImpl) streamMergeAndPut(
	ctx context.Context,
	chunks []types.Object,
	finalKey string,
	chunkPrefix string,
	totalSize int64,
) error {
	s.logger.Info("starting stream merge", "final_key", finalKey, "chunk_count", len(chunks), "total_size", totalSize)

	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		for i, obj := range chunks {
			select {
			case <-ctx.Done():
				pw.CloseWithError(ctx.Err())
				return
			default:
			}

			s.logger.Debug("streaming chunk", "chunk_index", i, "key", *obj.Key)

			out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(s.bucketName),
				Key:    obj.Key,
			})
			if err != nil {
				s.logger.Error("failed to get chunk object", "key", *obj.Key, "error", err)
				pw.CloseWithError(fmt.Errorf("failed to get object %s: %w", *obj.Key, err))
				return
			}

			_, err = io.Copy(pw, out.Body)
			out.Body.Close()
			if err != nil {
				s.logger.Error("failed to copy chunk data", "key", *obj.Key, "error", err)
				pw.CloseWithError(fmt.Errorf("failed to copy chunk %s: %w", *obj.Key, err))
				return
			}
		}
	}()

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucketName),
		Key:           aws.String(finalKey),
		Body:          pr,
		ContentLength: aws.Int64(totalSize),
	})
	if err != nil {
		s.logger.Error("failed to put merged object", "final_key", finalKey, "error", err)
		return fmt.Errorf("failed to put merged object: %w", err)
	}

	s.logger.Info("successfully merged and put object", "final_key", finalKey)

	err = s.deletePrefix(ctx, chunkPrefix)
	if err != nil {
		s.logger.Error("failed to delete chunks after merge", "prefix", chunkPrefix, "error", err)
		// Don't return error as the main operation succeeded
	}

	return nil
}

func (s *S3FileStorageImpl) abortStaleMultipartUploads(
	ctx context.Context,
	key string,
) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	s.logger.Info("aborting stale multipart uploads", "key", key)

	out, err := s.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(s.bucketName),
		Prefix: aws.String(key),
	})
	if err != nil {
		s.logger.Error("failed to list multipart uploads", "key", key, "error", err)
		return fmt.Errorf("failed to list multipart uploads: %w", err)
	}

	if len(out.Uploads) == 0 {
		s.logger.Debug("no stale multipart uploads found", "key", key)
		return nil
	}

	abortedCount := 0
	for _, upload := range out.Uploads {
		s.logger.Debug("aborting multipart upload", "upload_id", *upload.UploadId, "key", *upload.Key)

		_, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(s.bucketName),
			Key:      upload.Key,
			UploadId: upload.UploadId,
		})
		if err != nil {
			s.logger.Error("failed to abort multipart upload", "upload_id", *upload.UploadId, "key", *upload.Key, "error", err)
			// Continue with other uploads
		} else {
			abortedCount++
		}
	}

	s.logger.Info("aborted stale multipart uploads", "key", key, "aborted_count", abortedCount)
	return nil
}

func (s *S3FileStorageImpl) listChunks(ctx context.Context, prefix string) ([]types.Object, error) {
	if prefix == "" {
		return nil, fmt.Errorf("prefix cannot be empty")
	}

	s.logger.Debug("listing chunks", "prefix", prefix)

	out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucketName),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		s.logger.Error("failed to list objects", "prefix", prefix, "error", err)
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	objects := out.Contents
	if len(objects) == 0 {
		s.logger.Debug("no chunks found", "prefix", prefix)
		return objects, nil
	}

	sort.Slice(objects, func(i, j int) bool {
		return extractChunkIndex(*objects[i].Key) < extractChunkIndex(*objects[j].Key)
	})

	s.logger.Debug("listed chunks", "prefix", prefix, "count", len(objects))
	return objects, nil
}

func extractChunkIndex(key string) int {
	// key example: uploads/{uploadId}/chunk_3
	parts := strings.Split(key, "chunk_")
	i, _ := strconv.Atoi(parts[1])
	return i
}

func (s *S3FileStorageImpl) fileExists(ctx context.Context, key string) (bool, error) {
	if key == "" {
		return false, fmt.Errorf("key cannot be empty")
	}

	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(key),
	})

	if err == nil {
		s.logger.Debug("file exists", "key", key)
		return true, nil
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotFound" {
		s.logger.Debug("file does not exist", "key", key)
		return false, nil
	}

	s.logger.Error("failed to check file existence", "key", key, "error", err)
	return false, fmt.Errorf("failed to check file existence: %w", err)
}

func (s *S3FileStorageImpl) deletePrefix(ctx context.Context, prefix string) error {
	if prefix == "" {
		return fmt.Errorf("prefix cannot be empty")
	}

	s.logger.Info("starting deletion of prefix", "prefix", prefix)

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucketName),
		Prefix: aws.String(prefix),
	})

	totalDeleted := 0
	for paginator.HasMorePages() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		page, err := paginator.NextPage(ctx)
		if err != nil {
			s.logger.Error("failed to list objects for deletion", "prefix", prefix, "error", err)
			return fmt.Errorf("failed to list objects for deletion: %w", err)
		}

		if len(page.Contents) == 0 {
			continue
		}

		var objects []types.ObjectIdentifier
		for _, obj := range page.Contents {
			objects = append(objects, types.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		s.logger.Debug("deleting batch of objects", "count", len(objects))

		_, err = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucketName),
			Delete: &types.Delete{
				Objects: objects,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			s.logger.Error("failed to delete objects", "prefix", prefix, "batch_size", len(objects), "error", err)
			return fmt.Errorf("failed to delete objects: %w", err)
		}

		totalDeleted += len(objects)
	}

	s.logger.Info("successfully deleted prefix", "prefix", prefix, "total_deleted", totalDeleted)
	return nil
}
