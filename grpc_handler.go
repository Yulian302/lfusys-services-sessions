package main

import (
	"context"
	"fmt"
	"time"

	common "github.com/Yulian302/lfusys-services-commons"
	pb "github.com/Yulian302/lfusys-services-commons/api"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type grpcHandler struct {
	pb.UnimplementedUploaderServer
	tracing bool
	config  *common.Config
	store   *store
}

func NewGrpcHandler(grpcServer *grpc.Server, tracing bool, store *store, config *common.Config) {
	handler := &grpcHandler{
		tracing: tracing,
		config:  config,
		store:   store,
	}
	pb.RegisterUploaderServer(grpcServer, handler)

}

func (h *grpcHandler) StartUpload(ctx context.Context, req *pb.UploadRequest) (*pb.UploadReply, error) {
	const chunkSize = 5 * 1024 * 1024           // 5 MB
	const maxFileSize = 10 * 1024 * 1024 * 1024 // 10 GB
	if req.FileSize > maxFileSize {
		return nil, fmt.Errorf("file size exceeds 10GB limit")
	}

	totalChunks := (req.FileSize + chunkSize - 1) / chunkSize
	uuidGen := uuid.New()
	uploadId := uuidGen.String()
	uploadUrls := make([]string, totalChunks)
	for i := uint64(0); i < totalChunks; i++ {
		// load balancer url in PROD, single worker url in DEV
		uploadUrls[i] = fmt.Sprintf("%s/upload/%s/chunk/%d", h.config.ServiceConfig.UploadsURL, uploadId, i+1)
	}
	var uploadSession UploadSession = UploadSession{
		UploadId:    uploadId,
		UserEmail:   req.UserEmail,
		FileSize:    req.FileSize,
		TotalChunks: uint32(totalChunks),
		// UploadedChunks: []int64{},
		ExpirationTime: time.Now().Add(2 * time.Hour), // session should expire in 2 hours
		CreatedAt:      time.Now(),
		Status:         "pending",
	}

	uploadSessionItem, err := attributevalue.MarshalMap(uploadSession)
	if err != nil {
		return nil, err
	}

	_, err = h.store.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &h.store.TableName,
		Item:      uploadSessionItem,
	})
	if err != nil {
		return nil, err
	}

	return &pb.UploadReply{TotalChunks: uint32(totalChunks), UploadUrls: uploadUrls, UploadId: uploadId}, nil
}
