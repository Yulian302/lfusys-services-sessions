package handlers

import (
	"context"
	"fmt"
	"time"

	pb "github.com/Yulian302/lfusys-services-commons/api"
	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/Yulian302/lfusys-services-sessions/services"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GrpcHandler struct {
	sessionService services.SessionService
	fileService    services.FileService
	pb.UnimplementedUploaderServer
	uploadsUrl string
}

func NewGrpcHandler(sessSvc services.SessionService, fileSvc services.FileService, uploadsUrl string) *GrpcHandler {
	return &GrpcHandler{
		sessionService: sessSvc,
		fileService:    fileSvc,
		uploadsUrl:     uploadsUrl,
	}
}

func (h *GrpcHandler) StartUpload(ctx context.Context, req *pb.UploadRequest) (*pb.UploadReply, error) {
	// const chunkSize = 5 * 1024 * 1024           // 5 MB
	const chunkSize = 140 * 1024                // 140 kB (dev)
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
		uploadUrls[i] = fmt.Sprintf("%s/upload/%s/chunk/%d", h.uploadsUrl, uploadId, i+1)
	}
	var uploadSession models.UploadSession = models.UploadSession{
		UploadId:    uploadId,
		UserEmail:   req.UserEmail,
		FileSize:    req.FileSize,
		TotalChunks: uint32(totalChunks),
		// UploadedChunks: []int64{},
		ExpirationTime: time.Now().Add(2 * time.Hour), // session should expire in 2 hours
		CreatedAt:      time.Now(),
		Status:         "pending",
	}

	err := h.sessionService.CreateUpload(ctx, uploadSession)
	if err != nil {
		return nil, err
	}

	return &pb.UploadReply{TotalChunks: uint32(totalChunks), UploadUrls: uploadUrls, UploadId: uploadId}, nil
}

func (h *GrpcHandler) GetUploadStatus(ctx context.Context, upload *pb.UploadID) (*pb.StatusReply, error) {
	out, err := h.sessionService.GetUploadStatus(ctx, upload.UploadId)
	if err != nil {
		return nil, err
	}
	return &pb.StatusReply{
		Status:   out.Status.String(),
		Progress: uint32(out.Progress),
		Message:  out.Message,
	}, nil
}

func (h *GrpcHandler) GetFiles(ctx context.Context, userInfo *pb.UserInfo) (*pb.FilesReply, error) {
	filesResponse, err := h.fileService.GetFiles(ctx, userInfo.Email)
	if err != nil {
		return nil, err
	}

	pbFiles := make([]*pb.File, len(filesResponse.Files))
	for i, f := range filesResponse.Files {
		pbFiles[i] = &pb.File{
			Id:          f.FileId,
			UploadId:    f.UploadId,
			OwnerEmail:  f.OwnerEmail,
			Size:        f.Size,
			TotalChunks: f.TotalChunks,
			Checksum:    f.Checksum,
			CreatedAt:   timestamppb.New(f.CreatedAt),
		}
	}

	return &pb.FilesReply{
		Files: pbFiles,
	}, nil
}
