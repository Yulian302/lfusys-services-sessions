package handlers

import (
	"context"
	"fmt"
	"time"

	pb "github.com/Yulian302/lfusys-services-commons/api/uploader/v1"
	logger "github.com/Yulian302/lfusys-services-commons/logging"
	"github.com/Yulian302/lfusys-services-sessions/models"
	"github.com/Yulian302/lfusys-services-sessions/services"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GrpcHandler struct {
	sessionService services.SessionService
	fileService    services.FileService
	pb.UnimplementedUploaderServer
	uploadsUrl string

	logger logger.Logger
}

func NewGrpcHandler(sessSvc services.SessionService, fileSvc services.FileService, uploadsUrl string, l logger.Logger) *GrpcHandler {
	return &GrpcHandler{
		sessionService: sessSvc,
		fileService:    fileSvc,
		uploadsUrl:     uploadsUrl,
		logger:         l,
	}
}

func (h *GrpcHandler) StartUpload(ctx context.Context, req *pb.UploadRequest) (*pb.UploadReply, error) {
	const maxFileSize = 10 * 1024 * 1024 * 1024 // 10 GB
	if req.FileSize > maxFileSize {
		h.logger.Warn("start upload validation failed",
			"email", req.UserEmail,
			"file_size", req.FileSize,
			"reason", "file_size_exceeded",
		)
		return nil, fmt.Errorf("file size exceeds 10GB limit")
	}

	totalChunks := (req.FileSize + req.ChunkSize - 1) / req.ChunkSize
	uuidGen := uuid.New()
	uploadId := uuidGen.String()

	var uploadSession models.UploadSession = models.UploadSession{
		UploadId:    uploadId,
		UserEmail:   req.UserEmail,
		FileSize:    req.FileSize,
		TotalChunks: uint32(totalChunks),
		// UploadedChunks: []int64{},
		ExpiresAt: time.Now().Add(3 * time.Hour).Unix(),
		CreatedAt: time.Now(),
		Status:    "pending",
	}

	err := h.sessionService.CreateUpload(ctx, uploadSession)
	if err != nil {
		h.logger.Error("start upload failed",
			"email", req.UserEmail,
			"upload_id", uploadId,
			"error", err,
		)
		return nil, err
	}

	h.logger.Info("upload started",
		"email", req.UserEmail,
		"upload_id", uploadId,
		"total_chunks", totalChunks,
		"file_size", req.FileSize,
	)

	return &pb.UploadReply{TotalChunks: uint32(totalChunks), UploadId: uploadId}, nil
}

func (h *GrpcHandler) GetUploadStatus(ctx context.Context, upload *pb.UploadID) (*pb.StatusReply, error) {
	out, err := h.sessionService.GetUploadStatus(ctx, upload.UploadId)
	if err != nil {
		h.logger.Error("get upload status failed",
			"upload_id", upload.UploadId,
			"error", err,
		)
		return nil, err
	}

	h.logger.Debug("upload status retrieved",
		"upload_id", upload.UploadId,
		"status", out.Status,
		"progress", out.Progress,
	)

	return &pb.StatusReply{
		Status:   out.Status.String(),
		Progress: uint32(out.Progress),
		Message:  out.Message,
	}, nil
}

func (h *GrpcHandler) GetFiles(ctx context.Context, userInfo *pb.UserInfo) (*pb.FilesReply, error) {
	filesResponse, err := h.fileService.GetFiles(ctx, userInfo.Email)
	if err != nil {
		h.logger.Error("get files failed",
			"email", userInfo.Email,
			"error", err,
		)
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

	h.logger.Debug("files retrieved",
		"email", userInfo.Email,
		"file_count", len(filesResponse.Files),
	)

	return &pb.FilesReply{
		Files: pbFiles,
	}, nil
}

func (h *GrpcHandler) DeleteFile(ctx context.Context, req *pb.FileDeleteRequest) (*emptypb.Empty, error) {
	err := h.fileService.Delete(ctx, req.FileId, req.OwnerEmail)
	if err != nil {
		h.logger.Error("delete file failed",
			"file_id", req.FileId,
			"error", err,
		)
		return nil, err
	}

	h.logger.Info("file deleted",
		"file_id", req.FileId,
	)

	return &emptypb.Empty{}, nil
}
