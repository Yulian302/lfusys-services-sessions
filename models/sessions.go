package models

import (
	"fmt"
	"time"
)

type UploadStatus int

const (
	Pending = iota
	InProgress
	Completed
	Failed
	Expired
	Cancelled
)

var uploadStatusName = map[UploadStatus]string{
	Pending:    "pending",
	InProgress: "in_progress",
	Completed:  "completed",
	Failed:     "failed",
	Expired:    "expired",
	Cancelled:  "cancelled",
}

func (us UploadStatus) String() string {
	return uploadStatusName[us]
}

func ParseUploadStatus(s string) (UploadStatus, error) {
	switch s {
	case uploadStatusName[Pending]:
		return Pending, nil
	case uploadStatusName[InProgress]:
		return InProgress, nil
	case uploadStatusName[Completed]:
		return Completed, nil
	case uploadStatusName[Failed]:
		return Failed, nil
	case uploadStatusName[Expired]:
		return Expired, nil
	case uploadStatusName[Cancelled]:
		return Cancelled, nil
	default:
		return -1, fmt.Errorf("invalid upload status: %s", s)
	}
}

// UploadSession represents a multipart upload session
// Status values:
type UploadSession struct {
	UploadId    string `dynamodbav:"upload_id"`    // Unique identifier for upload session
	UserEmail   string `dynamodbav:"user_email"`   // Email(id) of user who owns this upload
	FileName    string `dynamodbav:"file_name"`    // File name
	FileType    string `dynamodbav:"file_type"`    // File type
	FileSize    uint64 `dynamodbav:"file_size"`    // Total file size in bytes (max 10GB)
	TotalChunks uint32 `dynamodbav:"total_chunks"` // Number of 5MB chunks required
	// UploadedChunks []int64   `dynamodbav:"uploaded_chunks,numberset"` // Bitmask of uploaded chunks (in bytes), created on a fly
	ExpiresAt int64     `dynamodbav:"expires_at"` // Session expires in 3 hours
	CreatedAt time.Time `dynamodbav:"created_at"` // Session creation timestamp
	Status    string    `dynamodbav:"status"`     // Current upload status
}

type UploadStatusResponse struct {
	Status   UploadStatus `json:"status"`
	Progress uint8        `json:"progress"`
	Message  string       `json:"message"`
}
