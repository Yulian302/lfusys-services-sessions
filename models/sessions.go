package models

import "time"

// UploadSession represents a multipart upload session
// Status values:
type UploadSession struct {
	UploadId    string `dynamodbav:"upload_id"`    // Unique identifier for upload session
	UserEmail   string `dynamodbav:"user_email"`   // Email(id) of user who owns this upload
	FileSize    uint64 `dynamodbav:"file_size"`    // Total file size in bytes (max 10GB)
	TotalChunks uint32 `dynamodbav:"total_chunks"` // Number of 5MB chunks required
	// UploadedChunks []int64   `dynamodbav:"uploaded_chunks,numberset"` // Bitmask of uploaded chunks (in bytes)
	ExpirationTime time.Time `dynamodbav:"expiration_time"` // Session expires after 24 hours
	CreatedAt      time.Time `dynamodbav:"created_at"`      // Session creation timestamp
	Status         string    `dynamodbav:"status"`          // Current upload status
}
