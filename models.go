package main

import "time"

// UploadSession represents a multipart upload session
// Status values:
type UploadSession struct {
	UploadId       string    `dynamodbav:"upload_id"`       // Unique identifier for upload session
	UserEmail      string    `dynamodbav:"user_email"`      // Email(id) of user who owns this upload
	FileSize       uint64    `dynamodbav:"file_size"`       // Total file size in bytes (max 10GB)
	TotalChunks    uint32    `dynamodbav:"total_chunks"`    // Number of 5MB chunks required
	UploadedChunks []string  `dynamodbav:"uploaded_chunks"` // List of successfully uploaded chunk ETags
	ExpirationTime time.Time `dynamodbav:"expiration_time"` // Session expires after 24 hours
	CreatedAt      time.Time `dynamodbav:"created_at"`      // Session creation timestamp
	Status         string    `dynamodbav:"status"`          // Current upload status
}

type File struct {
	FileId      string
	UploadId    string
	OwnerUserId string
	Size        int
	CreatedAt   time.Time
	Checksum    string
	Chunks      []Chunk
}

type Chunk struct {
	Number uint32
	Key    string
}
