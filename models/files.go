package models

import "time"

type File struct {
	FileId      string    `dynamodbav:"file_id"`      // Unique file identifier
	UploadId    string    `dynamodbav:"upload_id"`    // Corresponding upload id
	OwnerEmail  string    `dynamodbav:"owner_email"`  // File owner email
	Size        uint64    `dynamodbav:"file_size"`    // Size of a file
	TotalChunks uint32    `dynamodbav:"total_chunks"` // Number of 5MB file chunks
	Checksum    string    `dynamodbav:"checksum"`     // File checksum
	CreatedAt   time.Time `dynamodbav:"created_at"`   // Time of creation
}

type FilesResponse struct {
	Files []File
}
