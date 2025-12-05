package main

import "time"

type UploadSession struct {
	UploadId       string
	UserId         string
	FileSize       int
	TotalChunks    uint32
	UploadedChunks []uint32 // DynamoDB set
	ExpirationTime time.Time
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
