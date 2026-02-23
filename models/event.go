package models

type UploadCompletedEvent struct {
	UploadId string `json:"upload_id"`
	ChunkIdx uint32 `json:"chunk_idx"`
}
