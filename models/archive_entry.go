package models

import "time"

type ArchiveEntry struct {
	Id           string      `json:"id"`
	Bucket       string      `json:"bucket"`
	Path         string      `json:"path"`
	Region       *string     `json:"region"`
	Extension    *string     `json:"extension"`
	Size         int64       `json:"size"`
	LastModified time.Time   `json:"lastModified"`
	ETag         string      `json:"etag"`
	MimeType     interface{} `json:"mimeType"`
	Proxied      bool        `json:"proxied"`
	StorageClass string      `json:"storageClass"`
	BeenDeleted  bool        `json:"beenDeleted"`
}
