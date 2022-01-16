package peer

import "time"

type Torrent interface {
	StartTorrent(fileID string) error
	GetDownloadingFrom(fileID string) []string

	UploadFile(fileID string, parts [][]byte) error

	GetFileParts(fileID string) [][]byte
	GetFinishTime(fileID string) time.Time
}
