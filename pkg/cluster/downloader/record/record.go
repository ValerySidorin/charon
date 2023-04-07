package record

const (
	PROCESSING = "PROCESSING"
	COMPLETED  = "COMPLETED"
	SENT       = "SENT"
)

type Record struct {
	Version      int
	DownloaderID string
	DownloadURL  string
	Type         string
	Status       string
}

func New(version int, downloaderID string, url string, typ string) *Record {
	return &Record{
		Version:      version,
		DownloaderID: downloaderID,
		DownloadURL:  url,
		Type:         typ,
		Status:       PROCESSING,
	}
}
