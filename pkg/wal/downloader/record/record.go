package record

const (
	PROCESSING = "PROCESSING"
	COMPLETED  = "COMPLETED"
)

type Record struct {
	Version      int
	DownloaderID string
	DownloadURL  string
	Status       string
}

func New(version int, downloaderID string, url string, status string) *Record {
	return &Record{
		Version:      version,
		DownloaderID: downloaderID,
		DownloadURL:  url,
		Status:       status,
	}
}
