package record

const (
	DRAFT      = "DRAFT"
	PROCESSING = "PROCESSING"
	COMPLETED  = "COMPLETED"
)

type Record struct {
	Version     int
	ProcessorID string
	ObjName     string
	Type        string
	Status      string
}

func New(version int, obj string, typ string) *Record {
	return &Record{
		Version: version,
		ObjName: obj,
		Type:    typ,
		Status:  DRAFT,
	}
}
