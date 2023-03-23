package diffstore

import (
	"context"
	"fmt"
	"io"

	"github.com/ValerySidorin/charon/pkg/diffstore/minio"
)

const (
	Bucket = "charondiffstore"
)

type Config struct {
	Store string       `yaml:"store"`
	Minio minio.Config `yaml:"minio"`
}

type Writer interface {
	Store(ctx context.Context, version int, typ string, r io.Reader) error
}

type Reader interface {
	Retrieve(ctx context.Context, objName string) (io.ReadCloser, error)
	RetrieveObjNamesByVersion(ctx context.Context, version int, typ string) ([]string, error)
	ListVersionsWithTypes(ctx context.Context) ([]string, error)
}

func NewReader(cfg Config, bucket string) (Reader, error) {
	switch cfg.Store {
	case "minio":
		return minio.NewReader(cfg.Minio, bucket)
	}

	return nil, fmt.Errorf("invalid store for reader")
}

func NewWriter(cfg Config, bucket string) (Writer, error) {
	switch cfg.Store {
	case "minio":
		return minio.NewWriter(cfg.Minio, bucket)
	}

	return nil, fmt.Errorf("invalid store for writer")
}
