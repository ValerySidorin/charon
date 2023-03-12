package diffstore

import (
	"context"
	"fmt"
	"io"

	"github.com/ValerySidorin/charon/pkg/diffstore/minio"
)

type Config struct {
	Store string       `yaml:"store"`
	Minio minio.Config `yaml:"minio"`
}

type Writer interface {
	Store(ctx context.Context, version int, r io.Reader) error
	ListVersions(ctx context.Context) ([]int, error)
}

type Reader interface {
	Retrieve(ctx context.Context, objName string) (io.ReadCloser, error)
	RetrieveObjNamesByVersion(ctx context.Context, version int) ([]string, error)
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
