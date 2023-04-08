package objstore

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/ValerySidorin/charon/pkg/objstore/minio"
)

const (
	Bucket = "charon"
)

type Config struct {
	Store string       `mapstructure:"store"`
	Minio minio.Config `mapstructure:"minio"`
}

func (c *Config) RegisterFlags(flagPrefix string, f *flag.FlagSet) {
	f.StringVar(&c.Store, flagPrefix+"store", "", `Object storage, that will be used to store files from source.`)
	c.Minio.RegisterFlags(flagPrefix, f)
}

type Writer interface {
	Store(ctx context.Context, version int, typ string, r io.Reader) error
}

type Reader interface {
	Retrieve(ctx context.Context, objName string) (io.ReadCloser, error)
	RetrieveObjNamesByVersion(ctx context.Context, version int, typ string) ([]string, error)
	ListVersionsWithTypes(ctx context.Context) ([]string, error)
	Delete(ctx context.Context, version int) error
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
