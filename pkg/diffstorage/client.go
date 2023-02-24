package diffstorage

import (
	"context"
	"io"
)

type DiffStorageWriter interface {
	Store(ctx context.Context, objName string, r io.Reader) error
	GetLatestVersion(ctx context.Context) (int32, error)
}

type DiffStorageReader interface {
	Retrieve(ctx context.Context, objName string) (io.ReadCloser, error)
	RetrieveObjNamesByVersion(ctx context.Context, version int32) ([]string, error)
}
