package minio

import (
	"context"
	"io"
	"strconv"

	"github.com/ValerySidorin/charon/pkg/diffstorage"
	"github.com/minio/minio-go/v7"
	"github.com/pkg/errors"
)

const (
	ArchiveName string = "delta.zip"
	Delimiter   string = "/"
)

type MinioClient struct {
	client minio.Client
	bucket string
}

func NewClient(bucket string) (*MinioClient, error) {
	minioClient, err := minio.New("localhost:9000", &minio.Options{})
	if err != nil {
		return nil, errors.Wrap(err, "initialize minio client")
	}

	return &MinioClient{
		client: *minioClient,
		bucket: bucket,
	}, nil
}

func (c *MinioClient) Retrieve(ctx context.Context, objName string) (io.ReadCloser, error) {
	var opts minio.GetObjectOptions
	opts.Set("x-minio-extract", "true")

	obj, err := c.client.GetObject(ctx, c.bucket, objName, opts)
	if err != nil {
		return nil, errors.Wrap(err, "retrieve minio object")
	}

	return obj, nil
}

func (c *MinioClient) RetrieveObjNamesByVersion(ctx context.Context, version int32) ([]string, error) {
	var opts minio.ListObjectsOptions
	opts.Set("x-minio-extract", "true")
	opts.Recursive = true
	opts.Prefix = ArchiveName

	result := make([]string, 0)
	for obj := range c.client.ListObjects(ctx, c.bucket, opts) {
		result = append(result, obj.Key)
	}

	return result, nil
}

func (c *MinioClient) Store(ctx context.Context, objName string, r io.Reader) error {
	var opts minio.PutObjectOptions
	opts.ContentType = "application/x-zip-compressed"
	size, err := diffstorage.TryGetSize(r)
	if err != nil {
		return errors.Wrap(err, "store minio object")
	}

	_, err = c.client.PutObject(ctx, c.bucket, objName, r, size, opts)
	if err != nil {
		return errors.Wrap(err, "store minio object")
	}

	return nil
}

func (c *MinioClient) GetLatestVersion(ctx context.Context) (int32, error) {
	versions := make([]int32, 0)
	for obj := range c.client.ListObjects(ctx, c.bucket, minio.ListObjectsOptions{}) {
		last := len(obj.Key)
		version, err := strconv.Atoi(obj.Key[:last-1])
		if err != nil {
			return 0, errors.Wrap(err, "get latest diff version minio")
		}

		versions = append(versions, int32(version))
	}

	if len(versions) == 0 {
		return 0, nil
	}

	latestIdx := len(versions) - 1
	return versions[latestIdx], nil
}
