package minio

import (
	"context"
	"io"
	"strconv"

	util_io "github.com/ValerySidorin/charon/pkg/util/io"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
)

const (
	ArchiveName string = "delta.zip"
	Delimiter   string = "/"
)

type Config struct {
	Endpoint          string `yaml:"endpoint"`
	MinioRootUser     string `yaml:"minio_root_user"`
	MinioRootPassword string `yaml:"minio_root_password"`
	Secure            bool   `yaml:"secure"`
}

type MinioReader struct {
	client minio.Client
	bucket string
}

type MinioWriter struct {
	client minio.Client
	bucket string
}

func NewReader(cfg Config, bucket string) (*MinioReader, error) {
	minioClient, err := minio.New("localhost:9000", &minio.Options{})
	if err != nil {
		return nil, errors.Wrap(err, "initialize minio reader")
	}

	return &MinioReader{
		client: *minioClient,
		bucket: bucket,
	}, nil
}

func NewWriter(cfg Config, bucket string) (*MinioWriter, error) {
	minioClient, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioRootUser, cfg.MinioRootPassword, ""),
		Secure: cfg.Secure,
	})
	if err != nil {
		return nil, errors.Wrap(err, "initialize minio client for writer")
	}

	found, err := minioClient.BucketExists(context.Background(), bucket)
	if err != nil {
		return nil, errors.Wrap(err, "check minio bucket exists")
	}

	if !found {
		if err := minioClient.MakeBucket(context.Background(), bucket, *&minio.MakeBucketOptions{}); err != nil {
			return nil, errors.Wrap(err, "make minio bucket")
		}
	}

	return &MinioWriter{
		client: *minioClient,
		bucket: bucket,
	}, nil
}

func (c *MinioReader) Retrieve(ctx context.Context, objName string) (io.ReadCloser, error) {
	var opts minio.GetObjectOptions
	opts.Set("x-minio-extract", "true")

	obj, err := c.client.GetObject(ctx, c.bucket, objName, opts)
	if err != nil {
		return nil, errors.Wrap(err, "retrieve minio object")
	}

	return obj, nil
}

func (c *MinioReader) RetrieveObjNamesByVersion(ctx context.Context, version int32) ([]string, error) {
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

func (c *MinioWriter) Store(ctx context.Context, objName string, r io.Reader) error {
	size, err := util_io.TryGetSize(r)
	if err != nil {
		return errors.Wrap(err, "store minio object")
	}

	_, err = c.client.PutObject(ctx, c.bucket, objName, r, size, minio.PutObjectOptions{
		ContentType: "application/x-zip-compressed",
	})
	if err != nil {
		return errors.Wrap(err, "store minio object")
	}

	return nil
}

func (c *MinioWriter) GetLatestVersion(ctx context.Context) (int32, error) {
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

	return versions[len(versions)-1], nil
}
