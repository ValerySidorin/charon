package minio

import (
	"context"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	util_io "github.com/ValerySidorin/charon/pkg/util/io"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
)

const (
	ArchiveName string = "gar.zip"
	Delimiter   string = "/"
)

type Config struct {
	Endpoint          string `mapstructure:"endpoint"`
	MinioRootUser     string `mapstructure:"minio_root_user"`
	MinioRootPassword string `mapstructure:"minio_root_password"`
	Secure            bool   `mapstructure:"secure"`
}

func (c *Config) RegisterFlags(flagPrefix string, f *flag.FlagSet) {
	f.StringVar(&c.Endpoint, flagPrefix+"minio.endpoint", "", `Minio endpoint.`)
	f.StringVar(&c.MinioRootUser, flagPrefix+"minio.minio-root-user", "", `Minio root user.`)
	f.StringVar(&c.MinioRootPassword, flagPrefix+"minio.minio-root-password", "", `Minio root password.`)
	f.BoolVar(&c.Secure, flagPrefix+"minio.secure", false, `Minio secure.`)
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
	minioClient, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioRootUser, cfg.MinioRootPassword, ""),
		Secure: cfg.Secure,
	})
	if err != nil {
		return nil, errors.Wrap(err, "initialize minio client for reader")
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
		if err := minioClient.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{}); err != nil {
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

func (c *MinioReader) RetrieveObjNamesByVersion(ctx context.Context, version int, typ string) ([]string, error) {
	var opts minio.ListObjectsOptions
	opts.Set("x-minio-extract", "true")
	opts.Recursive = true
	opts.Prefix = fmt.Sprintf("%d%s%s.zip%s", version, Delimiter, typ, Delimiter)

	result := make([]string, 0)
	for obj := range c.client.ListObjects(ctx, c.bucket, opts) {
		result = append(result, obj.Key)
	}

	return result, nil
}

func (c *MinioWriter) Store(ctx context.Context, version int, typ string, r io.Reader) error {
	size, err := util_io.TryGetSize(r)
	if err != nil {
		return errors.Wrap(err, "store minio object")
	}

	objName := strconv.Itoa(version) + Delimiter + typ + ".zip"
	_, err = c.client.PutObject(ctx, c.bucket, objName, r, size, minio.PutObjectOptions{
		ContentType: "application/x-zip-compressed",
	})
	if err != nil {
		return errors.Wrap(err, "store minio object")
	}

	return nil
}

func (c *MinioReader) ListVersionsWithTypes(ctx context.Context) ([]string, error) {
	keys := make([]string, 0)
	for obj := range c.client.ListObjects(ctx, c.bucket, minio.ListObjectsOptions{
		Recursive: true,
	}) {
		if obj.Key == "" {
			return keys, nil
		}
		tokens := strings.Split(obj.Key, Delimiter)
		if len(tokens) != 2 {
			return nil, errors.New("invalid object")
		}
		ver := tokens[0]
		typ := strings.TrimSuffix(tokens[1], filepath.Ext(tokens[1]))
		keys = append(keys, fmt.Sprintf("%s_%s", ver, typ))
	}

	return keys, nil
}

func (c *MinioReader) Delete(ctx context.Context, version int) error {
	for obj := range c.client.ListObjects(ctx, c.bucket, minio.ListObjectsOptions{
		Prefix:    fmt.Sprintf("%d/", version),
		Recursive: true,
	}) {
		if err := c.client.RemoveObject(ctx, c.bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			return errors.Wrap(err, "minio reader: delete object")
		}
	}

	return nil
}
