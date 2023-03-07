package wal

import (
	"context"
	"encoding/json"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	downloaderSharedWalPrefix = "downloader_wal/"
	PROCESSING                = "processing"
	FINISHED                  = "finished"
)

type Record struct {
	DownloaderID string
	Status       string
}

type Client struct {
	cfg     kv.Config
	kvStore kv.Client
}

func NewRecord(downloaderID string, status string) *Record {
	return &Record{
		DownloaderID: downloaderID,
		Status:       status,
	}
}

func NewClient(cfg kv.Config, reg prometheus.Registerer, log log.Logger) (*Client, error) {
	cfg.Prefix += downloaderSharedWalPrefix
	kv, err := kv.NewClient(cfg, codec.String{}, reg, log)
	if err != nil {
		return nil, err
	}

	return &Client{
		cfg:     cfg,
		kvStore: kv,
	}, nil
}

func (w *Client) CreateRecord(ctx context.Context, version string, dID string) error {
	err := w.kvStore.CAS(ctx, version, func(in interface{}) (out interface{}, retry bool, retErr error) {
		r, err := w.kvStore.Get(ctx, version)
		if err != nil {
			retErr = err
		}
		if r != nil {
			retErr = errors.New("record already exists")
		}

		rec := NewRecord(dID, PROCESSING)
		j, _ := json.Marshal(*rec)
		out = string(j)
		retry = false
		return
	})
	if err != nil {
		return errors.Wrap(err, "create downloader wal record")
	}

	return nil
}

func (w *Client) StealRecord(ctx context.Context, version string, dID string) error {
	err := w.kvStore.CAS(ctx, version, func(in interface{}) (out interface{}, retry bool, err error) {
		rec := NewRecord(dID, PROCESSING)
		j, _ := json.Marshal(*rec)
		out = string(j)
		retry = false
		return
	})
	if err != nil {
		return errors.Wrap(err, "steal downloader wal record")
	}

	return nil
}

func (w *Client) GetRecord(ctx context.Context, version string) (*Record, error) {
	r, err := w.kvStore.Get(ctx, version)
	if err != nil {
		return nil, errors.Wrap(err, "get wal record")
	}

	j := r.(string)
	record := Record{}
	if err := json.Unmarshal([]byte(j), &record); err != nil {
		return nil, errors.Wrap(err, "unmarshal wal record")
	}

	return &record, nil
}

func (w *Client) GetVersionByDownloader(ctx context.Context, downloaderID string) (string, error) {
	keys, err := w.kvStore.List(ctx, w.cfg.Prefix)
	if err != nil {
		return "", errors.Wrap(err, "wal client list keys")
	}
	for _, key := range keys {
		v, err := w.kvStore.Get(ctx, key)
		if err != nil {
			return "", errors.Wrap(err, "wal client get record")
		}
		j := v.(string)
		record := Record{}
		if err := json.Unmarshal([]byte(j), &record); err != nil {
			return "", errors.Wrap(err, "unmarshal wal record")
		}

		if record.DownloaderID == downloaderID {
			return key, nil
		}
	}
	return "", nil
}
