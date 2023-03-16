package downloader

import (
	"context"

	"github.com/ValerySidorin/charon/pkg/wal/config"
	"github.com/ValerySidorin/charon/pkg/wal/downloader/record"
	"github.com/ValerySidorin/charon/pkg/wal/downloader/store"
	"github.com/go-kit/log"
)

const (
	FileTypeFull = "full"
	FileTypeDiff = "diff"
)

type WAL struct {
	cfg   config.Config
	store store.Store
}

func NewWAL(ctx context.Context, cfg config.Config, log log.Logger) (*WAL, error) {
	store, err := store.NewWALStore(ctx, cfg, log)
	if err != nil {
		return nil, err
	}

	return &WAL{
		cfg:   cfg,
		store: store,
	}, nil
}

func (w *WAL) Lock(ctx context.Context) error {
	if err := w.store.BeginTransaction(ctx); err != nil {
		return err
	}

	if err := w.store.LockAllRecords(ctx); err != nil {
		if rbErr := w.store.RollbackTransaction(ctx); rbErr != nil {
			return rbErr
		}

		return err
	}

	return nil
}

func (w *WAL) Unlock(ctx context.Context, commit bool) error {
	if commit {
		if err := w.store.CommitTransaction(ctx); err != nil {
			if rbErr := w.store.RollbackTransaction(ctx); rbErr != nil {
				return rbErr
			}

			return err
		}
	} else {
		if err := w.store.RollbackTransaction(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (w *WAL) GetFirstRecord(ctx context.Context) (*record.Record, bool, error) {
	return w.store.GetFirstRecord(ctx)
}

func (w *WAL) GetAllRecords(ctx context.Context) ([]*record.Record, error) {
	return w.store.GetAllRecords(ctx)
}

func (w *WAL) GetRecordsByStatus(ctx context.Context, status string) ([]*record.Record, error) {
	return w.store.GetRecordsByStatus(ctx, status)
}

func (w *WAL) CompleteRecord(ctx context.Context, rec *record.Record) error {
	rec.Status = record.COMPLETED
	return w.store.UpdateRecord(ctx, rec)
}

func (w *WAL) StealRecord(ctx context.Context, rec *record.Record, dID string) error {
	rec.DownloaderID = dID
	return w.store.UpdateRecord(ctx, rec)
}

func (w *WAL) AddRecord(ctx context.Context, rec *record.Record) error {
	return w.store.InsertRecord(ctx, rec)
}

func (w *WAL) Dispose(ctx context.Context) {
	w.store.Dispose(ctx)
}
