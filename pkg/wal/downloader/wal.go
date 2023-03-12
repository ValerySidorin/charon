package downloader

import (
	"context"

	"github.com/ValerySidorin/charon/pkg/wal/config"
	"github.com/ValerySidorin/charon/pkg/wal/downloader/record"
	"github.com/ValerySidorin/charon/pkg/wal/downloader/store"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
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
		return errors.Wrap(err, "wal lock begin transaction")
	}

	if err := w.store.LockAllRecords(ctx); err != nil {
		if rbErr := w.store.RollbackTransaction(ctx); rbErr != nil {
			return errors.Wrap(rbErr, "wal lock rollback transaction")
		}

		return errors.Wrap(err, "wal lock")
	}

	return nil
}

func (w *WAL) Unlock(ctx context.Context, commit bool) error {
	if commit {
		if err := w.store.CommitTransaction(ctx); err != nil {
			if rbErr := w.store.RollbackTransaction(ctx); rbErr != nil {
				return errors.Wrap(rbErr, "wal unlock rollback transaction")
			}

			return errors.Wrap(err, "wal unlock commit transaction")
		}
	} else {
		if err := w.store.RollbackTransaction(ctx); err != nil {
			return errors.Wrap(err, "wal unlock rollback transaction")
		}
	}

	return nil
}

func (w *WAL) GetProcessingRecords(ctx context.Context) ([]*record.Record, error) {
	r, err := w.store.GetProcessingRecords(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "wal get processing downloads")
	}

	return r, nil
}

func (w *WAL) CompleteRecord(ctx context.Context, rec *record.Record) error {
	rec.Status = record.COMPLETED
	if err := w.store.UpdateRecord(ctx, rec); err != nil {
		return errors.Wrap(err, "wal complete record")
	}

	return nil
}

func (w *WAL) StealRecord(ctx context.Context, rec *record.Record, dID string) error {
	rec.DownloaderID = dID
	if err := w.store.UpdateRecord(ctx, rec); err != nil {
		return errors.Wrap(err, "steal wal record")
	}

	return nil
}

func (w *WAL) AddRecord(ctx context.Context, rec *record.Record) error {
	if err := w.store.InsertRecord(ctx, rec); err != nil {
		return errors.Wrap(err, "add wal record")
	}

	return nil
}

func (w *WAL) Dispose(ctx context.Context) {
	w.store.Dispose(ctx)
}
