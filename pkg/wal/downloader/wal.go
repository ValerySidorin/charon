package downloader

import (
	"context"

	"github.com/ValerySidorin/charon/pkg/wal"
	"github.com/ValerySidorin/charon/pkg/wal/config"
	"github.com/ValerySidorin/charon/pkg/wal/downloader/record"
	d_wal_store "github.com/ValerySidorin/charon/pkg/wal/downloader/store"
	"github.com/go-kit/log"
)

type WAL struct {
	wal.CommonWAL
}

func NewWAL(ctx context.Context, cfg config.Config, log log.Logger) (*WAL, error) {
	store, err := d_wal_store.NewWALStore(ctx, cfg, log)
	if err != nil {
		return nil, err
	}

	w := WAL{}
	w.Cfg = cfg
	w.Store = store
	return &w, nil
}

func (w *WAL) GetFirstRecord(ctx context.Context) (*record.Record, bool, error) {
	return w.Store.(d_wal_store.Store).GetFirstRecord(ctx)
}

func (w *WAL) GetAllRecords(ctx context.Context) ([]*record.Record, error) {
	return w.Store.(d_wal_store.Store).GetAllRecords(ctx)
}

func (w *WAL) GetRecordsByStatus(ctx context.Context, status string) ([]*record.Record, error) {
	return w.Store.(d_wal_store.Store).GetRecordsByStatus(ctx, status)
}

func (w *WAL) GetUnsentRecords(ctx context.Context) ([]*record.Record, error) {
	return w.Store.(d_wal_store.Store).GetUnsentRecords(ctx)
}

func (w *WAL) UpdateRecord(ctx context.Context, rec *record.Record) error {
	return w.Store.(d_wal_store.Store).UpdateRecord(ctx, rec)
}

func (w *WAL) StealRecord(ctx context.Context, rec *record.Record, dID string) error {
	rec.DownloaderID = dID
	return w.Store.(d_wal_store.Store).UpdateRecord(ctx, rec)
}

func (w *WAL) AddRecord(ctx context.Context, rec *record.Record) error {
	return w.Store.(d_wal_store.Store).InsertRecord(ctx, rec)
}

func (w *WAL) HasCompletedRecords(ctx context.Context, dID string) (bool, error) {
	return w.Store.(d_wal_store.Store).HasCompletedRecords(ctx, dID)
}

func (w *WAL) Dispose(ctx context.Context) {
	_ = w.Store.(d_wal_store.Store).Dispose(ctx)
}
