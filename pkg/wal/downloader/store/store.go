package store

import (
	"context"

	"github.com/ValerySidorin/charon/pkg/wal/config"
	"github.com/ValerySidorin/charon/pkg/wal/downloader/record"
	"github.com/ValerySidorin/charon/pkg/wal/downloader/store/pg"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
)

type Store interface {
	BeginTransaction(ctx context.Context) error
	RollbackTransaction(ctx context.Context) error
	CommitTransaction(ctx context.Context) error
	LockAllRecords(ctx context.Context) error
	GetFirstRecord(ctx context.Context) (*record.Record, bool, error)
	GetAllRecords(ctx context.Context) ([]*record.Record, error)
	GetRecordsByStatus(ctx context.Context, status string) ([]*record.Record, error)
	InsertRecord(ctx context.Context, rec *record.Record) error
	UpdateRecord(ctx context.Context, rec *record.Record) error
	Dispose(ctx context.Context) error
}

func NewWALStore(ctx context.Context, cfg config.Config, log log.Logger) (Store, error) {
	switch cfg.Store {
	case "pg":
		return pg.NewWALStore(ctx, cfg.Pg, log)
	default:
		return nil, errors.New("invalid store in config")
	}
}
