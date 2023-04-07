package store

import (
	"context"

	wal "github.com/ValerySidorin/charon/pkg/cluster"
	"github.com/ValerySidorin/charon/pkg/cluster/config"
	"github.com/ValerySidorin/charon/pkg/cluster/downloader/record"
	"github.com/ValerySidorin/charon/pkg/cluster/downloader/store/pg"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
)

type Store interface {
	wal.CommonStore
	GetFirstRecord(ctx context.Context) (*record.Record, bool, error)
	GetAllRecords(ctx context.Context) ([]*record.Record, error)
	GetRecordsByStatus(ctx context.Context, status string) ([]*record.Record, error)
	GetUnsentRecords(ctx context.Context) ([]*record.Record, error)
	InsertRecord(ctx context.Context, rec *record.Record) error
	UpdateRecord(ctx context.Context, rec *record.Record) error
	HasCompletedRecords(ctx context.Context, dID string) (bool, error)
}

func NewClusterStore(ctx context.Context, cfg config.Config, log log.Logger) (Store, error) {
	switch cfg.Store {
	case "pg":
		return pg.NewClusterStore(ctx, cfg.Pg, log)
	default:
		return nil, errors.New("invalid store in config")
	}
}
