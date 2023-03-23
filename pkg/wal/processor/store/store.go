package store

import (
	"context"
	"errors"

	"github.com/ValerySidorin/charon/pkg/wal"
	"github.com/ValerySidorin/charon/pkg/wal/config"
	"github.com/ValerySidorin/charon/pkg/wal/processor/record"
	"github.com/ValerySidorin/charon/pkg/wal/processor/store/pg"
	"github.com/go-kit/log"
)

type Store interface {
	wal.CommonStore
	MergeRecords(ctx context.Context, recs []*record.Record) error
	UpdateRecord(ctx context.Context, rec *record.Record) error
	GetRecordsByVersion(ctx context.Context, version int) ([]*record.Record, error)
	GetLastVersion(ctx context.Context) (int, error)
}

func NewWALStore(ctx context.Context, cfg config.Config, log log.Logger) (Store, error) {
	switch cfg.Store {
	case "pg":
		return pg.NewWALStore(ctx, cfg.Pg, log)
	default:
		return nil, errors.New("invalid store in config")
	}
}
