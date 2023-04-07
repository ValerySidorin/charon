package store

import (
	"context"
	"errors"

	"github.com/ValerySidorin/charon/pkg/cluster"
	"github.com/ValerySidorin/charon/pkg/cluster/config"
	"github.com/ValerySidorin/charon/pkg/cluster/processor/record"
	"github.com/ValerySidorin/charon/pkg/cluster/processor/store/pg"
	"github.com/go-kit/log"
)

type Store interface {
	cluster.CommonStore
	MergeRecords(ctx context.Context, recs []*record.Record) error
	UpdateRecord(ctx context.Context, rec *record.Record) error
	GetRecordsByVersion(ctx context.Context, version int) ([]*record.Record, error)
	GetLastVersion(ctx context.Context) (int, error)
	GetFirstIncompleteVersion(ctx context.Context) (int, error)
	GetIncompleteRecordsByVersion(ctx context.Context, version int) ([]*record.Record, error)
}

func NewClusterStore(ctx context.Context, cfg config.Config, log log.Logger) (Store, error) {
	switch cfg.Store {
	case "pg":
		return pg.NewClusterStore(ctx, cfg.Name, cfg.Pg, log)
	default:
		return nil, errors.New("invalid store in config")
	}
}
