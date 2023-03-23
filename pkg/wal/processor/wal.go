package processor

import (
	"context"

	"github.com/ValerySidorin/charon/pkg/wal"
	"github.com/ValerySidorin/charon/pkg/wal/config"
	"github.com/ValerySidorin/charon/pkg/wal/processor/record"
	"github.com/ValerySidorin/charon/pkg/wal/processor/store"
	"github.com/go-kit/log"
)

type WAL struct {
	wal.CommonWAL
}

func NewWAL(ctx context.Context, cfg config.Config, log log.Logger) (*WAL, error) {
	store, err := store.NewWALStore(ctx, cfg, log)
	if err != nil {
		return nil, err
	}

	w := WAL{}
	w.Cfg = cfg
	w.Store = store
	return &w, nil
}

func (w *WAL) GetLastVersion(ctx context.Context) (int, error) {
	return w.Store.(store.Store).GetLastVersion(ctx)
}

func (w *WAL) MergeRecords(ctx context.Context, recs []*record.Record) error {
	return w.Store.(store.Store).MergeRecords(ctx, recs)
}
