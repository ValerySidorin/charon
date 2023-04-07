package processor

import (
	"context"

	"github.com/ValerySidorin/charon/pkg/cluster"
	"github.com/ValerySidorin/charon/pkg/cluster/config"
	"github.com/ValerySidorin/charon/pkg/cluster/processor/record"
	"github.com/ValerySidorin/charon/pkg/cluster/processor/store"
	"github.com/go-kit/log"
)

type Monitor struct {
	cluster.CommonMonitor
}

func NewMonitor(ctx context.Context, cfg config.Config, log log.Logger) (*Monitor, error) {
	store, err := store.NewClusterStore(ctx, cfg, log)
	if err != nil {
		return nil, err
	}

	w := Monitor{}
	w.Cfg = cfg
	w.Store = store
	return &w, nil
}

func (w *Monitor) GetRecordsByVersion(ctx context.Context, version int) ([]*record.Record, error) {
	return w.Store.(store.Store).GetRecordsByVersion(ctx, version)
}

func (w *Monitor) GetLastVersion(ctx context.Context) (int, error) {
	return w.Store.(store.Store).GetLastVersion(ctx)
}

func (w *Monitor) GetFirstIncompleteVersion(ctx context.Context) (int, error) {
	return w.Store.(store.Store).GetFirstIncompleteVersion(ctx)
}

func (w *Monitor) GetIncompleteRecordsByVersion(ctx context.Context, version int) ([]*record.Record, error) {
	return w.Store.(store.Store).GetIncompleteRecordsByVersion(ctx, version)
}

func (w *Monitor) MergeRecords(ctx context.Context, recs []*record.Record) error {
	return w.Store.(store.Store).MergeRecords(ctx, recs)
}

func (w *Monitor) UpdateRecord(ctx context.Context, rec *record.Record) error {
	return w.Store.(store.Store).UpdateRecord(ctx, rec)
}

func (w *Monitor) Dispose(ctx context.Context) error {
	return w.Store.(store.Store).Dispose(ctx)
}
