package downloader

import (
	"context"

	"github.com/ValerySidorin/charon/pkg/cluster"
	"github.com/ValerySidorin/charon/pkg/cluster/config"
	"github.com/ValerySidorin/charon/pkg/cluster/downloader/record"
	cluster_store "github.com/ValerySidorin/charon/pkg/cluster/downloader/store"
	"github.com/go-kit/log"
)

type Monitor struct {
	cluster.CommonMonitor
}

func NewMonitor(ctx context.Context, cfg config.Config, log log.Logger) (*Monitor, error) {
	store, err := cluster_store.NewClusterStore(ctx, cfg, log)
	if err != nil {
		return nil, err
	}

	m := Monitor{}
	m.Cfg = cfg
	m.Store = store
	return &m, nil
}

func (m *Monitor) GetFirstRecord(ctx context.Context) (*record.Record, bool, error) {
	return m.Store.(cluster_store.Store).GetFirstRecord(ctx)
}

func (m *Monitor) GetAllRecords(ctx context.Context) ([]*record.Record, error) {
	return m.Store.(cluster_store.Store).GetAllRecords(ctx)
}

func (m *Monitor) GetRecordsByStatus(ctx context.Context, status string) ([]*record.Record, error) {
	return m.Store.(cluster_store.Store).GetRecordsByStatus(ctx, status)
}

func (m *Monitor) GetUnsentRecords(ctx context.Context) ([]*record.Record, error) {
	return m.Store.(cluster_store.Store).GetUnsentRecords(ctx)
}

func (m *Monitor) UpdateRecord(ctx context.Context, rec *record.Record) error {
	return m.Store.(cluster_store.Store).UpdateRecord(ctx, rec)
}

func (m *Monitor) StealRecord(ctx context.Context, rec *record.Record, dID string) error {
	rec.DownloaderID = dID
	return m.Store.(cluster_store.Store).UpdateRecord(ctx, rec)
}

func (m *Monitor) AddRecord(ctx context.Context, rec *record.Record) error {
	return m.Store.(cluster_store.Store).InsertRecord(ctx, rec)
}

func (m *Monitor) HasCompletedRecords(ctx context.Context, dID string) (bool, error) {
	return m.Store.(cluster_store.Store).HasCompletedRecords(ctx, dID)
}

func (m *Monitor) Dispose(ctx context.Context) {
	_ = m.Store.(cluster_store.Store).Dispose(ctx)
}
