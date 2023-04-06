package plugin

import (
	"context"
	"flag"
	"io"

	"github.com/ValerySidorin/charon/pkg/processor/batch"
	"github.com/ValerySidorin/charon/pkg/processor/plugin/mock"
	"github.com/ValerySidorin/charon/pkg/wal/processor/record"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
)

type Config struct {
	Mock mock.Config `yaml:"mock"`
}

func (c *Config) RegisterFlags(flagPrefix string, f *flag.FlagSet) {
	c.Mock.RegisterFlags(flagPrefix, f)
}

type Plugin interface {
	Exec(ctx context.Context, rec *record.Record, stream io.Reader) error
	Filter(recs []*record.Record) []*record.Record
	Batches(recs []*record.Record) []*batch.Batch
	UpgradeVersion(ctx context.Context, version int) error
	GetVersion(ctx context.Context) (int, error)
	Dispose(ctx context.Context) error
}

func New(cfg Config, log log.Logger) (Plugin, error) {
	plug, err := mock.NewPlugin(cfg.Mock, log)
	if err != nil {
		return nil, errors.Wrap(err, "plugin: init:")
	}
	return plug, nil
}
