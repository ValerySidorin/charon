package plugin

import (
	"context"
	"flag"
	"io"

	"github.com/ValerySidorin/charon/pkg/processor/batch"
	"github.com/ValerySidorin/charon/pkg/processor/plugin/mock"
	"github.com/ValerySidorin/charon/pkg/wal/processor/record"
	"github.com/go-kit/log"
)

type Config struct {
	Mock mock.Config `yaml:"mock"`
}

func (c *Config) RegisterFlags(flagPrefix string, f *flag.FlagSet) {
	c.Mock.RegisterFlags(flagPrefix, f)
}

type Plugin interface {
	Exec(ctx context.Context, stream io.Reader) error
	Filter(recs []*record.Record) []*record.Record
	GetBatches(recs []*record.Record) []*batch.Batch
	UpgradeVersion(ctx context.Context, version int) error
	GetVersion(ctx context.Context) int
}

func New(cfg Config, log log.Logger) (Plugin, error) {
	plug := mock.NewPlugin(cfg.Mock, log)
	return plug, nil
}
