package mock

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/ValerySidorin/charon/pkg/cluster/processor/record"
	"github.com/ValerySidorin/charon/pkg/processor/batch"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/samber/lo"
)

const (
	fail = false
)

type Config struct {
	Type string
}

func (c *Config) RegisterFlags(flagPrefix string, f *flag.FlagSet) {
	f.StringVar(&c.Type, flagPrefix+"mock.type", "mock", `Mock plugin.`)
}

type Plugin struct {
	cfg Config
	log log.Logger
}

func NewPlugin(cfg Config, log log.Logger) (*Plugin, error) {
	return &Plugin{
		cfg: cfg,
		log: log,
	}, nil
}

func (m *Plugin) Exec(ctx context.Context, rec *record.Record, stream io.Reader) error {
	_ = level.Debug(m.log).Log("msg", "mock plugin start")
	if fail {
		return errors.New("mock plugin")
	}
	_ = level.Debug(m.log).Log("msg", "mock plugin stop")
	return nil
}

func (m *Plugin) UpgradeVersion(ctx context.Context, version int) error {
	_ = level.Debug(m.log).Log("msg", fmt.Sprintf("mock upgrade version to: %d", version))
	return nil
}

func (m *Plugin) GetVersion(ctx context.Context) (int, error) {
	return 20230313, nil
}

func (m *Plugin) Filter(recs []*record.Record) []*record.Record {
	return lo.Filter(recs, func(item *record.Record, index int) bool {
		return strings.Contains(item.ObjName, "OBJECT_LEVEL") || strings.Contains(item.ObjName, "ADM_HIERARCHY") || strings.Contains(item.ObjName, "ADDR_OBJ_2")
	})
}

func (m *Plugin) Batches(recs []*record.Record) []*batch.Batch {
	sort.Slice(recs[:], func(i, j int) bool {
		return recs[i].ObjName < recs[j].ObjName
	})
	batches := make([][]*record.Record, 3)
	for _, rec := range recs {
		if strings.Contains(rec.ObjName, "OBJECT_LEVEL") {
			batches[0] = append(batches[0], rec)
		}
		if strings.Contains(rec.ObjName, "ADM_HIERARCHY") {
			batches[1] = append(batches[1], rec)
		}
		if strings.Contains(rec.ObjName, "ADDR_OBJ_2") {
			batches[2] = append(batches[2], rec)
		}
	}

	res := make([]*batch.Batch, 3)
	for i, b := range batches {
		res[i] = batch.New(uint(i), b)
	}
	return res
}

func (p *Plugin) Dispose(ctx context.Context) error {
	return nil
}
