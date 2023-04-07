package batch

import (
	"github.com/ValerySidorin/charon/pkg/cluster/processor/record"
	"github.com/samber/lo"
)

type Batch struct {
	Order   uint
	Records []*record.Record
}

func New(order uint, recs []*record.Record) *Batch {
	return &Batch{
		Order:   order,
		Records: recs,
	}
}

func (b Batch) IsCompleted() bool {
	return lo.EveryBy(b.Records, func(item *record.Record) bool {
		return item.Status == record.COMPLETED
	})
}
