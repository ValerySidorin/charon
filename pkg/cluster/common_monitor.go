package cluster

import (
	"context"

	"github.com/ValerySidorin/charon/pkg/cluster/config"
)

const (
	FileTypeFull = "FULL"
	FileTypeDiff = "DIFF"
)

type CommonMonitor struct {
	Cfg   config.Config
	Store CommonStore
}

func (m *CommonMonitor) Lock(ctx context.Context) error {
	if err := m.Store.BeginTransaction(ctx); err != nil {
		return err
	}

	if err := m.Store.LockAllRecords(ctx); err != nil {
		if rbErr := m.Store.RollbackTransaction(ctx); rbErr != nil {
			return rbErr
		}

		return err
	}

	return nil
}

func (m *CommonMonitor) Unlock(ctx context.Context, commit bool) error {
	if commit {
		if err := m.Store.CommitTransaction(ctx); err != nil {
			if rbErr := m.Store.RollbackTransaction(ctx); rbErr != nil {
				return rbErr
			}

			return err
		}
	} else {
		if err := m.Store.RollbackTransaction(ctx); err != nil {
			return err
		}
	}

	return nil
}
