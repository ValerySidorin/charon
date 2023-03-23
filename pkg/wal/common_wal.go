package wal

import (
	"context"

	"github.com/ValerySidorin/charon/pkg/wal/config"
)

const (
	FileTypeFull = "FULL"
	FileTypeDiff = "DIFF"
)

type CommonWAL struct {
	Cfg   config.Config
	Store CommonStore
}

func (w *CommonWAL) Lock(ctx context.Context) error {
	if err := w.Store.BeginTransaction(ctx); err != nil {
		return err
	}

	if err := w.Store.LockAllRecords(ctx); err != nil {
		if rbErr := w.Store.RollbackTransaction(ctx); rbErr != nil {
			return rbErr
		}

		return err
	}

	return nil
}

func (w *CommonWAL) Unlock(ctx context.Context, commit bool) error {
	if commit {
		if err := w.Store.CommitTransaction(ctx); err != nil {
			if rbErr := w.Store.RollbackTransaction(ctx); rbErr != nil {
				return rbErr
			}

			return err
		}
	} else {
		if err := w.Store.RollbackTransaction(ctx); err != nil {
			return err
		}
	}

	return nil
}
