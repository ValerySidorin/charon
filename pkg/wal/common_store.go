package wal

import "context"

type CommonStore interface {
	BeginTransaction(ctx context.Context) error
	RollbackTransaction(ctx context.Context) error
	CommitTransaction(ctx context.Context) error
	LockAllRecords(ctx context.Context) error
	Dispose(ctx context.Context) error
}
