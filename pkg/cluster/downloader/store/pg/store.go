package pg

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ValerySidorin/charon/pkg/cluster/config/pg"
	"github.com/ValerySidorin/charon/pkg/cluster/downloader/record"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

type Store struct {
	cfg pg.Config
	log log.Logger

	cancel chan struct{}

	pool   *pgxpool.Pool
	currTx pgx.Tx
}

func NewClusterStore(ctx context.Context, cfg pg.Config, log log.Logger) (*Store, error) {
	store := &Store{
		cfg: cfg,
		log: log,
	}

	err := store.connect(ctx)
	if err != nil {
		return nil, err
	}

	q := `create table if not exists public.downloader_cluster (
		version integer not null, 
		downloader_id text not null, 
		download_url text not null, 
		type text not null, 
		status text not null,
		constraint unique_version unique (version));`
	if _, err := store.pool.Exec(ctx, q); err != nil {
		return nil, errors.Wrap(err, "pg wal store init wal table")
	}

	return store, nil
}

func (s *Store) connect(ctx context.Context) error {
	var err error
	s.pool, err = pgxpool.New(ctx, s.cfg.Conn)
	if err != nil {
		return errors.Wrap(err, "postgres: init connection")
	}

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()

		select {
		case <-t.C:
			err := s.pool.Ping(ctx)
			if err != nil {
				_ = level.Warn(s.log).Log("msg", "lost connection to database, attempting to reconnect...", "err", err.Error())
				pool, err := pgxpool.New(ctx, s.cfg.Conn)
				if err != nil {
					_ = level.Error(s.log).Log("msg", fmt.Sprintf("failed to reconnect to database: %s", err))
				} else {
					oldPool := s.pool
					s.pool = pool

					// Dispose old connection
					oldPool.Close()
					s.currTx = nil

					_ = level.Info(s.log).Log("msg", "successfully reconnected to database")
				}
			}
		case <-s.cancel:
			return
		}
	}()

	return nil
}

func (s *Store) BeginTransaction(ctx context.Context) error {
	if s.currTx != nil {
		return errors.New("pg wal store s.currTx is not null")
	}
	t, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "pg wal store begin transation")
	}
	s.currTx = t
	return nil
}

func (s *Store) RollbackTransaction(ctx context.Context) error {
	if s.currTx == nil {
		return nil
	}

	if err := s.currTx.Rollback(ctx); err != nil {
		return errors.Wrap(err, "pg wal store rollback transaction")
	}

	s.currTx = nil
	return nil
}

func (s *Store) CommitTransaction(ctx context.Context) error {
	if s.currTx == nil {
		return nil
	}

	if err := s.currTx.Commit(ctx); err != nil {
		return errors.Wrap(err, "pg wal store commit transaction")
	}

	s.currTx = nil
	return nil
}

func (s *Store) LockAllRecords(ctx context.Context) error {
	if s.currTx == nil {
		return errors.New("pg wal store can't lock table without transaction")
	}

	q := "lock table downloader_cluster in access exclusive mode;"
	_, err := s.currTx.Exec(ctx, q)
	if err != nil {
		return errors.Wrap(err, "pg wal store lock table")
	}

	return nil
}

func (s *Store) GetFirstRecord(ctx context.Context) (*record.Record, bool, error) {
	q := "select version, downloader_id, download_url, type, status from downloader_cluster limit 1"
	var rec record.Record

	if s.currTx != nil {
		row := s.currTx.QueryRow(ctx, q)
		if err := scanRecordFromRow(row, &rec); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, false, nil
			}
			return nil, false, errors.Wrap(err, "pg wal store query first record")
		}

		return &rec, true, nil
	}

	row := s.pool.QueryRow(ctx, q)
	if err := scanRecordFromRow(row, &rec); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, errors.Wrap(err, "pg wal store query first record")
	}

	return &rec, true, nil
}

func (s *Store) GetAllRecords(ctx context.Context) ([]*record.Record, error) {
	q := "select version, downloader_id, download_url, type, status from downloader_cluster;"
	recs := make([]*record.Record, 0)

	if s.currTx != nil {
		rows, err := s.currTx.Query(ctx, q)
		if err != nil {
			return nil, errors.Wrap(err, "pg wal store query all records")
		}

		for rows.Next() {
			rec := record.Record{}
			if err := scanRecordFromRows(rows, &rec); err != nil {
				return nil, errors.Wrap(err, "pg wal store scan all records")
			}
			recs = append(recs, &rec)
		}

		return recs, nil
	}

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, errors.Wrap(err, "pg wal store query all records")
	}

	for rows.Next() {
		rec := record.Record{}
		if err := scanRecordFromRows(rows, &rec); err != nil {
			return nil, errors.Wrap(err, "pg wal store scan all records")
		}
		recs = append(recs, &rec)
	}

	return recs, nil
}

func (s *Store) GetRecordsByStatus(ctx context.Context, status string) ([]*record.Record, error) {
	q := "select version, downloader_id, download_url, type, status from downloader_cluster where status = $1;"
	recs := make([]*record.Record, 0)

	if s.currTx != nil {
		rows, err := s.currTx.Query(ctx, q, status)
		if err != nil {
			return nil, errors.Wrap(err, "pg wal store query records by status")
		}

		for rows.Next() {
			rec := record.Record{}
			if err := scanRecordFromRows(rows, &rec); err != nil {
				return nil, errors.Wrap(err, "pg wal store scan records by status")
			}
			recs = append(recs, &rec)
		}

		return recs, nil
	}

	rows, err := s.pool.Query(ctx, q, status)
	if err != nil {
		return nil, errors.Wrap(err, "pg wal store query records by status")
	}

	for rows.Next() {
		rec := record.Record{}
		if err := scanRecordFromRows(rows, &rec); err != nil {
			return nil, errors.Wrap(err, "pg wal store scan records by status")
		}
		recs = append(recs, &rec)
	}

	return recs, nil
}

func (s *Store) GetUnsentRecords(ctx context.Context) ([]*record.Record, error) {
	q := "select version, downloader_id, download_url, type, status from downloader_cluster where status != 'SENT';"
	recs := make([]*record.Record, 0)

	if s.currTx != nil {
		rows, err := s.currTx.Query(ctx, q)
		if err != nil {
			return nil, errors.Wrap(err, "pg wal store query unsent records")
		}

		for rows.Next() {
			rec := record.Record{}
			if err := scanRecordFromRows(rows, &rec); err != nil {
				return nil, errors.Wrap(err, "pg wal store scan unsent records")
			}
			recs = append(recs, &rec)
		}

		return recs, nil
	}

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, errors.Wrap(err, "pg wal store query unsent records")
	}

	for rows.Next() {
		rec := record.Record{}
		if err := scanRecordFromRows(rows, &rec); err != nil {
			return nil, errors.Wrap(err, "pg wal store scan unsent records")
		}
		recs = append(recs, &rec)
	}

	return recs, nil
}

func (s *Store) InsertRecord(ctx context.Context, rec *record.Record) error {
	q := `insert into downloader_cluster(version, downloader_id, download_url, type, status)
	values($1, $2, $3, $4, $5);`

	if s.currTx != nil {
		_, err := s.currTx.Exec(ctx, q, rec.Version, rec.DownloaderID, rec.DownloadURL, rec.Type, rec.Status)
		if err != nil {
			return errors.Wrap(err, "pg wal store insert record")
		}

		return nil
	}

	_, err := s.pool.Exec(ctx, q, rec.Version, rec.DownloaderID, rec.DownloadURL, rec.Type, rec.Status)
	if err != nil {
		return errors.Wrap(err, "pg wal store insert record")
	}

	return nil
}

func (s *Store) UpdateRecord(ctx context.Context, rec *record.Record) error {
	q := `update downloader_cluster
	set downloader_id = $2,
	download_url = $3,
	type = $4,
	status = $5
	where version = $1;`

	if s.currTx != nil {
		_, err := s.currTx.Exec(ctx, q, rec.Version, rec.DownloaderID, rec.DownloadURL, rec.Type, rec.Status)
		if err != nil {
			return errors.Wrap(err, "pg wal store update record")
		}

		return nil
	}

	_, err := s.pool.Exec(ctx, q, rec.Version, rec.DownloaderID, rec.DownloadURL, rec.Type, rec.Status)
	if err != nil {
		return errors.Wrap(err, "pg wal store update record")
	}

	return nil
}

func (s *Store) HasCompletedRecords(ctx context.Context, dID string) (bool, error) {
	var count int
	q := "select count(version) from downloader_cluster where downloader_id = $1 and status = 'COMPLETED'"

	if s.currTx != nil {
		if err := s.currTx.QueryRow(ctx, q, dID).Scan(&count); err != nil {
			return false, errors.Wrap(err, "pg wal store has completed records")
		}

		return count > 0, nil
	}

	if err := s.pool.QueryRow(ctx, q, dID).Scan(&count); err != nil {
		return false, errors.Wrap(err, "pg wal store has completed records")
	}

	return count > 0, nil
}

func (s *Store) Dispose(ctx context.Context) error {
	s.cancel <- struct{}{}
	close(s.cancel)

	if s.currTx != nil {
		if err := s.currTx.Rollback(ctx); err != nil {
			return errors.Wrap(err, "postgres: rollback transaction")
		}
	}

	s.pool.Close()

	return nil
}

func scanRecordFromRows(rows pgx.Rows, rec *record.Record) error {
	if err := rows.Scan(&rec.Version, &rec.DownloaderID, &rec.DownloadURL, &rec.Type, &rec.Status); err != nil {
		return err
	}

	return nil
}

func scanRecordFromRow(row pgx.Row, rec *record.Record) error {
	if err := row.Scan(&rec.Version, &rec.DownloaderID, &rec.DownloadURL, &rec.Type, &rec.Status); err != nil {
		return err
	}

	return nil
}
