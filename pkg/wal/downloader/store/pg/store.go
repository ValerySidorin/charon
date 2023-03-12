package pg

import (
	"context"

	"github.com/ValerySidorin/charon/pkg/wal/config/pg"
	"github.com/ValerySidorin/charon/pkg/wal/downloader/record"
	"github.com/go-kit/log"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

type Store struct {
	cfg  pg.Config
	log  log.Logger
	conn *pgx.Conn

	currTx pgx.Tx
}

func NewWALStore(ctx context.Context, cfg pg.Config, log log.Logger) (*Store, error) {
	conn, err := pgx.Connect(ctx, cfg.Conn)
	if err != nil {
		return nil, errors.Wrap(err, "pg wal store init conn")
	}

	q := `create table if not exists public.wal 
	(version integer not null, downloader_id text not null, download_url text not null, status text not null);`
	if _, err := conn.Exec(ctx, q); err != nil {
		return nil, errors.Wrap(err, "pg wal store init wal table")
	}

	return &Store{
		cfg:  cfg,
		log:  log,
		conn: conn,
	}, nil
}

func (s *Store) BeginTransaction(ctx context.Context) error {
	if s.currTx != nil {
		return errors.New("pg wal store s.currTx is not null")
	}
	t, err := s.conn.Begin(ctx)
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

	q := "lock table wal in access exclusive mode;"
	_, err := s.currTx.Exec(ctx, q)
	if err != nil {
		return errors.Wrap(err, "pg wal store lock table")
	}

	return nil
}

func (s *Store) GetProcessingRecords(ctx context.Context) ([]*record.Record, error) {
	q := "select version, downloader_id, download_url, status from wal where status = 'PROCESSING';"
	recs := make([]*record.Record, 0)

	if s.currTx != nil {
		rows, err := s.currTx.Query(ctx, q)
		if err != nil {
			return nil, errors.Wrap(err, "pg wal store query processing records")
		}

		for rows.Next() {
			rec := record.Record{}
			if err := scanRecordFromRows(rows, &rec); err != nil {
				return nil, errors.Wrap(err, "pg wal store scan processing records")
			}
			recs = append(recs, &rec)
		}

		return recs, nil
	}

	rows, err := s.conn.Query(ctx, q)
	if err != nil {
		return nil, errors.Wrap(err, "pg wal store query processing records")
	}

	for rows.Next() {
		rec := record.Record{}
		if err := scanRecordFromRows(rows, &rec); err != nil {
			return nil, errors.Wrap(err, "pg wal store scan processing records")
		}
		recs = append(recs, &rec)
	}

	return recs, nil
}

func (s *Store) InsertRecord(ctx context.Context, rec *record.Record) error {
	q := `insert into wal(version, downloader_id, download_url, status)
	values($1, $2, $3, $4);`

	if s.currTx != nil {
		_, err := s.currTx.Exec(ctx, q, rec.Version, rec.DownloaderID, rec.DownloadURL, rec.Status)
		if err != nil {
			return errors.Wrap(err, "pg wal store insert record")
		}

		return nil
	}

	_, err := s.conn.Exec(ctx, q, rec.Version, rec.DownloaderID, rec.DownloadURL, rec.Status)
	if err != nil {
		return errors.Wrap(err, "pg wal store insert record")
	}

	return nil
}

func (s *Store) UpdateRecord(ctx context.Context, rec *record.Record) error {
	q := `update wal
	set downloader_id = $2,
	download_url = $3,
	status = $4
	where version = $1;`

	if s.currTx != nil {
		_, err := s.currTx.Exec(ctx, q, rec.Version, rec.DownloaderID, rec.DownloadURL, rec.Status)
		if err != nil {
			return errors.Wrap(err, "pg wal store update record")
		}

		return nil
	}

	_, err := s.conn.Exec(ctx, q, rec.Version, rec.DownloaderID, rec.DownloadURL, rec.Status)
	if err != nil {
		return errors.Wrap(err, "pg wal store update record")
	}

	return nil
}

func (s *Store) Dispose(ctx context.Context) error {
	if s.currTx != nil {
		if err := s.currTx.Rollback(ctx); err != nil {
			return errors.Wrap(err, "pg wal store rollback transaction")
		}
	}

	if err := s.conn.Close(ctx); err != nil {
		return errors.Wrap(err, "pg wal store close connection")
	}

	return nil
}

func scanRecordFromRows(rows pgx.Rows, rec *record.Record) error {
	if err := rows.Scan(&rec.Version, &rec.DownloaderID, &rec.DownloadURL, &rec.Status); err != nil {
		return err
	}

	return nil
}
