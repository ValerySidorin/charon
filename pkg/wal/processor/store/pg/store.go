package pg

import (
	"context"
	"database/sql"

	"github.com/ValerySidorin/charon/pkg/wal/config/pg"
	"github.com/ValerySidorin/charon/pkg/wal/processor/record"
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
		return nil, errors.Wrap(err, "pg processor wal store init conn")
	}

	q := `create table if not exists public.processor_wal (
		id bigserial primary key,
		version integer not null, 
		processor_id text null, 
		obj_name text not null, 
		type text not null, 
		status text not null,
		constraint unique_obj_name unique (obj_name));`
	if _, err := conn.Exec(ctx, q); err != nil {
		return nil, errors.Wrap(err, "pg wal store init processor wal table")
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
		return errors.Wrap(err, "pg processor wal store begin transation")
	}
	s.currTx = t
	return nil
}

func (s *Store) RollbackTransaction(ctx context.Context) error {
	if s.currTx == nil {
		return nil
	}

	if err := s.currTx.Rollback(ctx); err != nil {
		return errors.Wrap(err, "pg processor wal store rollback transaction")
	}

	s.currTx = nil
	return nil
}

func (s *Store) CommitTransaction(ctx context.Context) error {
	if s.currTx == nil {
		return nil
	}

	if err := s.currTx.Commit(ctx); err != nil {
		return errors.Wrap(err, "pg processor wal store commit transaction")
	}

	s.currTx = nil
	return nil
}

func (s *Store) LockAllRecords(ctx context.Context) error {
	if s.currTx == nil {
		return errors.New("pg processor wal store can't lock table without transaction")
	}

	q := "lock table processor_wal in access exclusive mode;"
	_, err := s.currTx.Exec(ctx, q)
	if err != nil {
		return errors.Wrap(err, "pg processor wal store lock table")
	}

	return nil
}

func (s *Store) MergeRecords(ctx context.Context, recs []*record.Record) error {
	stmt := "INSERT INTO public.processor_wal (version, processor_id, obj_name, type, status) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (obj_name) DO NOTHING;"
	if s.currTx != nil {
		_, err := s.currTx.Prepare(ctx, "merge_records", stmt)
		if err != nil {
			return errors.Wrap(err, "pg processor wal merge records")
		}

		for _, rec := range recs {
			_, err := s.currTx.Exec(ctx, "merge_records", rec.Version, rec.ProcessorID, rec.ObjName, rec.Type, rec.Status)
			if err != nil {
				return errors.Wrap(err, "pg processor wal insert inside merge")
			}
		}

		return nil
	}

	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "pg processor wal begin tran")
	}
	defer tx.Rollback(ctx)

	_, err = tx.Prepare(ctx, "merge_records", stmt)
	if err != nil {
		return errors.Wrap(err, "pg processor wal merge records")
	}

	for _, rec := range recs {
		_, err := s.currTx.Exec(ctx, "merge_records", rec.Version, rec.ProcessorID, rec.ObjName, rec.Type, rec.Status)
		if err != nil {
			return errors.Wrap(err, "pg processor wal insert inside merge")
		}
	}

	return nil
}

func (s *Store) UpdateRecord(ctx context.Context, rec *record.Record) error {
	q := `update processor_wal
	set version = $1,
	processor_id = $2,
	type = $3
	status = $4
	where obj_name = $5;`

	if s.currTx != nil {
		_, err := s.currTx.Exec(ctx, q, rec.Version, rec.ProcessorID, rec.Type, rec.Status, rec.ObjName)
		if err != nil {
			return errors.Wrap(err, "pg wal store update record")
		}

		return nil
	}

	_, err := s.conn.Exec(ctx, q, rec.Version, rec.ProcessorID, rec.Type, rec.Status, rec.ObjName)
	if err != nil {
		return errors.Wrap(err, "pg wal store update record")
	}

	return nil
}

func (s *Store) GetRecordsByVersion(ctx context.Context, version int) ([]*record.Record, error) {
	q := "select version, processor_id, obj_name, type, status from processor_wal where version = $1;"
	recs := make([]*record.Record, 0)

	if s.currTx != nil {
		rows, err := s.currTx.Query(ctx, q, version)
		if err != nil {
			return nil, errors.Wrap(err, "pg wal store query records by version")
		}
		defer rows.Close()

		for rows.Next() {
			rec := record.Record{}
			if err := scanRecordFromRows(rows, &rec); err != nil {
				return nil, errors.Wrap(err, "pg wal store scan records by version")
			}
			recs = append(recs, &rec)
		}

		return recs, nil
	}

	rows, err := s.conn.Query(ctx, q)
	if err != nil {
		return nil, errors.Wrap(err, "pg wal store query records by version")
	}
	defer rows.Close()

	for rows.Next() {
		rec := record.Record{}
		if err := scanRecordFromRows(rows, &rec); err != nil {
			return nil, errors.Wrap(err, "pg wal store scan records by version")
		}
		recs = append(recs, &rec)
	}

	return recs, nil
}

func (s *Store) GetLastVersion(ctx context.Context) (int, error) {
	q := `select max(version) from processor_wal;`
	var id sql.NullInt64

	if s.currTx != nil {
		err := s.currTx.QueryRow(ctx, q).Scan(&id)
		if err != nil {
			return 0, errors.Wrap(err, "pg processor wal store get last version")
		}

		if !id.Valid {
			return 0, nil
		}

		return int(id.Int64), nil
	}

	err := s.conn.QueryRow(ctx, q).Scan(&id)
	if err != nil {
		return 0, errors.Wrap(err, "pg processor wal store get last version")
	}

	if !id.Valid {
		return 0, nil
	}

	return int(id.Int64), nil
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
	if err := rows.Scan(nil, &rec.Version, &rec.ProcessorID, &rec.ObjName, &rec.Type, &rec.Status); err != nil {
		return err
	}

	return nil
}