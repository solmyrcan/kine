//go:build cgo
// +build cgo

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/rancher/kine/pkg/drivers/generic"
	"github.com/rancher/kine/pkg/logstructured"
	"github.com/rancher/kine/pkg/logstructured/sqllog"
	"github.com/rancher/kine/pkg/server"
	"github.com/sirupsen/logrus"

	// sqlite db driver
	_ "github.com/mattn/go-sqlite3"
)

var (
	schema = []string{
		`CREATE TABLE IF NOT EXISTS kine
			(
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT NOT NULL,
				created INTEGER,
				deleted INTEGER,
				create_revision INTEGER NOT NULL,
				prev_revision INTEGER,
				lease INTEGER,
				value BLOB,
				old_value BLOB
			)`,
		//`CREATE INDEX IF NOT EXISTS kine_name_index ON kine (name, id)`,
		//`CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON kine (name, prev_revision)`,
	}

	dropIndices = []string{
		`DROP INDEX IF EXISTS kine_name_index`,
		`DROP INDEX IF EXISTS kine_name_prev_revision_uindex`,
	}

	createIndices = []string{
		`CREATE INDEX IF NOT EXISTS kine_name_index ON kine (name, id)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON kine (prev_revision, name)`,
	}

	userVersionSQL    = `PRAGMA user_version`
	setUserVersionSQL = `PRAGMA user_version = 1`
	tableListSQL      = `SELECT COUNT(*) FROM PRAGMA table_list('table_name') WHERE name = 'key_value'`
)

func New(ctx context.Context, dataSourceName string) (server.Backend, error) {
	backend, _, err := NewVariant(ctx, "sqlite3", dataSourceName)
	return backend, err
}

func NewVariant(ctx context.Context, driverName, dataSourceName string) (server.Backend, *generic.Generic, error) {
	if dataSourceName == "" {
		if err := os.MkdirAll("./db", 0700); err != nil {
			return nil, nil, err
		}
		dataSourceName = "./db/state.db?_journal=WAL&cache=shared"
	}

	dialect, err := generic.Open(ctx, driverName, dataSourceName, "?", false)
	if err != nil {
		return nil, nil, err
	}

	dialect.LastInsertID = true
	dialect.TranslateErr = func(err error) error {
		if err, ok := err.(sqlite3.Error); ok && err.ExtendedCode == sqlite3.ErrConstraintUnique {
			return server.ErrKeyExists
		}
		return err
	}
	dialect.GetSizeSQL = `SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()`

	// Added but not used in our version - requires porting upstream compaction
	dialect.CompactSQL = `
		DELETE FROM kine AS kv
		WHERE
			kv.id IN (
				SELECT kp.prev_revision AS id
				FROM kine as kp
				WHERE
					kp.name != 'compact_rev_key' AND
					kp.prev_revision != 0 AND
					kp.id <= ?
				UNION
				SELECT kd.id AS id
				FROM kine AS id
				WHERE
					kd.deleted != 0 AND
					kd.id <= ?
			)`
	dialect.ErrCode = func(err error) string {
		if err == nil {
			return ""
		}
		if err, ok := err.(sqlite3.Error); ok {
			return fmt.Sprint(err.ExtendedCode)
		}
		return err.Error()
	}

	// this is the first SQL that will be executed on a new DB conn so
	// loop on failure here because in the case of dqlite it could still be initializing
	for i := 0; i < 300; i++ {
		err = setup(dialect.DB)
		if err == nil {
			break
		}
		logrus.Errorf("failed to setup db: %v", err)
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(time.Second):
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return nil, nil, errors.Wrap(err, "setup db")
	}

	//dialect.Migrate(context.Background())

	if err := checkMigrate(context.Background(), dialect); err != nil {
		return nil, nil, err
	}

	if err := dialect.Prepare(); err != nil {
		fmt.Println("Prepare() error", err)

		return nil, nil, err
	}

	return logstructured.New(sqllog.New(dialect)), dialect, nil
}

func setup(db *sql.DB) error {
	for _, stmt := range schema {
		_, err := db.Exec(stmt)
		if err != nil {
			return err
		}
	}

	return nil
}

// AlterTableIndices drops the given old table indices from the kine table and creates the given new ones.
func alterTableIndices(d *generic.Generic) error {
	if d.LockWrites {
		d.Lock()
		defer d.Unlock()
	}

	// Drop old indices
	for _, stmt := range dropIndices {
		_, err := d.DB.Exec(stmt)
		if err != nil {
			return err
		}
	}

	// Create new indices
	for _, stmt := range createIndices {
		_, err := d.DB.Exec(stmt)
		if err != nil {
			return err
		}
	}

	return nil
}

// CheckMigrate performs migration from an old key value table to the kine table only if
// the old key value table exists and migration has not been done already.
func checkMigrate(ctx context.Context, d *generic.Generic) error {
	row := d.DB.QueryRowContext(ctx, userVersionSQL)
	if row == nil {
		return fmt.Errorf("migrate: cannot find user_version pragma")
	}

	var userVersion int
	if err := row.Scan(&userVersion); err != nil {
		return err
	}
	// No need for migration
	if userVersion == 1 {
		return nil
	}

	row = d.DB.QueryRowContext(ctx, tableListSQL)

	var tableCount int
	if err := row.Scan(&tableCount); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("migrate: cannot get key_value table")
		}
		return err
	}

	// Perform migration from key_value table to kine table
	if tableCount > 0 {
		d.Migrate(ctx)
	}

	if err := alterTableIndices(d); err != nil {
		return nil
	}

	_, err := d.DB.ExecContext(ctx, setUserVersionSQL)
	if err != nil {
		return err
	}

	return nil
}
