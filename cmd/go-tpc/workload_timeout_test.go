package main

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"testing"

	"github.com/pingcap/go-tpc/ch"
	"github.com/pingcap/go-tpc/pkg/workload"
	"github.com/pingcap/go-tpc/rawsql"
	"github.com/pingcap/go-tpc/tpcc"
	"github.com/pingcap/go-tpc/tpch"
	"github.com/stretchr/testify/require"
)

type cancelingConnector struct {
	cancel     context.CancelFunc
	connectErr error
	execErr    error
}

func (c cancelingConnector) Connect(context.Context) (sqldriver.Conn, error) {
	if c.connectErr != nil {
		return nil, c.connectErr
	}
	return &cancelingConn{cancel: c.cancel, execErr: c.execErr}, nil
}

func (cancelingConnector) Driver() sqldriver.Driver {
	return cancelingDriver{}
}

type cancelingDriver struct{}

func (cancelingDriver) Open(string) (sqldriver.Conn, error) {
	return nil, sqldriver.ErrSkip
}

type cancelingConn struct {
	cancel  context.CancelFunc
	execErr error
}

func (*cancelingConn) Prepare(string) (sqldriver.Stmt, error) {
	return nil, sqldriver.ErrSkip
}

func (*cancelingConn) Close() error {
	return nil
}

func (*cancelingConn) Begin() (sqldriver.Tx, error) {
	return nil, sqldriver.ErrSkip
}

func (*cancelingConn) Ping(context.Context) error {
	return nil
}

func (c *cancelingConn) QueryContext(context.Context, string, []sqldriver.NamedValue) (sqldriver.Rows, error) {
	if c.cancel != nil {
		c.cancel()
	}
	return nil, context.Canceled
}

func (c *cancelingConn) ExecContext(context.Context, string, []sqldriver.NamedValue) (sqldriver.Result, error) {
	return nil, c.execErr
}

func TestExecute_treats_wrapped_workload_context_errors_as_timeout_completion(t *testing.T) {
	tests := []struct {
		name string
		new  func(*sql.DB) workload.Workloader
	}{
		{
			name: "rawsql",
			new: func(db *sql.DB) workload.Workloader {
				return rawsql.NewWorkloader(db, &rawsql.Config{
					QueryNames: []string{"q1"},
					Queries:    map[string]string{"q1": "SELECT 1"},
				})
			},
		},
		{
			name: "tpch",
			new: func(db *sql.DB) workload.Workloader {
				return tpch.NewWorkloader(db, &tpch.Config{
					Driver:     mysqlDriver,
					QueryNames: []string{"q1"},
				})
			},
		},
		{
			name: "ch",
			new: func(db *sql.DB) workload.Workloader {
				return ch.NewWorkloader(db, &ch.Config{
					Driver:     mysqlDriver,
					QueryNames: []string{"q1"},
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			configureExecuteTest(t, false)
			ctx, cancel := context.WithCancel(context.Background())
			db := sql.OpenDB(cancelingConnector{cancel: cancel})
			t.Cleanup(func() {
				cancel()
				require.NoError(t, db.Close())
			})

			// When
			err := execute(ctx, tt.new(db), "run", 1, 0)

			// Then
			require.NoError(t, err)
		})
	}
}

func TestExecuteWorkload_returns_tpcc_ddl_error_without_blocking_peers(t *testing.T) {
	tests := []struct {
		name string
		new  func(*testing.T, *sql.DB) (workload.Workloader, error)
	}{
		{
			name: "database workload",
			new: func(_ *testing.T, db *sql.DB) (workload.Workloader, error) {
				return tpcc.NewWorkloader(db, &tpcc.Config{
					Driver:        mysqlDriver,
					Threads:       2,
					Warehouses:    1,
					Parts:         1,
					PartitionType: tpcc.PartitionTypeHash,
				})
			},
		},
		{
			name: "CSV workload",
			new: func(t *testing.T, db *sql.DB) (workload.Workloader, error) {
				return tpcc.NewCSVWorkloader(db, &tpcc.Config{
					Driver:        mysqlDriver,
					Threads:       2,
					Warehouses:    1,
					Parts:         1,
					PartitionType: tpcc.PartitionTypeHash,
					OutputDir:     t.TempDir(),
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			configureExecuteTest(t, false)
			ddlErr := errors.New("create table failed")
			db := sql.OpenDB(cancelingConnector{execErr: ddlErr})
			t.Cleanup(func() {
				require.NoError(t, db.Close())
			})
			w, err := tt.new(t, db)
			require.NoError(t, err)

			// When
			err = executeWorkload(context.Background(), w, 2, "prepare")

			// Then
			require.ErrorIs(t, err, ddlErr)
		})
	}
}

func TestExecuteWorkload_returns_tpcc_drop_error_without_starting_prepare(t *testing.T) {
	// Given
	configureExecuteTest(t, false)
	dropData = true
	dropErr := errors.New("drop table failed")
	db := sql.OpenDB(cancelingConnector{execErr: dropErr})
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	w, err := tpcc.NewWorkloader(db, &tpcc.Config{
		Driver:        mysqlDriver,
		Threads:       2,
		Warehouses:    1,
		Parts:         1,
		PartitionType: tpcc.PartitionTypeHash,
	})
	require.NoError(t, err)

	// When
	err = executeWorkload(context.Background(), w, 2, "prepare")

	// Then
	require.ErrorIs(t, err, dropErr)
}

func TestExecuteWorkload_returns_ch_connection_error_during_view_setup(t *testing.T) {
	// Given
	configureExecuteTest(t, false)
	connectErr := errors.New("connect failed")
	db := sql.OpenDB(cancelingConnector{connectErr: connectErr})
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	w := ch.NewWorkloader(db, &ch.Config{QueryNames: []string{"q1"}})

	// When
	err := executeWorkload(context.Background(), w, 1, "run")

	// Then
	require.ErrorIs(t, err, connectErr)
}
