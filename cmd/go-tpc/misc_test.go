package main

import (
	"context"
	sqldrv "database/sql/driver"
	"errors"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

type errorWorkloader struct {
	err error
}

func (w *errorWorkloader) Name() string {
	return "test"
}

func (w *errorWorkloader) InitThread(ctx context.Context, _ int) context.Context {
	return ctx
}

func (w *errorWorkloader) CleanupThread(context.Context, int) {}

func (w *errorWorkloader) Prepare(context.Context, int) error {
	return nil
}

func (w *errorWorkloader) CheckPrepare(context.Context, int) error {
	return nil
}

func (w *errorWorkloader) Run(context.Context, int) error {
	return w.err
}

func (w *errorWorkloader) Cleanup(context.Context, int) error {
	return nil
}

func (w *errorWorkloader) Check(context.Context, int) error {
	return nil
}

func (w *errorWorkloader) OutputStats(bool) {}

func (w *errorWorkloader) DBName() string {
	return "test"
}

func (w *errorWorkloader) IsPlanReplayerDumpEnabled() bool {
	return false
}

func (w *errorWorkloader) PreparePlanReplayerDump() error {
	return nil
}

func (w *errorWorkloader) FinishPlanReplayerDump() error {
	return nil
}

func (w *errorWorkloader) Exec(string) error {
	return nil
}

func TestExecute_tolerates_network_errors_only_when_ignore_error_is_enabled(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		ignore      bool
		wantIgnored bool
	}{
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF, ignore: true, wantIgnored: true},
		{name: "bad connection", err: sqldrv.ErrBadConn, ignore: true, wantIgnored: true},
		{name: "invalid MySQL connection", err: mysql.ErrInvalidConn, ignore: true, wantIgnored: true},
		{
			name: "TCP connection reset",
			err: &net.OpError{
				Op:  "write",
				Net: "tcp",
				Err: syscall.ECONNRESET,
			},
			ignore:      true,
			wantIgnored: true,
		},
		{name: "unexpected EOF without flag", err: io.ErrUnexpectedEOF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			configureExecuteTest(t, tt.ignore)
			w := &errorWorkloader{err: tt.err}

			// When
			err := execute(context.Background(), w, "run", 1, 0)

			// Then
			if tt.wantIgnored {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tt.err)
		})
	}
}

func TestExecute_never_tolerates_data_or_unknown_errors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "MySQL server error", err: &mysql.MySQLError{Number: 1062, Message: "duplicate entry"}},
		{name: "explicit data error", err: errors.New("[DATA ERROR] inconsistent warehouse totals")},
		{
			name: "data error joined with network error",
			err: errors.Join(
				errors.New("[DATA ERROR] inconsistent warehouse totals"),
				io.ErrUnexpectedEOF,
			),
		},
		{name: "unknown error", err: errors.New("result verification failed")},
		{name: "network-looking text without typed cause", err: errors.New("unexpected EOF")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			configureExecuteTest(t, true)
			w := &errorWorkloader{err: tt.err}

			// When
			err := execute(context.Background(), w, "run", 1, 0)

			// Then
			require.ErrorIs(t, err, tt.err)
		})
	}
}

func configureExecuteTest(t *testing.T, ignore bool) {
	t.Helper()

	previousTotalCount := totalCount
	previousIgnoreError := ignoreError
	previousSilence := silence
	t.Cleanup(func() {
		totalCount = previousTotalCount
		ignoreError = previousIgnoreError
		silence = previousSilence
	})

	totalCount = 1
	ignoreError = ignore
	silence = true
}
