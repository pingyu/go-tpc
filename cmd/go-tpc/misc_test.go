package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/go-tpc/pkg/workload"
	"github.com/stretchr/testify/require"
)

type errorWorkloader struct {
	runErr     error
	checkErr   error
	onRun      func()
	runCount   int
	checkCount int
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
	w.runCount++
	if w.onRun != nil {
		w.onRun()
	}
	return w.runErr
}

func (w *errorWorkloader) Cleanup(context.Context, int) error {
	return nil
}

func (w *errorWorkloader) Check(context.Context, int) error {
	w.checkCount++
	return w.checkErr
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

func TestExecute_ignores_all_non_data_errors_when_ignore_error_is_enabled(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF},
		{name: "MySQL server error", err: &mysql.MySQLError{Number: 1062, Message: "duplicate entry"}},
		{name: "workload error", err: errors.New("result verification failed")},
		{name: "marker text without type", err: errors.New("[DATA ERROR] inconsistent warehouse totals")},
		{name: "joined ordinary errors", err: errors.Join(io.ErrUnexpectedEOF, errors.New("retry exhausted"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			configureExecuteTest(t, true)
			totalCount = 2
			w := &errorWorkloader{runErr: tt.err}

			// When
			err := execute(context.Background(), w, "run", 1, 0)

			// Then
			require.NoError(t, err)
			require.Equal(t, 2, w.runCount)
		})
	}
}

func TestExecute_never_ignores_typed_data_errors(t *testing.T) {
	dataErr := workload.NewDataError("inconsistent warehouse totals")
	tests := []struct {
		name string
		err  error
	}{
		{name: "typed data error", err: dataErr},
		{name: "wrapped typed data error", err: fmt.Errorf("transaction failed: %w", dataErr)},
		{
			name: "typed data error joined with network error",
			err:  errors.Join(dataErr, io.ErrUnexpectedEOF),
		},
		{
			name: "network operation wrapping typed data error",
			err:  &net.OpError{Op: "read", Net: "tcp", Err: dataErr},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			configureExecuteTest(t, true)
			w := &errorWorkloader{runErr: tt.err}

			// When
			err := execute(context.Background(), w, "run", 1, 0)

			// Then
			require.ErrorIs(t, err, tt.err)
			require.Equal(t, 1, w.runCount)
		})
	}
}

func TestExecute_returns_non_data_errors_when_ignore_error_is_disabled(t *testing.T) {
	// Given
	configureExecuteTest(t, false)
	w := &errorWorkloader{runErr: io.ErrUnexpectedEOF}

	// When
	err := execute(context.Background(), w, "run", 1, 0)

	// Then
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, 1, w.runCount)
}

func TestExecute_ignores_non_data_errors_during_check(t *testing.T) {
	// Given
	configureExecuteTest(t, true)
	w := &errorWorkloader{checkErr: fmt.Errorf("check query failed: %w", mysql.ErrInvalidConn)}

	// When
	err := execute(context.Background(), w, "check", 1, 0)

	// Then
	require.NoError(t, err)
	require.Equal(t, 1, w.checkCount)
}

func TestExecute_never_ignores_data_errors_during_check(t *testing.T) {
	// Given
	configureExecuteTest(t, true)
	dataErr := workload.NewDataError("inconsistent warehouse totals")
	w := &errorWorkloader{checkErr: dataErr}

	// When
	err := execute(context.Background(), w, "check", 1, 0)

	// Then
	require.ErrorIs(t, err, dataErr)
	require.Equal(t, 1, w.checkCount)
}

func TestExecute_never_ignores_typed_data_errors_when_deadline_fires(t *testing.T) {
	dataErr := workload.NewDataError("inconsistent warehouse totals")
	tests := []struct {
		name string
		err  error
	}{
		{name: "data error", err: dataErr},
		{
			name: "context joined with data error",
			err:  errors.Join(context.Canceled, dataErr),
		},
		{name: "data error wrapping context", err: fmt.Errorf("%w: %w", dataErr, context.Canceled)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configureExecuteTest(t, true)
			ctx, cancel := context.WithCancel(context.Background())
			w := &errorWorkloader{runErr: tt.err, onRun: cancel}

			err := execute(ctx, w, "run", 1, 0)

			require.ErrorIs(t, err, tt.err)
			require.Equal(t, 1, w.runCount)
		})
	}
}

func TestExecute_treats_pure_context_or_network_errors_as_deadline_completion(t *testing.T) {
	for _, runErr := range []error{context.Canceled, io.ErrUnexpectedEOF} {
		configureExecuteTest(t, false)
		ctx, cancel := context.WithCancel(context.Background())
		w := &errorWorkloader{runErr: runErr, onRun: cancel}

		err := execute(ctx, w, "run", 1, 0)

		require.NoError(t, err)
		require.Equal(t, 1, w.runCount)
	}
}

func TestExecuteWorkload_returns_nonignored_worker_error(t *testing.T) {
	configureExecuteTest(t, true)
	dataErr := workload.NewDataError("inconsistent warehouse totals")
	w := &errorWorkloader{runErr: dataErr}

	err := executeWorkload(context.Background(), w, 1, "run")

	require.ErrorIs(t, err, dataErr)
}

func configureExecuteTest(t *testing.T, ignore bool) {
	t.Helper()

	previousTotalCount := totalCount
	previousIgnoreError := ignoreError
	previousOutputInterval := outputInterval
	previousSilence := silence
	t.Cleanup(func() {
		totalCount = previousTotalCount
		ignoreError = previousIgnoreError
		outputInterval = previousOutputInterval
		silence = previousSilence
	})

	totalCount = 1
	ignoreError = ignore
	outputInterval = time.Hour
	silence = true
}
