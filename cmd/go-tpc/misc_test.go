package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/go-tpc/pkg/workload"
	"github.com/stretchr/testify/require"
)

type errorWorkloader struct {
	name            string
	runErr          error
	prepareErr      error
	prepareCheckErr error
	checkErr        error
	execErr         error
	exec            func(context.Context) error
	onRun           func()
	run             func(context.Context) error
	runCount        atomic.Int32
	checkCount      int
}

func (w *errorWorkloader) Name() string {
	if w.name != "" {
		return w.name
	}
	return "test"
}

func (w *errorWorkloader) InitThread(ctx context.Context, _ int) (context.Context, error) {
	return ctx, nil
}

func (w *errorWorkloader) CleanupThread(context.Context, int) {}

func (w *errorWorkloader) Prepare(context.Context, int) error {
	return w.prepareErr
}

func (w *errorWorkloader) CheckPrepare(context.Context, int) error {
	return w.prepareCheckErr
}

func (w *errorWorkloader) Run(ctx context.Context, _ int) error {
	w.runCount.Add(1)
	if w.onRun != nil {
		w.onRun()
	}
	if w.run != nil {
		return w.run(ctx)
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
	return w.execErr
}

func (w *errorWorkloader) ExecContext(ctx context.Context, _ string) error {
	if w.exec != nil {
		return w.exec(ctx)
	}
	return w.execErr
}

func TestExecute_ignores_non_data_errors_per_iteration_when_enabled(t *testing.T) {
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
			totalCount = 3
			w := &errorWorkloader{runErr: tt.err}

			// When
			err := execute(context.Background(), w, "run", 1, 0)

			// Then
			require.NoError(t, err)
			require.Equal(t, int32(3), w.runCount.Load())
		})
	}
}

func TestExecute_reports_ignored_errors_and_continues(t *testing.T) {
	// Given
	configureExecuteTest(t, true)
	silence = false
	runErr := errors.New("retryable query failure")
	w := &errorWorkloader{runErr: runErr}
	var err error

	// When
	output := captureStdout(t, func() {
		err = execute(context.Background(), w, "run", 1, 0)
	})

	// Then
	require.NoError(t, err)
	require.Contains(t, output, "execute run failed")
	require.Contains(t, output, runErr.Error())
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
			require.Equal(t, int32(1), w.runCount.Load())
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
	require.Equal(t, int32(1), w.runCount.Load())
}

func TestExecute_returns_non_data_check_errors_to_command_boundary(t *testing.T) {
	// Given
	configureExecuteTest(t, true)
	w := &errorWorkloader{checkErr: fmt.Errorf("check query failed: %w", mysql.ErrInvalidConn)}

	// When
	err := execute(context.Background(), w, "check", 1, 0)

	// Then
	require.ErrorIs(t, err, w.checkErr)
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
			require.Equal(t, int32(1), w.runCount.Load())
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
		require.Equal(t, int32(1), w.runCount.Load())
	}
}

func TestExecute_reports_cancellation_without_calling_it_timeout(t *testing.T) {
	// Given
	configureExecuteTest(t, false)
	silence = false
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &errorWorkloader{}
	var err error

	// When
	output := captureStdout(t, func() {
		err = execute(ctx, w, "run", 1, 0)
	})

	// Then
	require.NoError(t, err)
	require.Contains(t, output, "cancellation")
	require.NotContains(t, output, "timeout")
}

func TestExecuteWorkload_returns_nonignored_worker_error(t *testing.T) {
	configureExecuteTest(t, true)
	dataErr := workload.NewDataError("inconsistent warehouse totals")
	w := &errorWorkloader{runErr: dataErr}

	err := executeWorkload(context.Background(), w, 1, "run")

	require.ErrorIs(t, err, dataErr)
}

func TestExecuteConfiguredWorkload_prefers_data_error_over_ordinary_error(t *testing.T) {
	// Given
	configureExecuteTest(t, false)
	ordinaryErr := errors.New("ordinary worker error")
	dataErr := workload.NewDataError("inconsistent warehouse totals")
	started := make(chan struct{}, 2)
	releaseOrdinary := make(chan struct{})
	releaseData := make(chan struct{})
	ordinaryObserved := make(chan struct{})
	var calls atomic.Int32
	w := &errorWorkloader{run: func(context.Context) error {
		call := calls.Add(1)
		started <- struct{}{}
		if call == 1 {
			<-releaseOrdinary
			return ordinaryErr
		}
		<-releaseData
		return dataErr
	}}
	done := make(chan error, 1)
	go func() {
		done <- executeConfiguredWorkload(context.Background(), workLoaderSetting{
			workLoader: w,
			threads:    2,
			onWorkerError: func(err error) {
				if errors.Is(err, ordinaryErr) {
					close(ordinaryObserved)
				}
			},
		}, "run")
	}()
	<-started
	<-started
	close(releaseOrdinary)
	<-ordinaryObserved
	close(releaseData)

	// When
	err := <-done

	// Then
	require.ErrorIs(t, err, dataErr)
}

func TestExecuteWorkload_returns_prepare_worker_error_when_ignore_error_is_enabled(t *testing.T) {
	// Given
	configureExecuteTest(t, true)
	prepareErr := errors.New("prepare failed")
	w := &errorWorkloader{prepareErr: prepareErr}

	// When
	err := executeWorkload(context.Background(), w, 1, "prepare")

	// Then
	require.ErrorIs(t, err, prepareErr)
}

func TestExecuteWorkload_returns_post_prepare_check_error_when_ignore_error_is_enabled(t *testing.T) {
	// Given
	configureExecuteTest(t, true)
	checkErr := errors.New("prepared data is inconsistent")
	w := &errorWorkloader{prepareCheckErr: checkErr}

	// When
	err := executeWorkload(context.Background(), w, 1, "prepare")

	// Then
	require.ErrorIs(t, err, checkErr)
}

func TestExecuteWorkload_returns_view_setup_error(t *testing.T) {
	// Given
	configureExecuteTest(t, false)
	viewErr := errors.New("create view failed")
	w := &errorWorkloader{name: "tpch", execErr: viewErr}

	// When
	err := executeWorkload(context.Background(), w, 1, "run")

	// Then
	require.ErrorIs(t, err, viewErr)
}

func TestExecuteWorkload_passes_cancellation_to_view_setup(t *testing.T) {
	// Given
	configureExecuteTest(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &errorWorkloader{name: "tpch", exec: func(ctx context.Context) error {
		return ctx.Err()
	}}

	// When
	err := executeWorkload(ctx, w, 1, "run")

	// Then
	require.ErrorIs(t, err, context.Canceled)
}

func configureExecuteTest(t *testing.T, ignore bool) {
	t.Helper()

	previousTotalCount := totalCount
	previousIgnoreError := ignoreError
	previousOutputInterval := outputInterval
	previousSilence := silence
	previousDropData := dropData
	t.Cleanup(func() {
		totalCount = previousTotalCount
		ignoreError = previousIgnoreError
		outputInterval = previousOutputInterval
		silence = previousSilence
		dropData = previousDropData
	})

	totalCount = 1
	ignoreError = ignore
	outputInterval = time.Hour
	silence = true
	dropData = false
}

func captureStdout(t *testing.T, run func()) string {
	t.Helper()

	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	original := os.Stdout
	os.Stdout = writer
	t.Cleanup(func() {
		os.Stdout = original
		reader.Close()
		writer.Close()
	})

	run()
	os.Stdout = original
	require.NoError(t, writer.Close())
	output, err := io.ReadAll(reader)
	require.NoError(t, err)
	return string(output)
}
