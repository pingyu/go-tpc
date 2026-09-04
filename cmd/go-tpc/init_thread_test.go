package main

import (
	"context"
	"testing"

	"github.com/pingcap/go-tpc/pkg/workload"
	"github.com/stretchr/testify/require"
)

type initErrorWorkloader struct {
	*errorWorkloader
	initErr error
}

func (w *initErrorWorkloader) InitThread(context.Context, int) (context.Context, error) {
	return nil, w.initErr
}

type prepareCheckInitErrorWorkloader struct {
	*errorWorkloader
	initErr  error
	initCall int
}

func (w *prepareCheckInitErrorWorkloader) InitThread(ctx context.Context, _ int) (context.Context, error) {
	w.initCall++
	if w.initCall > 1 {
		return nil, w.initErr
	}
	return ctx, nil
}

func TestExecute_returns_init_error_when_ignore_error_is_enabled(t *testing.T) {
	// Given
	configureExecuteTest(t, true)
	w := &initErrorWorkloader{errorWorkloader: &errorWorkloader{}, initErr: context.DeadlineExceeded}

	// When
	err := execute(context.Background(), w, "run", 1, 0)

	// Then
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestExecute_returns_operational_init_error_when_ignore_error_is_disabled(t *testing.T) {
	// Given
	configureExecuteTest(t, false)
	w := &initErrorWorkloader{errorWorkloader: &errorWorkloader{}, initErr: context.DeadlineExceeded}

	// When
	err := execute(context.Background(), w, "run", 1, 0)

	// Then
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestExecute_never_ignores_data_error_from_init(t *testing.T) {
	// Given
	configureExecuteTest(t, true)
	dataErr := workload.NewDataError("inconsistent warehouse totals")
	w := &initErrorWorkloader{errorWorkloader: &errorWorkloader{}, initErr: dataErr}

	// When
	err := execute(context.Background(), w, "run", 1, 0)

	// Then
	require.ErrorIs(t, err, dataErr)
}

func TestExecuteWorkload_returns_prepare_check_init_error_when_ignore_error_is_enabled(t *testing.T) {
	// Given
	configureExecuteTest(t, true)
	initErr := context.DeadlineExceeded
	w := &prepareCheckInitErrorWorkloader{errorWorkloader: &errorWorkloader{}, initErr: initErr}

	// When
	err := executeWorkload(context.Background(), w, 1, "prepare")

	// Then
	require.ErrorIs(t, err, initErr)
}
