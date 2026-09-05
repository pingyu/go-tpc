package main

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/pingcap/go-tpc/pkg/workload"
	"github.com/stretchr/testify/require"
)

func TestIsDataError_detects_only_typed_data_errors(t *testing.T) {
	dataErr := workload.NewDataError("inconsistent warehouse totals")
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "typed data error", err: dataErr, want: true},
		{name: "wrapped typed data error", err: fmt.Errorf("check failed: %w", dataErr), want: true},
		{
			name: "joined typed data error",
			err:  errors.Join(io.ErrUnexpectedEOF, dataErr),
			want: true,
		},
		{name: "marker text without type", err: errors.New("[DATA ERROR] inconsistent warehouse totals")},
		{name: "ordinary error", err: errors.New("result verification failed")},
		{name: "nil error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When
			got := isDataError(tt.err)

			// Then
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIgnoreCommandError_preserves_disabled_and_data_errors(t *testing.T) {
	ordinaryErr := errors.New("query failed")
	dataErr := workload.NewDataError("inconsistent warehouse totals")
	tests := []struct {
		name   string
		ignore bool
		err    error
		want   error
	}{
		{name: "enabled ordinary error", ignore: true, err: ordinaryErr},
		{name: "disabled ordinary error", err: ordinaryErr, want: ordinaryErr},
		{name: "enabled data error", ignore: true, err: dataErr, want: dataErr},
		{name: "nil error", ignore: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			configureExecuteTest(t, tt.ignore)

			// When
			err := ignoreCommandError(tt.err)

			// Then
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestSelectWorkerError_prefers_data_error_after_ordinary_error(t *testing.T) {
	ordinaryErr := errors.New("ordinary worker error")
	secondOrdinaryErr := errors.New("second ordinary worker error")
	dataErr := workload.NewDataError("inconsistent warehouse totals")
	tests := []struct {
		name        string
		firstError  error
		workerError error
		want        error
	}{
		{name: "first ordinary error", workerError: ordinaryErr, want: ordinaryErr},
		{name: "retain first ordinary error", firstError: ordinaryErr, workerError: secondOrdinaryErr, want: ordinaryErr},
		{name: "prefer later data error", firstError: ordinaryErr, workerError: dataErr, want: dataErr},
		{name: "retain first data error", firstError: dataErr, workerError: ordinaryErr, want: dataErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When
			got := selectWorkerError(tt.firstError, tt.workerError)

			// Then
			require.ErrorIs(t, got, tt.want)
		})
	}
}
