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
