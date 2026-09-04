package tpcc

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoveredError_preserves_error_cause(t *testing.T) {
	err := recoveredError("transaction panic", io.ErrUnexpectedEOF)

	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestRecoveredError_formats_non_error_values(t *testing.T) {
	err := recoveredError("transaction panic", "broken invariant")

	require.EqualError(t, err, "transaction panic: broken invariant")
	require.False(t, errors.Is(err, io.ErrUnexpectedEOF))
}

func TestNewWorkloader_returns_error_when_database_is_unavailable(t *testing.T) {
	// Given
	cfg := &Config{}

	// When
	w, err := NewWorkloader(nil, cfg)

	// Then
	require.Nil(t, w)
	require.EqualError(t, err, "failed to connect to database when loading data")
}
