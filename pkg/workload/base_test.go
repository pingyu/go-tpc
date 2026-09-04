package workload

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestNewTpcState_returns_connection_error(t *testing.T) {
	// Given
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:1)/test")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// When
	state, err := NewTpcState(ctx, db)

	// Then
	require.Nil(t, state)
	require.ErrorIs(t, err, context.Canceled)
}
