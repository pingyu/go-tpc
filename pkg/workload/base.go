package workload

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/pingcap/go-tpc/pkg/util"
)

// TpcState saves state for each thread
type TpcState struct {
	DB   *sql.DB
	Conn *sql.Conn

	R *rand.Rand

	Buf *util.BufAllocator
}

func (t *TpcState) RefreshConn(ctx context.Context) error {
	if t.Conn != nil {
		t.Conn.Close()
	}
	conn, err := t.DB.Conn(ctx)
	if err != nil {
		return err
	}
	t.Conn = conn
	return nil
}

// NewTpcState creates a base TpcState
func NewTpcState(ctx context.Context, db *sql.DB) (*TpcState, error) {
	var conn *sql.Conn
	if db != nil {
		var err error
		conn, err = db.Conn(ctx)
		if err != nil {
			return nil, fmt.Errorf("get database connection: %w", err)
		}
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	s := &TpcState{
		DB:   db,
		Conn: conn,
		R:    r,
		Buf:  util.NewBufAllocator(),
	}
	return s, nil
}
