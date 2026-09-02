package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

type delayedErrorWorkloader struct {
	errorWorkloader
	calls          atomic.Int32
	peerStarted    <-chan struct{}
	blockedStarted chan struct{}
	release        <-chan struct{}
	workerErr      error
}

func (w *delayedErrorWorkloader) Run(context.Context, int) error {
	if w.calls.Add(1) == 1 {
		<-w.peerStarted
		<-w.blockedStarted
		return w.workerErr
	}
	close(w.blockedStarted)
	<-w.release
	return nil
}

func TestWorkloadCommands_use_error_returning_handlers(t *testing.T) {
	// Given
	root := &cobra.Command{Use: "go-tpc"}
	registerTpch(root)
	registerRawsql(root)
	registerCHBenchmark(root)

	for _, path := range [][]string{
		{"tpch", "prepare"},
		{"tpch", "run"},
		{"tpch", "cleanup"},
		{"rawsql", "run"},
		{"ch", "prepare"},
		{"ch", "run"},
	} {
		// When
		cmd, _, err := root.Find(path)

		// Then
		require.NoError(t, err)
		require.NotNilf(t, cmd.RunE, "%v must return execution errors", path)
		require.Nilf(t, cmd.Run, "%v must not discard execution errors", path)
	}
}

func TestExecuteCHWorkloads_cancels_peer_before_local_teardown_finishes(t *testing.T) {
	// Given
	configureExecuteTest(t, false)
	totalCount = 2
	peerStarted := make(chan struct{})
	peerStopped := make(chan struct{})
	blockedStarted := make(chan struct{})
	release := make(chan struct{})
	workerErr := errors.New("OLAP worker failed")
	failing := &delayedErrorWorkloader{
		peerStarted:    peerStarted,
		blockedStarted: blockedStarted,
		release:        release,
		workerErr:      workerErr,
	}
	peer := &errorWorkloader{run: func(ctx context.Context) error {
		close(peerStarted)
		<-ctx.Done()
		close(peerStopped)
		return ctx.Err()
	}}
	done := make(chan error, 1)

	// When
	go func() {
		done <- executeCHWorkloads(context.Background(), []workLoaderSetting{
			{workLoader: failing, threads: 2},
			{workLoader: peer, threads: 1},
		})
	}()
	<-blockedStarted

	// Then
	select {
	case <-peerStopped:
	case <-time.After(time.Second):
		close(release)
		<-done
		require.FailNow(t, "peer workload was not canceled while the failing workload was still tearing down")
	}
	close(release)
	err := <-done
	require.ErrorIs(t, err, workerErr)
}

func TestExecuteCHWorkloads_preserves_initiating_error_when_canceled_peer_returns_another_error(t *testing.T) {
	// Given
	configureExecuteTest(t, false)
	totalCount = 2
	peerStarted := make(chan struct{})
	peerStopped := make(chan struct{})
	blockedStarted := make(chan struct{})
	release := make(chan struct{})
	workerErr := errors.New("initiating worker failed")
	peerErr := errors.New("peer cancellation artifact")
	failing := &delayedErrorWorkloader{
		peerStarted:    peerStarted,
		blockedStarted: blockedStarted,
		release:        release,
		workerErr:      workerErr,
	}
	peer := &errorWorkloader{run: func(ctx context.Context) error {
		close(peerStarted)
		<-ctx.Done()
		close(peerStopped)
		return peerErr
	}}
	done := make(chan error, 1)

	// When
	go func() {
		done <- executeCHWorkloads(context.Background(), []workLoaderSetting{
			{workLoader: failing, threads: 2},
			{workLoader: peer, threads: 1},
		})
	}()
	<-blockedStarted
	<-peerStopped
	<-time.After(100 * time.Millisecond)
	close(release)
	err := <-done

	// Then
	require.ErrorIs(t, err, workerErr)
	require.NotErrorIs(t, err, peerErr)
}

func TestExecuteCHWorkloads_returns_worker_error_and_cancels_peer(t *testing.T) {
	// Given
	configureExecuteTest(t, false)
	peerStarted := make(chan struct{})
	peerStopped := make(chan struct{})
	workerErr := errors.New("OLAP worker failed")
	failing := &errorWorkloader{run: func(context.Context) error {
		<-peerStarted
		return workerErr
	}}
	peer := &errorWorkloader{run: func(ctx context.Context) error {
		close(peerStarted)
		<-ctx.Done()
		close(peerStopped)
		return ctx.Err()
	}}

	// When
	err := executeCHWorkloads(context.Background(), []workLoaderSetting{
		{workLoader: failing, threads: 1},
		{workLoader: peer, threads: 1},
	})

	// Then
	require.ErrorIs(t, err, workerErr)
	requireClosed(t, peerStopped)
}

func requireClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	default:
		require.Fail(t, "channel is not closed")
	}
}
