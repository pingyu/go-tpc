package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockingPrepareWorkloader struct {
	errorWorkloader
	peerStarted  chan struct{}
	peerCanceled chan struct{}
	releasePeer  <-chan struct{}
	workerErr    error
}

func (w *blockingPrepareWorkloader) Prepare(ctx context.Context, threadID int) error {
	if threadID == 0 {
		<-w.peerStarted
		return w.workerErr
	}

	close(w.peerStarted)
	select {
	case <-ctx.Done():
		close(w.peerCanceled)
		return ctx.Err()
	case <-w.releasePeer:
		return nil
	}
}

type workerErrorOrderWorkloader struct {
	errorWorkloader
	peerStarted  chan struct{}
	workerErr    error
	initiatorCtx context.Context
}

func (w *workerErrorOrderWorkloader) InitThread(ctx context.Context, threadID int) (context.Context, error) {
	if threadID == 0 {
		w.initiatorCtx = ctx
	}
	return ctx, nil
}

func (w *workerErrorOrderWorkloader) Run(ctx context.Context, threadID int) error {
	if threadID == 0 {
		<-w.peerStarted
		return w.workerErr
	}

	close(w.peerStarted)
	<-ctx.Done()
	return ctx.Err()
}

func TestNewWorkloadContext_omits_non_run_deadline_and_keeps_parent_cancellation(t *testing.T) {
	// Given
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := newWorkloadContext(parent, "prepare", time.Nanosecond)
	t.Cleanup(cancel)

	// When
	_, hasDeadline := ctx.Deadline()
	cancelParent()

	// Then
	require.False(t, hasDeadline)
	select {
	case <-ctx.Done():
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	case <-time.After(time.Second):
		require.FailNow(t, "non-run context did not preserve parent cancellation")
	}
}

func TestExecuteWorkload_cancels_blocked_prepare_peer_on_worker_error(t *testing.T) {
	// Given
	configureExecuteTest(t, false)
	peerStarted := make(chan struct{})
	peerCanceled := make(chan struct{})
	releasePeer := make(chan struct{})
	workerErr := errors.New("prepare worker failed")
	w := &blockingPrepareWorkloader{
		peerStarted:  peerStarted,
		peerCanceled: peerCanceled,
		releasePeer:  releasePeer,
		workerErr:    workerErr,
	}
	done := make(chan error, 1)

	// When
	go func() {
		done <- executeWorkload(context.Background(), w, 2, "prepare")
	}()

	// Then
	select {
	case <-peerCanceled:
	case <-time.After(time.Second):
		close(releasePeer)
		err := <-done
		require.FailNow(t, "blocked prepare peer was not canceled", "executeWorkload returned %v", err)
	}
	require.ErrorIs(t, <-done, workerErr)
}

func TestExecuteConfiguredWorkload_reports_worker_error_before_canceling_peers(t *testing.T) {
	// Given
	configureExecuteTest(t, false)
	peerStarted := make(chan struct{})
	workerErr := errors.New("initiating worker failed")
	w := &workerErrorOrderWorkloader{peerStarted: peerStarted, workerErr: workerErr}
	callbackContextErrors := make(chan error, 1)
	setting := workLoaderSetting{
		workLoader: w,
		threads:    2,
		onWorkerError: func(err error) {
			if errors.Is(err, workerErr) {
				callbackContextErrors <- w.initiatorCtx.Err()
			}
		},
	}

	// When
	err := executeConfiguredWorkload(context.Background(), setting, "run")

	// Then
	require.ErrorIs(t, err, workerErr)
	require.NoError(t, <-callbackContextErrors)
}
