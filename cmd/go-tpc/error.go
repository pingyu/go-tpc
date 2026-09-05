package main

import (
	"context"
	sqldrv "database/sql/driver"
	"errors"
	"io"
	"net"
	"os"
	"syscall"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/go-tpc/pkg/workload"
)

func shouldIgnoreError(err error) bool {
	return err != nil && ignoreError && !isDataError(err)
}

func ignoreCommandError(err error) error {
	if shouldIgnoreError(err) {
		return nil
	}
	return err
}

func isPureContextTermination(err error) bool {
	if err == nil || isDataError(err) {
		return false
	}
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func isDataError(err error) bool {
	var dataErr *workload.DataError
	return errors.As(err, &dataErr)
}

func selectWorkerError(firstError, workerError error) error {
	if firstError == nil || (!isDataError(firstError) && isDataError(workerError)) {
		return workerError
	}
	return firstError
}

func isToleratedNetworkError(err error) bool {
	if err == nil || isDataError(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		return false
	}

	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrShortWrite) ||
		errors.Is(err, os.ErrDeadlineExceeded) ||
		errors.Is(err, sqldrv.ErrBadConn) ||
		errors.Is(err, mysql.ErrInvalidConn) {
		return true
	}

	var operationErr *net.OpError
	if errors.As(err, &operationErr) {
		return isToleratedNetworkError(operationErr.Err)
	}

	return errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETRESET) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ETIMEDOUT)
}
