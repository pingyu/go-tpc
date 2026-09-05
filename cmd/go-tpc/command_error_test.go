package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/go-tpc/pkg/workload"
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

func TestTpccCheckCommand_ignores_workloader_init_error_when_ignore_error_is_enabled(t *testing.T) {
	// Given
	configureExecuteTest(t, true)
	configureExecuteTpccUnavailableDBTest(t)
	cmd := registeredCommand(t, registerTpcc, "tpcc", "check")

	// When
	err := cmd.RunE(cmd, nil)

	// Then
	require.NoError(t, err)
}

func TestTpccRunCommand_ignores_workloader_init_error_when_ignore_error_is_enabled(t *testing.T) {
	// Given
	configureExecuteTest(t, true)
	configureExecuteTpccUnavailableDBTest(t)
	cmd := registeredCommand(t, registerTpcc, "tpcc", "run")

	// When
	err := cmd.RunE(cmd, nil)

	// Then
	require.NoError(t, err)
}

func TestRawsqlRunCommand_ignores_validation_error_when_ignore_error_is_enabled(t *testing.T) {
	// Given
	configureExecuteTest(t, true)
	previousQueryFiles := queryFiles
	t.Cleanup(func() {
		queryFiles = previousQueryFiles
	})
	queryFiles = ""
	cmd := registeredCommand(t, registerRawsql, "rawsql", "run")

	// When
	err := cmd.RunE(cmd, nil)

	// Then
	require.NoError(t, err)
}

func TestRunCommands_handle_errors_at_the_correct_boundary(t *testing.T) {
	if os.Getenv("GO_TPC_TEST_CHILD") == "1" {
		workloadName := os.Getenv("GO_TPC_TEST_WORKLOAD")
		ignoreError = true
		targets = []string{"127.0.0.1:0"}
		hosts = []string{"127.0.0.1"}
		ports = []int{0}
		driver = os.Getenv("GO_TPC_TEST_DRIVER")
		threads = 1
		acThreads = 1
		globalCtx = context.Background()
		apHosts = hosts
		apPorts = ports

		var register func(*cobra.Command)
		var commandPath []string
		switch workloadName {
		case "tpcc":
			register = registerTpcc
			commandPath = []string{"tpcc", "run"}
		case "tpch":
			register = registerTpch
			commandPath = []string{"tpch", "run"}
		case "ch":
			register = registerCHBenchmark
			commandPath = []string{"ch", "run"}
		case "rawsql":
			register = registerRawsql
			commandPath = []string{"rawsql", "run"}
		default:
			os.Exit(2)
		}

		root := &cobra.Command{Use: "go-tpc"}
		register(root)
		if workloadName == "rawsql" {
			queryFiles = "unused.sql"
		}
		cmd, _, err := root.Find(commandPath)
		if err == nil && cmd.PreRun != nil {
			cmd.PreRun(cmd, nil)
		}
		if err == nil {
			err = cmd.RunE(cmd, nil)
		}
		if err != nil {
			os.Exit(2)
		}
		return
	}

	for _, failure := range []struct {
		name      string
		driver    string
		wantPanic bool
	}{
		{name: "unavailable database", driver: mysqlDriver},
		{name: "invalid driver", driver: "invalid", wantPanic: true},
	} {
		t.Run(failure.name, func(t *testing.T) {
			for _, workloadName := range []string{"tpcc", "tpch", "ch", "rawsql"} {
				t.Run(workloadName, func(t *testing.T) {
					cmd := exec.Command(os.Args[0], "-test.run=^TestRunCommands_handle_errors_at_the_correct_boundary$")
					cmd.Env = append(os.Environ(),
						"GO_TPC_TEST_CHILD=1",
						"GO_TPC_TEST_WORKLOAD="+workloadName,
						"GO_TPC_TEST_DRIVER="+failure.driver,
					)
					output, err := cmd.CombinedOutput()
					if failure.wantPanic {
						require.Errorf(t, err, "%s output: %s", workloadName, output)
						require.Contains(t, string(output), `panic: unknown driver: "invalid"`)
						return
					}
					require.NoErrorf(t, err, "%s output: %s", workloadName, output)
				})
			}
		})
	}
}

func TestTpccCheckCommand_returns_workloader_init_error_when_ignore_error_is_disabled(t *testing.T) {
	// Given
	configureExecuteTest(t, false)
	configureExecuteTpccUnavailableDBTest(t)
	cmd := registeredCommand(t, registerTpcc, "tpcc", "check")

	// When
	err := cmd.RunE(cmd, nil)

	// Then
	require.ErrorContains(t, err, "init work loader: failed to connect to database when loading data")
}

func TestStartHTTPServer_returns_bind_error(t *testing.T) {
	// Given
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, listener.Close())
	})

	// When
	err = startHTTPServer(listener.Addr().String(), http.NewServeMux())

	// Then
	require.Error(t, err)
}

func registeredCommand(t *testing.T, register func(*cobra.Command), path ...string) *cobra.Command {
	t.Helper()

	root := &cobra.Command{Use: "go-tpc"}
	register(root)
	cmd, _, err := root.Find(path)
	require.NoError(t, err)
	return cmd
}

func configureExecuteTpccUnavailableDBTest(t *testing.T) {
	t.Helper()

	previousTargets := targets
	previousDriver := driver
	t.Cleanup(func() {
		targets = previousTargets
		driver = previousDriver
	})
	targets = []string{"127.0.0.1:0"}
	driver = mysqlDriver
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

func TestExecuteCHWorkloads_prefers_data_error_over_ordinary_error(t *testing.T) {
	// Given
	configureExecuteTest(t, true)
	ordinaryErr := errors.New("ordinary worker error")
	dataErr := workload.NewDataError("inconsistent warehouse totals")
	dataStarted := make(chan struct{})
	ordinary := &errorWorkloader{name: "ordinary", run: func(context.Context) error {
		<-dataStarted
		return ordinaryErr
	}}
	data := &errorWorkloader{name: "data", run: func(ctx context.Context) error {
		close(dataStarted)
		<-ctx.Done()
		return dataErr
	}}

	// When
	err := executeCHWorkloads(context.Background(), []workLoaderSetting{
		{workLoader: ordinary, threads: 1},
		{workLoader: data, threads: 1},
	})

	// Then
	require.ErrorIs(t, err, dataErr)
}

func requireClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	default:
		require.Fail(t, "channel is not closed")
	}
}
