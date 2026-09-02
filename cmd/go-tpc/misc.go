package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/go-tpc/pkg/workload"
)

func checkPrepare(ctx context.Context, w workload.Workloader, threads int) error {
	// skip preparation check in csv case
	if w.Name() == "tpcc-csv" {
		fmt.Println("Skip preparing checking. Please load CSV data into database and check later.")
		return nil
	}
	if w.Name() == "tpcc" && tpccConfig.NoCheck {
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(threads)
	checkErrors := make(chan error, threads)
	for i := 0; i < threads; i++ {
		go func(index int) {
			defer wg.Done()

			threadCtx := w.InitThread(ctx, index)
			defer w.CleanupThread(threadCtx, index)

			if err := w.CheckPrepare(threadCtx, index); err != nil {
				checkErrors <- fmt.Errorf("check prepare worker %d: %w", index, err)
			}
		}(i)
	}
	wg.Wait()
	close(checkErrors)
	for err := range checkErrors {
		return err
	}
	return nil
}

func execute(timeoutCtx context.Context, w workload.Workloader, action string, threads, index int) error {
	count := totalCount / threads

	// For prepare, cleanup and check operations, use background context to avoid timeout constraints
	// Only run phases should be limited by timeout
	var ctx context.Context
	if action == "prepare" || action == "cleanup" || action == "check" {
		ctx = w.InitThread(context.Background(), index)
	} else {
		ctx = w.InitThread(timeoutCtx, index)
	}
	defer w.CleanupThread(ctx, index)

	switch action {
	case "prepare":
		return w.Prepare(ctx, index)
	case "cleanup":
		return w.Cleanup(ctx, index)
	case "check":
		err := w.Check(ctx, index)
		if shouldIgnoreError(err) {
			return nil
		}
		return err
	}

	// This loop is only reached for "run" action since other actions return earlier
	for i := 0; i < count || count <= 0; i++ {
		// Check if timeout has occurred before starting next query
		select {
		case <-ctx.Done():
			if !silence {
				fmt.Printf("[%s] %s worker %d stopped due to %s after %d iterations\n",
					time.Now().Format("2006-01-02 15:04:05"), action, index, contextTerminationReason(ctx), i)
			}
			return nil
		default:
		}

		err := w.Run(ctx, index)
		if err != nil {
			// Check if the error is due to timeout/cancellation
			if ctx.Err() != nil && (isPureContextTermination(err) || isToleratedNetworkError(err)) {
				if !silence {
					fmt.Printf("[%s] %s worker %d stopped due to %s: %v\n",
						time.Now().Format("2006-01-02 15:04:05"), action, index, contextTerminationReason(ctx), err)
				}
				return nil
			}

			if shouldIgnoreError(err) {
				if !silence {
					fmt.Printf("[%s] execute %s failed, err %v\n", time.Now().Format("2006-01-02 15:04:05"), action, err)
				}
				continue
			}
			return err
		}
	}

	return nil
}

func executeWorkload(ctx context.Context, w workload.Workloader, threads int, action string) error {
	return executeConfiguredWorkload(ctx, workLoaderSetting{workLoader: w, threads: threads}, action)
}

type workLoaderSetting struct {
	workLoader    workload.Workloader
	threads       int
	onWorkerError func(error)
}

func executeConfiguredWorkload(ctx context.Context, setting workLoaderSetting, action string) error {
	w := setting.workLoader
	threads := setting.threads
	if action == "prepare" && dropData {
		if err := executeConfiguredWorkload(ctx, setting, "cleanup"); err != nil {
			return fmt.Errorf("cleanup before prepare: %w", err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(threads)

	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	outputCtx, outputCancel := context.WithCancel(workerCtx)
	ch := make(chan struct{}, 1)
	workerErrors := make(chan error, threads)
	go func() {
		ticker := time.NewTicker(outputInterval)
		defer ticker.Stop()

		for {
			select {
			case <-outputCtx.Done():
				ch <- struct{}{}
				return
			case <-ticker.C:
				w.OutputStats(false)
			}
		}
	}()
	defer func() {
		outputCancel()
		<-ch
	}()
	if w.Name() == "tpch" && action == "run" {
		err := execWorkloadSQL(workerCtx, w, `create or replace view revenue0 (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= '1997-07-01'
		and l_shipdate < date_add('1997-07-01', interval '3' month)
	group by
		l_suppkey;`)
		if err != nil {
			return fmt.Errorf("prepare tpch view: %w", err)
		}
	}
	// CH benchmark requires the revenue1 view for analytical queries.
	// During normal prepare flow, this view is created in prepareView() method.
	// However, when using CSV data ingestion, the prepare stage is skipped and
	// the view won't exist. So we create it here when action is "run" to ensure
	// the view is available regardless of how data was loaded.
	if w.Name() == "ch" && action == "run" {
		err := execWorkloadSQL(workerCtx, w, `create or replace view revenue1 (supplier_no, total_revenue) as (
    select	mod((s_w_id * s_i_id),10000) as supplier_no,
              sum(ol_amount) as total_revenue
    from	order_line, stock
    where ol_i_id = s_i_id and ol_supply_w_id = s_w_id
      and ol_delivery_d >= '2007-01-02 00:00:00.000000'
    group by mod((s_w_id * s_i_id),10000));`)
		if err != nil {
			return fmt.Errorf("prepare ch view: %w", err)
		}
	}
	enabledDumpPlanReplayer := w.IsPlanReplayerDumpEnabled()
	if enabledDumpPlanReplayer {
		err := w.PreparePlanReplayerDump()
		if err != nil {
			fmt.Printf("[%s] prepare plan replayer failed, err%v\n",
				time.Now().Format("2006-01-02 15:04:05"), err)
		}
		defer func() {
			err = w.FinishPlanReplayerDump()
			if err != nil {
				fmt.Printf("[%s] dump plan replayer failed, err%v\n",
					time.Now().Format("2006-01-02 15:04:05"), err)
			}
		}()
	}

	for i := 0; i < threads; i++ {
		go func(index int) {
			defer wg.Done()
			if err := execute(workerCtx, w, action, threads, index); err != nil {
				workerErrors <- err
				stopWorkers()
				if setting.onWorkerError != nil {
					setting.onWorkerError(err)
				}
			}
		}(i)
	}

	wg.Wait()
	close(workerErrors)
	var firstError error
	for err := range workerErrors {
		if firstError == nil {
			firstError = err
		}
	}

	if action == "prepare" && firstError == nil {
		// For prepare, we must check the data consistency after all prepare finished
		return checkPrepare(ctx, w, threads)
	}
	return firstError
}

func contextTerminationReason(ctx context.Context) string {
	if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
		return "timeout"
	}
	return "cancellation"
}

func execWorkloadSQL(ctx context.Context, w workload.Workloader, sql string) error {
	if executor, ok := w.(interface {
		ExecContext(context.Context, string) error
	}); ok {
		return executor.ExecContext(ctx, sql)
	}
	return w.Exec(sql)
}
