package main

import (
	"fmt"
	"io/ioutil"
	"path"
	"strings"
	"time"

	"github.com/pingcap/go-tpc/rawsql"
	"github.com/spf13/cobra"
)

var (
	rawsqlConfig    rawsql.Config
	queryFiles      string
	refreshConnWait time.Duration
)

func registerRawsql(root *cobra.Command) {
	cmd := &cobra.Command{
		Use: "rawsql",
	}

	cmdRun := &cobra.Command{
		Use:   "run",
		Short: "Run workload",
		RunE: func(_ *cobra.Command, _ []string) error {
			// Usage errors must surface even with --ignore-error.
			if len(queryFiles) == 0 {
				return fmt.Errorf("empty query files")
			}
			return ignoreCommandError(execRawsql("run"))
		},
	}

	cmdRun.PersistentFlags().BoolVar(&rawsqlConfig.EnablePlanReplayer,
		"use-plan-replayer",
		false,
		"Use Plan Replayer to dump stats and variables before running queries")

	cmdRun.PersistentFlags().StringVar(&rawsqlConfig.PlanReplayerConfig.PlanReplayerDir,
		"plan-replayer-dir",
		"",
		"Dir of Plan Replayer file dumps")

	cmdRun.PersistentFlags().StringVar(&rawsqlConfig.PlanReplayerConfig.PlanReplayerFileName,
		"plan-replayer-file",
		"",
		"Name of plan Replayer file dumps")

	cmdRun.PersistentFlags().StringVar(&queryFiles,
		"query-files",
		"",
		"path of query files")

	cmdRun.PersistentFlags().BoolVar(&rawsqlConfig.ExecExplainAnalyze,
		"use-explain",
		false,
		"execute explain analyze")

	cmdRun.PersistentFlags().DurationVar(&refreshConnWait, "refresh-conn-wait", 5*time.Second, "duration to wait before refreshing sql connection")

	cmd.AddCommand(cmdRun)
	root.AddCommand(cmd)
}

func execRawsql(action string) error {
	if err := openDB(); err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer closeDB()

	// if globalDB == nil
	if globalDB == nil {
		return fmt.Errorf("cannot connect to the database")
	}

	rawsqlConfig.OutputStyle = outputStyle
	rawsqlConfig.DBName = dbName
	rawsqlConfig.QueryNames = strings.Split(queryFiles, ",")
	rawsqlConfig.Queries = make(map[string]string, len(rawsqlConfig.QueryNames))
	rawsqlConfig.RefreshWait = refreshConnWait
	rawsqlConfig.PlanReplayerConfig.Host = hosts[0]
	rawsqlConfig.PlanReplayerConfig.StatusPort = statusPort

	for i, filename := range rawsqlConfig.QueryNames {
		queryData, err := ioutil.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("read file %s: %w", filename, err)
		}

		baseName := path.Base(filename)
		queryName := strings.TrimSuffix(baseName, path.Ext(baseName))
		rawsqlConfig.QueryNames[i] = queryName
		rawsqlConfig.Queries[queryName] = string(queryData)
	}

	w := rawsql.NewWorkloader(globalDB, &rawsqlConfig)

	workloadCtx, cancel := newWorkloadContext(globalCtx, action, totalTime)
	defer cancel()
	if err := executeWorkload(workloadCtx, w, threads, action); err != nil {
		return fmt.Errorf("execute %s failed: %w", action, err)
	}
	fmt.Println("Finished")
	w.OutputStats(true)
	return nil
}
