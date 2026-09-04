package tpcc

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCSVWorkLoader_generates_consistent_scaled_data(t *testing.T) {
	// Given
	outputDir := t.TempDir()
	loader, err := NewCSVWorkloader(nil, &Config{
		DBName:        "test",
		Threads:       1,
		Parts:         1,
		PartitionType: PartitionTypeHash,
		Warehouses:    1,
		OutputDir:     outputDir,
	})
	require.NoError(t, err)
	ctx, err := loader.InitThread(context.Background(), 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		loader.CleanupThread(ctx, 0)
	})

	// When
	err = loader.Prepare(ctx, 0)

	// Then
	require.NoError(t, err)
	warehouseRows := readCSVRows(t, outputDir, tableWareHouse)
	require.Equal(t, fmt.Sprintf("%.6f", float64(districtPerWarehouse*customerPerDistrict)*ytdPaymentPerCustomer), warehouseRows[0][8])

	districtRows := readCSVRows(t, outputDir, tableDistrict)
	require.Equal(t, fmt.Sprintf("%.6f", float64(customerPerDistrict)*ytdPaymentPerCustomer), districtRows[0][9])
	require.Equal(t, strconv.Itoa(orderPerDistrict+1), districtRows[0][10])

	orderRows := readCSVRows(t, outputDir, tableOrders)
	newOrderRows := readCSVRows(t, outputDir, tableNewOrder)
	orders := make(map[string]struct{}, len(orderRows))
	for _, row := range orderRows {
		orders[row[2]+","+row[1]+","+row[0]] = struct{}{}
		orderID := parseCSVInt(t, row[0])
		if orderID <= orderPerDistrict-newOrderPerDistrict {
			require.NotEqual(t, "NULL", row[5])
		} else {
			require.Equal(t, "NULL", row[5])
		}
	}
	for _, row := range newOrderRows {
		orderID := parseCSVInt(t, row[0])
		require.GreaterOrEqual(t, orderID, orderPerDistrict-newOrderPerDistrict+1)
		require.LessOrEqual(t, orderID, orderPerDistrict)
		_, ok := orders[row[2]+","+row[1]+","+row[0]]
		require.True(t, ok, "new_order row must reference an existing order: %v", row)
	}

	for _, row := range readCSVRows(t, outputDir, tableOrderLine) {
		itemID := parseCSVInt(t, row[4])
		require.GreaterOrEqual(t, itemID, 1)
		require.LessOrEqual(t, itemID, maxItems)
		orderID := parseCSVInt(t, row[0])
		if orderID <= orderPerDistrict-newOrderPerDistrict {
			require.NotEqual(t, "NULL", row[6])
		} else {
			require.Equal(t, "NULL", row[6])
		}
	}
}

func readCSVRows(t *testing.T, outputDir, table string) [][]string {
	t.Helper()
	file, err := os.Open(filepath.Join(outputDir, "test."+table+".0.csv"))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, file.Close())
	})
	rows, err := csv.NewReader(file).ReadAll()
	require.NoError(t, err)
	return rows
}

func parseCSVInt(t *testing.T, value string) int {
	t.Helper()
	parsed, err := strconv.Atoi(value)
	require.NoError(t, err)
	return parsed
}
