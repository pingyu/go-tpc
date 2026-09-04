package workload

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataError_Error_retains_cloud_detection_marker(t *testing.T) {
	// Given
	err := NewDataError("inconsistent warehouse totals")

	// When
	message := err.Error()

	// Then
	require.Equal(t, "[DATA ERROR] inconsistent warehouse totals", message)
}
