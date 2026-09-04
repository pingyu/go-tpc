package workload

const dataErrorMarker = "[DATA ERROR]"

type DataError struct {
	message string
}

func NewDataError(message string) *DataError {
	return &DataError{message: message}
}

func (e *DataError) Error() string {
	return dataErrorMarker + " " + e.message
}
