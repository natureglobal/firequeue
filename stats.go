package firequeue

// Stats contains firequeue statistics.
type Stats struct {
	QueueLength      int
	Success          int64
	RetryCount       int64
	UnretryableError int64
	QueueFullError   int64
}
