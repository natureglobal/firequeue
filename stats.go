package firequeue

// Stats contains firequeue statistics.
type Stats struct {
	QueueLength      int
	Success          int64
	RetrySuccess     int64
	UnretryableError int64
	QueueFullError   int64
	GiveupError      int64
}
