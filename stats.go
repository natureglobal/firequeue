package firequeue

type Stats struct {
	Success          int64
	RetrySuccess     int64
	UnretryableError int64
	QueueFullError   int64
	GiveupError      int64
}
