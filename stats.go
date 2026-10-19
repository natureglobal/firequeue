package firequeue

import "time"

// Stats separates input events from Firehose records and reports pending payloads.
type Stats struct {
	// QueueLength counts pending events outside active requests.
	QueueLength int
	// BatchLength counts Firehose records in the most recent request.
	BatchLength int64
	// Success counts acknowledged Firehose records, not input events.
	Success int64
	// RetryCount counts records queued for another attempt.
	RetryCount int64
	// UnretryableError counts terminally failed Firehose records.
	UnretryableError int64
	// QueueFullError counts input rejections caused by either capacity limit.
	QueueFullError int64
	// AcceptedEvents counts input events accepted by Enqueue.
	AcceptedEvents int64
	// SuccessfulEvents counts input events acknowledged by Firehose.
	SuccessfulEvents int64
	// FailedEvents counts input events with terminal API failures.
	FailedEvents int64
	// PendingEvents includes aggregation, queued, retrying, and in-flight events.
	PendingEvents int64
	// PendingBytes counts payload bytes owned by all pending events.
	PendingBytes int64
	// SentRecords counts every attempted record, including retries.
	SentRecords int64
	// EstimatedBillableBytes rounds every attempt up to 5 KiB; it is not a bill.
	EstimatedBillableBytes int64
	// OldestEventAge measures the age of the oldest pending input event.
	OldestEventAge time.Duration
}

// Stats returns a concurrency-safe snapshot of delivery accounting.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	stat := q.stats
	var inFlightEvents int64
	var oldest time.Time
	visit := func(record *queuedRecord) {
		if oldest.IsZero() || record.createdAt.Before(oldest) {
			oldest = record.createdAt
		}
	}
	for record := range q.inFlight {
		inFlightEvents += record.events
		visit(record)
	}
	for _, record := range q.queue {
		visit(record)
	}
	if q.current != nil {
		visit(q.current)
	}
	stat.QueueLength = int(stat.PendingEvents - inFlightEvents)
	if !oldest.IsZero() {
		stat.OldestEventAge = time.Since(oldest)
	}
	return stat
}
