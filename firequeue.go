package firequeue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/cenkalti/backoff/v4"
)

const (
	maxRecordBytes       = 1000 * 1024
	maxBatchBytes        = 4000000
	maxBatchRecords      = 500
	billingUnitBytes     = 5 * 1024
	defaultQueueLength   = 100000
	defaultQueueBytes    = 64 * 1024 * 1024
	defaultBatchInterval = 2 * time.Millisecond
	defaultDrainTimeout  = 10 * time.Second
	startTimeout         = 2 * time.Second
	idleInterval         = time.Hour
)

// Option configures a Queue before Loop starts.
type Option func(*Queue)

// Parallel sets the number of concurrent delivery workers.
func Parallel(n int) Option {
	if n <= 0 {
		panic("parallelism must be positive")
	}
	return func(q *Queue) { q.para = n }
}

// ErrorHandler receives API errors and their affected records.
// It must be concurrency-safe, return promptly, and leave record data unchanged.
func ErrorHandler(fn func(error, []*firehose.Record)) Option {
	return func(q *Queue) { q.errorHandler = fn }
}

// MaxQueueLength bounds pending input events, including in-flight events.
func MaxQueueLength(n int) Option {
	if n <= 0 {
		panic("max queue length must be positive")
	}
	return func(q *Queue) { q.maxQueueLength = n }
}

// MaxQueueBytes bounds pending payload bytes, including in-flight and retry data.
func MaxQueueBytes(n int) Option {
	if n <= 0 {
		panic("max queue bytes must be positive")
	}
	return func(q *Queue) { q.maxQueueBytes = n }
}

// BatchSize bounds Firehose records per request.
func BatchSize(n int) Option {
	if n <= 0 || n > maxBatchRecords {
		panic("batch size must be between 1 and 500")
	}
	return func(q *Queue) { q.batchSize = n }
}

// BatchInterval controls the delay between successful requests with queued data.
func BatchInterval(interval time.Duration) Option {
	if interval <= time.Microsecond {
		panic("batch interval must exceed one microsecond")
	}
	return func(q *Queue) { q.batchInterval = interval }
}

// DrainTimeout bounds delivery after Loop's context ends.
func DrainTimeout(timeout time.Duration) Option {
	if timeout <= 0 {
		panic("drain timeout must be positive")
	}
	return func(q *Queue) { q.drainTimeout = timeout }
}

// Aggregate concatenates payloads without adding delimiters. Callers supply framing.
// Payloads larger than targetBytes remain separate records, up to the API limit.
func Aggregate(targetBytes int, maxWait time.Duration) Option {
	if targetBytes <= 0 || targetBytes > maxRecordBytes || maxWait <= 0 {
		panic("invalid aggregation size or wait")
	}
	return func(q *Queue) {
		q.aggregateBytes = targetBytes
		q.aggregateWait = maxWait
	}
}

// AggregationWaitObserver receives each record's aggregation time before its first attempt.
// The observer must be concurrency-safe and return promptly.
func AggregationWaitObserver(fn func(time.Duration)) Option {
	return func(q *Queue) { q.aggregationObserver = fn }
}

// New creates a queue without starting its delivery workers.
func New(fh firehoseiface.FirehoseAPI, streamName string, opts ...Option) *Queue {
	q := &Queue{
		firehose:           fh,
		DeliveryStreamName: aws.String(streamName),
		initialized:        make(chan struct{}),
		wake:               make(chan struct{}, 1),
		aggregateWake:      make(chan struct{}, 1),
		inFlight:           make(map[*queuedRecord]struct{}),
		para:               1,
		maxQueueLength:     defaultQueueLength,
		maxQueueBytes:      defaultQueueBytes,
		batchSize:          maxBatchRecords,
		batchInterval:      defaultBatchInterval,
		drainTimeout:       defaultDrainTimeout,
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// Queue owns an in-memory queue; it does not provide durable or exactly-once delivery.
type Queue struct {
	mu                  sync.Mutex
	queue               []*queuedRecord
	current             *queuedRecord
	inFlight            map[*queuedRecord]struct{}
	initialized         chan struct{}
	wake                chan struct{}
	aggregateWake       chan struct{}
	started             bool
	closed              bool
	para                int
	maxQueueLength      int
	maxQueueBytes       int
	batchSize           int
	batchInterval       time.Duration
	drainTimeout        time.Duration
	aggregateBytes      int
	aggregateWait       time.Duration
	aggregationObserver func(time.Duration)
	firehose            firehoseiface.FirehoseAPI
	errorHandler        func(error, []*firehose.Record)
	stats               Stats

	// DeliveryStreamName selects the destination. Do not mutate it after Loop starts.
	DeliveryStreamName *string
}

type queuedRecord struct {
	record    *firehose.Record
	events    int64
	createdAt time.Time
	sealedAt  time.Time
	attempted bool
}

// Enqueue copies the payload. Loop must start within two seconds of the call.
// Shutdown rejects new input; successful acceptance does not imply S3 persistence.
func (q *Queue) Enqueue(record *firehose.Record) error {
	select {
	case <-q.initialized:
	default:
		timer := time.NewTimer(startTimeout)
		defer timer.Stop()
		select {
		case <-q.initialized:
		case <-timer.C:
			return errors.New("loop has not yet started. call Loop before Enqueue")
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return errors.New("queue is closed")
	}
	if record == nil || len(record.Data) == 0 || len(record.Data) > maxRecordBytes {
		return errors.New("record data must contain between 1 and 1024000 bytes")
	}
	if q.stats.PendingEvents >= int64(q.maxQueueLength) || int64(len(record.Data)) > int64(q.maxQueueBytes)-q.stats.PendingBytes {
		q.stats.QueueFullError++
		return fmt.Errorf("queue capacity exceeded: %d events, %d bytes pending", q.stats.PendingEvents, q.stats.PendingBytes)
	}
	wasEmpty := len(q.queue) == 0 && q.current == nil
	readyWasEmpty := len(q.queue) == 0
	now := time.Now()
	q.stats.AcceptedEvents++
	q.stats.PendingEvents++
	q.stats.PendingBytes += int64(len(record.Data))
	if q.aggregateBytes == 0 || len(record.Data) > q.aggregateBytes {
		q.sealCurrentLocked(now)
		q.queue = append(q.queue, &queuedRecord{
			record: &firehose.Record{Data: append([]byte(nil), record.Data...)},
			events: 1, createdAt: now, sealedAt: now,
		})
	} else {
		if q.current != nil && (len(q.current.record.Data)+len(record.Data) > q.aggregateBytes || now.Sub(q.current.createdAt) >= q.aggregateWait) {
			q.sealCurrentLocked(now)
		}
		if q.current == nil {
			q.current = &queuedRecord{record: &firehose.Record{}, createdAt: now}
			select {
			case q.aggregateWake <- struct{}{}:
			default:
			}
		}
		q.current.record.Data = append(q.current.record.Data, record.Data...)
		q.current.events++
		if len(q.current.record.Data) == q.aggregateBytes {
			q.sealCurrentLocked(now)
		}
	}
	if wasEmpty || readyWasEmpty && len(q.queue) > 0 {
		q.notify()
	}
	return nil
}

func (q *Queue) sealCurrentLocked(now time.Time) {
	if q.current != nil {
		q.current.sealedAt = now
		q.queue = append(q.queue, q.current)
		q.current = nil
	}
}

func (q *Queue) notify() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// Loop closes input when ctx ends, then drains within the configured timeout.
// It returns a drain error with remaining counts if delivery cannot finish in time.
func (q *Queue) Loop(ctx context.Context) error {
	q.mu.Lock()
	if q.started {
		q.mu.Unlock()
		return errors.New("queue already initialized")
	}
	q.started = true
	close(q.initialized)
	q.mu.Unlock()
	var wg sync.WaitGroup
	if q.aggregateBytes > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q.aggregateLoop(ctx)
		}()
	}
	for i := 0; i < q.para; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q.loop(ctx)
		}()
	}
	<-ctx.Done()
	q.mu.Lock()
	q.closed = true
	q.sealCurrentLocked(time.Now())
	q.mu.Unlock()
	drainCtx, cancel := context.WithTimeout(context.Background(), q.drainTimeout)
	defer cancel()
	wg.Wait()
	bf := backoff.NewExponentialBackOff()
	bf.MaxElapsedTime = 0
	for {
		if drainCtx.Err() != nil {
			stat := q.Stats()
			return fmt.Errorf("drain incomplete: %d events, %d bytes pending: %w", stat.PendingEvents, stat.PendingBytes, drainCtx.Err())
		}
		batch := q.takeBatch()
		if len(batch) == 0 {
			return nil
		}
		if q.send(drainCtx, batch) {
			timer := time.NewTimer(bf.NextBackOff())
			select {
			case <-timer.C:
			case <-drainCtx.Done():
			}
			timer.Stop()
		} else {
			bf.Reset()
		}
	}
}

func (q *Queue) aggregateLoop(ctx context.Context) {
	timer := time.NewTimer(idleInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.aggregateWake:
		case <-timer.C:
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		q.mu.Lock()
		delay := idleInterval
		if q.current != nil {
			delay = time.Until(q.current.createdAt.Add(q.aggregateWait))
			if delay <= 0 {
				q.sealCurrentLocked(time.Now())
				q.notify()
				delay = idleInterval
			}
		}
		q.mu.Unlock()
		timer.Reset(delay)
	}
}

func (q *Queue) loop(ctx context.Context) {
	bf := backoff.NewExponentialBackOff()
	bf.MaxElapsedTime = 0
	timer := time.NewTimer(q.batchInterval)
	defer timer.Stop()
	var notBefore time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.wake:
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if delay := time.Until(notBefore); delay > 0 {
			timer.Reset(delay)
			continue
		}
		batch := q.takeBatch()
		if len(batch) > 0 && q.send(ctx, batch) {
			notBefore = time.Now().Add(bf.NextBackOff())
			timer.Reset(time.Until(notBefore))
		} else {
			bf.Reset()
			if len(batch) > 0 {
				notBefore = time.Now().Add(q.batchInterval)
			}
			timer.Reset(q.nextDelay())
		}
	}
}

func (q *Queue) nextDelay() time.Duration {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) > 0 {
		return q.batchInterval
	}
	return idleInterval
}

func (q *Queue) takeBatch() []*queuedRecord {
	q.mu.Lock()
	n := 0
	bytes := 0
	for n < len(q.queue) && n < q.batchSize {
		if bytes+len(q.queue[n].record.Data) > maxBatchBytes {
			break
		}
		bytes += len(q.queue[n].record.Data)
		n++
	}
	batch := make([]*queuedRecord, n)
	copy(batch, q.queue[:n])
	for i := 0; i < n; i++ {
		q.queue[i] = nil
	}
	q.queue = q.queue[n:]
	if len(q.queue) == 0 {
		q.queue = nil
	} else {
		q.notify()
	}
	var waits []time.Duration
	for _, record := range batch {
		q.inFlight[record] = struct{}{}
		if !record.attempted {
			if q.aggregateBytes > 0 && q.aggregationObserver != nil {
				waits = append(waits, record.sealedAt.Sub(record.createdAt))
			}
			record.attempted = true
		}
	}
	q.mu.Unlock()
	for _, wait := range waits {
		q.aggregationObserver(wait)
	}
	return batch
}
