package firequeue

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/cenkalti/backoff/v4"
)

// Option is a type for constructor options
type Option func(*Queue)

// Parallel is an option to specify the number of parallelism
func Parallel(i int) Option {
	return func(q *Queue) {
		q.para = i
	}
}

// ErrorHandler is an option to specify the error handler
func ErrorHandler(fn func(error, *firehose.PutRecordInput)) Option {
	return func(q *Queue) {
		q.errorHandler = fn
	}
}

// MaxQueueLength is an option to specify max length of in-memory queue
func MaxQueueLength(length int) Option {
	return func(q *Queue) {
		q.maxQueueLength = length
	}
}

// New return new Queue
func New(fh firehoseiface.FirehoseAPI, opts ...Option) *Queue {
	q := &Queue{firehose: fh, initialized: make(chan struct{})}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// Queue manages a sending list for firehose
type Queue struct {
	queue []*firehose.PutRecordInput
	mu    sync.RWMutex

	initialized chan struct{}
	initMu      sync.Mutex

	para            int
	inFlightCounter int32
	firehose        firehoseiface.FirehoseAPI
	errorHandler    func(error, *firehose.PutRecordInput)
	maxQueueLength  int

	successCount          expvar.Int
	retrySuccessCount     expvar.Int
	unretryableErrorCount expvar.Int
	queueFullErrorCount   expvar.Int
	giveupErrorCount      expvar.Int
}

// Stats return queue stats
func (q *Queue) Stats() Stats {
	return Stats{
		QueueLength:      q.len(),
		Success:          q.successCount.Value(),
		RetrySuccess:     q.retrySuccessCount.Value(),
		UnretryableError: q.unretryableErrorCount.Value(),
		QueueFullError:   q.queueFullErrorCount.Value(),
		GiveupError:      q.giveupErrorCount.Value(),
	}
}

func (q *Queue) init() error {
	q.initMu.Lock()
	defer q.initMu.Unlock()
	select {
	case <-q.initialized:
		return fmt.Errorf("already initialized and started the loop")
	default:
	}
	if q.maxQueueLength == 0 {
		q.maxQueueLength = 100000
	}
	if q.para == 0 {
		q.para = 1
	}
	close(q.initialized)
	return nil
}

// Loop proceeds jobs in queue
func (q *Queue) Loop(ctx context.Context) error {
	if err := q.init(); err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(q.para)
	for i := 0; i < q.para; i++ {
		go func() {
			defer wg.Done()
			q.loop(ctx)
		}()
	}
	wg.Wait()

	// draining process:
	// The draining will continue if enqueuing continues or the Firehose is in failure,
	// but it should be taken care of it at higher levels.
	for {
		for q.remaining() {
			q.put(&backoff.ZeroBackOff{})
		}
		// Wait 2 seconds and wait to see if the jobs will accumulate, because they might
		// come in queue
		time.Sleep(time.Second * 2)
		if !q.remaining() {
			break
		}
	}
	return nil
}

func (q *Queue) loop(ctx context.Context) {
	var bf = backoff.NewExponentialBackOff()
	bf.MaxElapsedTime = 0
	// Use 1 instead of 0 because time.NewTicker and ticker.Reset disallow zero
	var nextInterval time.Duration = 1
	ticker := time.NewTicker(1)
	defer ticker.Stop()

	for {
		ticker.Reset(nextInterval)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nextInterval = q.put(bf)
		}
	}
}

func isRetryable(err error) bool {
	if reqfailure, ok := err.(awserr.RequestFailure); ok {
		// ref. https://github.com/aws/aws-sdk-go/blob/fe72a52350a8962175bb71c531ec9724ce48abd8/aws/request/retryer.go#L228-L250
		switch reqfailure.StatusCode() {
		case
			429,
			500,
			502,
			503,
			504:
			return true
		}
	}
	return request.IsErrorRetryable(err) ||
		request.IsErrorThrottle(err) ||
		isErrConnectionResetByPeer(err)
}

// request.isErrConnectionReset, which is used inside request.IsErrorRetryable,
// intentionally marks false (do not retry) if it contains "read: connection reset".
// ref. https://github.com/aws/aws-sdk-go/blob/7814a7f61bf93cb54d00a5f97918c5501f07d351/aws/request/connection_reset_error.go#L7-L18
// However, in firehose, the connection reset by peer error often bursts, so it is to be retry
func isErrConnectionResetByPeer(err error) bool {
	return strings.Contains(err.Error(), "read: connection reset by peer")
}

// Send firehorseInput
func (q *Queue) Send(r *firehose.PutRecordInput) error {
	select {
	case <-q.initialized:
		// nop and continue to send
	default:
		// It is useless if a timer is created in every Send invocation, so tuck the "default:" here.
		select {
		case <-time.After(2 * time.Second):
			return errors.New("loop has not yet started. call Loop() before Send()")
		case <-q.initialized:
			// nop and continue to send
		}
	}
	_, err := q.firehose.PutRecord(r)
	if err == nil {
		q.successCount.Add(1)
		return nil
	}
	if !isRetryable(err) {
		q.unretryableErrorCount.Add(1)
		return err
	}
	if l := q.len(); l >= q.maxQueueLength {
		q.queueFullErrorCount.Add(1)
		return fmt.Errorf("too many jobs accumlated: %d, %w", l, err)
	}
	// Actually, race conditions may occur here and make the queue a little longer than maxQueueLength
	// temporarily, but it's not a big problem and we don't care.
	q.push(r)
	return nil
}

func (q *Queue) handleError(err error, r *firehose.PutRecordInput) {
	if q.errorHandler != nil {
		q.errorHandler(err, r)
		return
	}
	log.Println(err)
}

// put puts item and returns interval to put next
func (q *Queue) put(bf backoff.BackOff) time.Duration {
	r, done := q.shift()
	if r == nil {
		bf.Reset()
		return 3 * time.Second
	}
	defer done()

	if _, err := q.firehose.PutRecord(r); err != nil {
		if isRetryable(err) {
			// If an error occurs, move it back to the top of the queue and wait a while.
			// It might be better to stuff it at the back of the queue, to avoid continuous
			// error due to invalid input, but we do it that for now.
			q.unshift(r)
		} else {
			q.giveupErrorCount.Add(1)
			q.handleError(err, r)
		}
		return bf.NextBackOff()
	}
	q.retrySuccessCount.Add(1)
	bf.Reset()
	// go to the next immediately when jobs are still in the queue
	return 1
}

func (q *Queue) len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.queue)
}

func (q *Queue) inFlight() bool {
	return atomic.LoadInt32(&q.inFlightCounter) > 0
}

func (q *Queue) remaining() bool {
	// XXX race
	return q.len() > 0 || q.inFlight()
}

func (q *Queue) push(r *firehose.PutRecordInput) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(q.queue, r)
}

func (q *Queue) shift() (*firehose.PutRecordInput, func()) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) == 0 {
		return nil, nil
	}

	atomic.AddInt32(&q.inFlightCounter, 1)
	r := q.queue[0]
	q.queue = q.queue[1:]
	return r, func() { atomic.AddInt32(&q.inFlightCounter, -1) }
}

func (q *Queue) unshift(r *firehose.PutRecordInput) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.queue = append([]*firehose.PutRecordInput{r}, q.queue...)
}
