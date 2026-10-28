package firequeue

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/cenkalti/backoff/v4"
)

// FirehoseAPI is the part of *firehose.Client that Queue uses.
type FirehoseAPI interface {
	PutRecordBatch(ctx context.Context, params *firehose.PutRecordBatchInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error)
}

// Option is a type for constructor options
type Option func(*Queue)

// Parallel is an option to specify the number of parallelism
func Parallel(i int) Option {
	return func(q *Queue) {
		q.para = i
	}
}

// ErrorHandler is an option to specify the error handler
func ErrorHandler(fn func(error, []types.Record)) Option {
	return func(q *Queue) {
		q.errorHandler = fn
	}
}

// MaxQueueLength is an option to specify max length of in-memory queue
func MaxQueueLength(length int) Option {
	if length <= 0 {
		panic("max queue length must be positive")
	}
	return func(q *Queue) {
		q.maxQueueLength = length
	}
}

// BatchSize is an option to specify batch size
func BatchSize(length int) Option {
	if length <= 0 {
		panic("max queue length must be positive")
	}
	// "Each PutRecordBatch request supports up to 500 records."
	// ref. https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/firehose#Client.PutRecordBatch
	if length > 500 {
		panic("max queue length must be less than 500")
	}
	return func(q *Queue) {
		q.batchSize = length
	}
}

// BatchInterval is an option to specify batch interval
func BatchInterval(period time.Duration) Option {
	// API limit is 1000 requests per second per shard(default).
	// This means an average of 1 request per millisecond.
	// ref. https://docs.aws.amazon.com/ja_jp/firehose/latest/dev/limits.html#:~:text=Direct%20PUT%20%E3%81%8C%E3%83%87%E3%83%BC%E3%82%BF%E3%82%BD%E3%83%BC%E3%82%B9%E3%81%A8%E3%81%97%E3%81%A6%E8%A8%AD%E5%AE%9A%E3%81%95%E3%82%8C%E3%81%A6%E3%81%84%E3%82%8B%E5%A0%B4%E5%90%88%E3%80%81%E5%90%84%20Kinesis%20Data%20Firehose%20%E3%83%87%E3%83%AA%E3%83%90%E3%83%AA%E3%83%BC%E3%82%B9%E3%83%88%E3%83%AA%E3%83%BC%E3%83%A0%E3%81%AF%E3%80%81PutRecord%E3%81%8A%E3%82%88%E3%81%B3%E3%83%AA%E3%82%AF%E3%82%A8%E3%82%B9%E3%83%88%E3%81%AB%E5%AF%BE%E3%81%97%E3%81%A6%E4%BB%A5%E4%B8%8B%E3%81%AE%E5%90%88%E8%A8%88%E3%82%AF%E3%82%A9%E3%83%BC%E3%82%BF%E3%82%92%E6%8F%90%E4%BE%9B%E3%81%97%E3%81%BE%E3%81%99%E3%80%82PutRecordBatch
	if period <= 1*time.Microsecond {
		panic("batch interval must be greater than 1 microsecond")
	}

	return func(q *Queue) {
		q.batchInterval = period
	}
}

// New return new Queue
func New(fh FirehoseAPI, DeliveryStreamName string, opts ...Option) *Queue {
	q := &Queue{
		queue:              make([]types.Record, 0),
		workerCh:           make(chan []types.Record),
		firehose:           fh,
		initialized:        make(chan struct{}),
		DeliveryStreamName: aws.String(DeliveryStreamName),
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// Queue manages a sending list for firehose
type Queue struct {
	queue    []types.Record
	mu       sync.RWMutex
	workerCh chan []types.Record

	initialized chan struct{}
	initMu      sync.Mutex

	para                 int
	inFlightBatchCounter int32
	maxQueueLength       int
	firehose             FirehoseAPI
	errorHandler         func(error, []types.Record)
	batchSize            int
	batchInterval        time.Duration
	DeliveryStreamName   *string

	// successCount counts successfully sent records when attemptToSendBatch() is called.
	successCount expvar.Int
	// retryCount counts retryable records when attemptToSendBatch() is called.
	retryCount expvar.Int
	// unretryableErrorCount counts non-retryable batch when attemptToSendBatch() is called.
	unretryableErrorCount expvar.Int
	// queueFullErrorCount counts queue full errors when Enqueue() is called.
	queueFullErrorCount expvar.Int
	// batchLength is length of the latest batch.
	batchLength expvar.Int
}

// Stats return queue stats
func (q *Queue) Stats() Stats {
	return Stats{
		QueueLength:      q.len(),
		BatchLength:      q.batchLength.Value(),
		Success:          q.successCount.Value(),
		RetryCount:       q.retryCount.Value(),
		UnretryableError: q.unretryableErrorCount.Value(),
		QueueFullError:   q.queueFullErrorCount.Value(),
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
	if q.batchSize == 0 {
		q.batchSize = 500
	}
	if q.batchInterval == 0 {
		q.batchInterval = 2 * time.Millisecond
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
	var bf = backoff.NewExponentialBackOff()
	bf.MaxElapsedTime = 0

	for {
		if q.remaining() {
			nextInterval := q.sendBatch(bf)
			time.Sleep(nextInterval)
			continue
		}
		if !q.remaining() {
			break
		}
	}
	return nil
}

func (q *Queue) loop(ctx context.Context) {
	var bf = backoff.NewExponentialBackOff()
	bf.MaxElapsedTime = 0
	var nextInterval time.Duration = q.batchInterval
	ticker := time.NewTicker(q.batchInterval)
	defer ticker.Stop()

	for {
		ticker.Reset(nextInterval)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nextInterval = q.sendBatch(bf)
		}
	}
}

// isRetryable keeps the set of retryable errors that firequeue had with
// aws-sdk-go v1 (request.IsErrorRetryable, request.IsErrorThrottle and the
// HTTP status codes below).
func isRetryable(err error) bool {
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		// ref. https://github.com/aws/aws-sdk-go/blob/fe72a52350a8962175bb71c531ec9724ce48abd8/aws/request/retryer.go#L228-L250
		switch respErr.HTTPStatusCode() {
		case
			429,
			500,
			502,
			503,
			504:
			return true
		}
	}
	return isErrorRetryable(err) || isErrConnectionResetByPeer(err)
}

// retryableErrorCodes are the API error codes that aws-sdk-go v1 retried:
// retryableCodes, credsExpiredCodes and throttleCodes in aws/request/retryer.go.
// v1 also had the client-side codes "RequestError" and "ResponseTimeout".
// v2 does not use these codes. isErrorRetryable examines
// smithyhttp.RequestSendError in place of "RequestError".
var retryableErrorCodes = map[string]struct{}{
	"RequestTimeout":          {},
	"RequestTimeoutException": {},

	"ExpiredToken":          {},
	"ExpiredTokenException": {},
	"RequestExpired":        {},

	"ProvisionedThroughputExceededException": {},
	"ThrottledException":                     {},
	"Throttling":                             {},
	"ThrottlingException":                    {},
	"RequestLimitExceeded":                   {},
	"RequestThrottled":                       {},
	"RequestThrottledException":              {},
	"TooManyRequestsException":               {},
	"PriorRequestNotComplete":                {},
	"TransactionInProgressException":         {},
	"EC2ThrottledException":                  {},
}

// isErrorRetryable follows request.IsErrorRetryable and
// request.IsErrorThrottle of aws-sdk-go v1 for the errors that the v2 client
// returns.
func isErrorRetryable(err error) bool {
	// v1 did not retry "RequestCanceled". v2 returns *smithy.CanceledError or
	// *aws.RequestCanceledError when the context is done.
	var canceledErr interface{ CanceledError() bool }
	if errors.As(err, &canceledErr) && canceledErr.CanceledError() {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		_, ok := retryableErrorCodes[apiErr.ErrorCode()]
		return ok
	}
	// v1 "SerializationError": v1 retried it when it retried the cause,
	// and did not retry it without a cause.
	var deserializationErr *smithy.DeserializationError
	if errors.As(err, &deserializationErr) {
		return deserializationErr.Err != nil && shouldRetryError(deserializationErr.Err)
	}
	// v1 "RequestError": v1 retried it when it retried the cause,
	// and retried it without a cause.
	var sendErr *smithyhttp.RequestSendError
	if errors.As(err, &sendErr) {
		return shouldRetryError(sendErr.Err)
	}
	// v1 did not retry errors from parameter validation either.
	return false
}

// shouldRetryError is the part of request.shouldRetryError in aws-sdk-go v1
// that examines the causes of "RequestError" and "SerializationError".
// ref. https://github.com/aws/aws-sdk-go/blob/v1.35.33/aws/request/retryer.go#L168-L226
func shouldRetryError(err error) bool {
	switch e := err.(type) {
	case *url.Error:
		if strings.Contains(e.Error(), "connection refused") {
			return true
		}
		return shouldRetryError(e.Err)
	case interface{ Temporary() bool }:
		if netErr, ok := e.(*net.OpError); ok && netErr.Op == "dial" {
			return true
		}
		return e.Temporary() || isErrConnectionReset(err)
	case nil:
		return true
	default:
		switch err.Error() {
		case "net/http: request canceled",
			"net/http: request canceled while waiting for connection":
			return false
		}
		return true
	}
}

// isErrConnectionReset is request.isErrConnectionReset in aws-sdk-go v1.
// It intentionally marks false (do not retry) if it contains "read: connection reset".
// ref. https://github.com/aws/aws-sdk-go/blob/7814a7f61bf93cb54d00a5f97918c5501f07d351/aws/request/connection_reset_error.go#L7-L18
func isErrConnectionReset(err error) bool {
	msg := err.Error()
	if strings.Contains(msg, "read: connection reset") {
		return false
	}
	return strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe")
}

// isErrConnectionResetByPeer catches "read: connection reset by peer", which isErrConnectionReset does not retry.
// In firehose, the connection reset by peer error often bursts, so it is to be retry
func isErrConnectionResetByPeer(err error) bool {
	return strings.Contains(err.Error(), "read: connection reset by peer")
}

// Enqueue add data to queue
func (q *Queue) Enqueue(r types.Record) error {
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
	if l := q.len(); l >= q.maxQueueLength {
		q.queueFullErrorCount.Add(1)
		return fmt.Errorf("too many jobs accumlated: %d", l)
	}
	q.push(r)
	return nil
}

func (q *Queue) sendBatch(bf backoff.BackOff) time.Duration {
	if q.len() == 0 {
		return q.batchInterval
	}
	batch := q.createBatch()
	q.batchLength.Set(int64(len(batch)))

	if err := q.attemptToSendBatch(batch); err != nil {
		// Handle non-retryable errors directly.
		q.handleError(err, batch)
		return bf.NextBackOff()
	}
	bf.Reset()
	return q.batchInterval
}

func (q *Queue) createBatch() []types.Record {
	q.mu.Lock()
	defer q.mu.Unlock()

	batchSize := q.batchSize
	if len(q.queue) < batchSize {
		batchSize = len(q.queue)
	}

	batch := make([]types.Record, batchSize)
	copy(batch, q.queue)
	q.queue = q.queue[batchSize:]
	return batch
}

func (q *Queue) attemptToSendBatch(batch []types.Record) error {
	atomic.AddInt32(&q.inFlightBatchCounter, 1)
	defer atomic.AddInt32(&q.inFlightBatchCounter, -1)

	input := &firehose.PutRecordBatchInput{
		DeliveryStreamName: q.DeliveryStreamName,
		Records:            batch,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := q.firehose.PutRecordBatch(ctx, input)
	if err != nil {
		// Handle non-retryable errors directly.
		if !isRetryable(err) {
			q.unretryableErrorCount.Add(1)
			q.handleError(err, batch)
			return nil
		}
		// Retry failed records.
		if resp != nil && resp.FailedPutCount != nil && *resp.FailedPutCount > 0 {
			q.successCount.Add(int64(len(batch)) - int64(*resp.FailedPutCount))
			q.retryFailedRecords(batch, resp)
			return nil
		}
		return err
	}

	q.successCount.Add(int64(len(batch)))
	return nil
}

func (q *Queue) retryFailedRecords(batch []types.Record, resp *firehose.PutRecordBatchOutput) {
	retryRecords := make([]types.Record, 0, *resp.FailedPutCount)

	for i, record := range batch {
		if resp.RequestResponses != nil && resp.RequestResponses[i].ErrorCode != nil {
			q.retryCount.Add(1)
			retryRecords = append(retryRecords, record)
		}
	}
	q.push(retryRecords...)
}
func (q *Queue) handleError(err error, r []types.Record) {
	if q.errorHandler != nil {
		q.errorHandler(err, r)
		return
	}
	log.Println(err)
}

func (q *Queue) len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.queue)
}

func (q *Queue) inFlightBatch() bool {
	return atomic.LoadInt32(&q.inFlightBatchCounter) > 0
}

func (q *Queue) remaining() bool {
	// XXX race
	return q.len() > 0 || q.inFlightBatch()
}

func (q *Queue) push(r ...types.Record) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(q.queue, r...)
}
