package firequeue

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/firehose"
)

const requestTimeout = 30 * time.Second

func (q *Queue) send(ctx context.Context, batch []*queuedRecord) bool {
	records := make([]*firehose.Record, len(batch))
	var billableBytes int64
	for i, record := range batch {
		records[i] = record.record
		billableBytes += int64((len(record.record.Data) + billingUnitBytes - 1) / billingUnitBytes * billingUnitBytes)
	}
	q.mu.Lock()
	q.stats.BatchLength = int64(len(batch))
	q.stats.SentRecords += int64(len(batch))
	q.stats.EstimatedBillableBytes += billableBytes
	q.mu.Unlock()
	requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	resp, err := q.firehose.PutRecordBatchWithContext(requestCtx, &firehose.PutRecordBatchInput{
		DeliveryStreamName: q.DeliveryStreamName,
		Records:            records,
	})
	if err != nil {
		if ctx.Err() != nil || requestCtx.Err() != nil || isRetryable(err) {
			q.retry(batch)
			if ctx.Err() == nil {
				q.handleError(err, records)
			}
			return true
		}
		q.finish(batch, false)
		q.handleError(err, records)
		return false
	}
	if err := validateResponse(resp, len(batch)); err != nil {
		q.retry(batch)
		q.handleError(err, records)
		return true
	}
	var succeeded, failed []*queuedRecord
	for i, record := range batch {
		if resp.RequestResponses[i].ErrorCode != nil && *resp.RequestResponses[i].ErrorCode != "" {
			failed = append(failed, record)
		} else {
			succeeded = append(succeeded, record)
		}
	}
	q.finish(succeeded, true)
	q.retry(failed)
	return len(failed) > 0
}

func validateResponse(resp *firehose.PutRecordBatchOutput, count int) error {
	if resp == nil || resp.FailedPutCount == nil || len(resp.RequestResponses) != count {
		return fmt.Errorf("invalid PutRecordBatch response for %d records", count)
	}
	var failed int64
	for _, entry := range resp.RequestResponses {
		if entry == nil {
			return fmt.Errorf("nil PutRecordBatch response entry")
		}
		if entry.ErrorCode != nil && *entry.ErrorCode != "" {
			failed++
		} else if entry.RecordId == nil || *entry.RecordId == "" {
			return fmt.Errorf("missing PutRecordBatch acknowledgment ID")
		}
	}
	if failed != *resp.FailedPutCount {
		return fmt.Errorf("inconsistent PutRecordBatch response: %d failures, declared %d", failed, *resp.FailedPutCount)
	}
	return nil
}

func (q *Queue) finish(batch []*queuedRecord, succeeded bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, record := range batch {
		delete(q.inFlight, record)
		q.stats.PendingEvents -= record.events
		q.stats.PendingBytes -= int64(len(record.record.Data))
		if succeeded {
			q.stats.Success++
			q.stats.SuccessfulEvents += record.events
		} else {
			q.stats.UnretryableError++
			q.stats.FailedEvents += record.events
		}
	}
}

func (q *Queue) retry(batch []*queuedRecord) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, record := range batch {
		delete(q.inFlight, record)
		q.stats.RetryCount++
		q.queue = append(q.queue, record)
	}
}

func (q *Queue) handleError(err error, records []*firehose.Record) {
	if q.errorHandler != nil {
		q.errorHandler(err, records)
		return
	}
	log.Println(err)
}

func isRetryable(err error) bool {
	if failure, ok := err.(awserr.RequestFailure); ok {
		switch failure.StatusCode() {
		case 429, 500, 502, 503, 504:
			return true
		}
	}
	return request.IsErrorRetryable(err) || request.IsErrorThrottle(err) ||
		strings.Contains(err.Error(), "read: connection reset by peer")
}
