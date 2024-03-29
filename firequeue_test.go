package firequeue_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/natureglobal/firequeue"
)

type testAWSError struct {
}

func (te *testAWSError) Error() string {
	return "retryable error"
}

type errCodeMode string

const (
	retryable   errCodeMode = "retryable"
	unretryable errCodeMode = "unretryable"
)

var ecm = retryable

func (te *testAWSError) Code() string {
	switch ecm {
	case retryable:
		return request.ErrCodeResponseTimeout
	case unretryable:
		return request.CanceledErrorCode
	default:
		return ""
	}
}

func (te *testAWSError) Message() string {
	return "msg"
}

func (te *testAWSError) OrigErr() error {
	return fmt.Errorf("retryable error!")
}

var _ awserr.Error = (*testAWSError)(nil)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

type testFirehose struct {
	firehoseiface.FirehoseAPI
	counter uint32
}

// fhErrorRate is a rate of error occurrence.
// 0 means no error, 10 means always error.
var fhErrorRate = 3

func (tf *testFirehose) PutRecordBatch(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
	if rand.Intn(10) >= fhErrorRate {
		return &firehose.PutRecordBatchOutput{
			FailedPutCount: aws.Int64(0),
		}, nil
	}

	var failedPutCounter int64
	inputLength := len(input.Records)
	firehoseResp := make([]*firehose.PutRecordBatchResponseEntry, inputLength)
	for i := 0; i < inputLength; i++ {
		if rand.Intn(10) < fhErrorRate {
			firehoseResp[i] = &firehose.PutRecordBatchResponseEntry{
				ErrorCode:    aws.String("test_error"),
				ErrorMessage: aws.String("test_error_message"),
			}
			failedPutCounter++
		} else {
			firehoseResp[i] = &firehose.PutRecordBatchResponseEntry{
				RecordId: aws.String(fmt.Sprintf("%d", atomic.LoadUint32(&tf.counter))),
			}
		}
	}
	resp := &firehose.PutRecordBatchOutput{
		RequestResponses: firehoseResp,
		FailedPutCount:   aws.Int64(failedPutCounter),
	}

	if failedPutCounter > 0 {
		return resp, &testAWSError{}
	}
	return resp, nil
}

func (tf *testFirehose) PutRecordBatchWithContext(_ context.Context, r *firehose.PutRecordBatchInput, _ ...request.Option) (*firehose.PutRecordBatchOutput, error) {
	return tf.PutRecordBatch(r)
}

// success
func TestQueue(t *testing.T) {
	fhErrorRate = 3
	testCases := []struct {
		name  string
		times int
	}{
		{
			name:  "1 record",
			times: 1,
		},
		{
			name:  "50 records",
			times: 50,
		},
		{
			name:  "100 records",
			times: 100,
		},
		{
			name:  "1000 records",
			times: 1000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tf := &testFirehose{}
			q := firequeue.New(tf, "env", firequeue.BatchInterval(100*time.Millisecond))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go q.Loop(ctx)

			time.Sleep(1000 * time.Millisecond)

			for i := 0; i < tc.times; i++ {
				err := q.Enqueue(&firehose.Record{
					Data: []byte("test"),
				})
				if err != nil {
					t.Errorf("error should not be occurred but: %s", err)
				}
			}
			time.Sleep(10 * time.Second)
			stats := q.Stats()
			if stats.Success != int64(tc.times) {
				t.Log(stats)
				t.Errorf("Not all records were success. expected: %d, actual: %d", tc.times, stats.Success)
			}
			t.Log(q.Stats())

		})
	}
}

func TestQueue_Para(t *testing.T) {
	testCases := []struct {
		name  string
		times int
		para  int
	}{
		{
			name:  "parallel 2",
			times: 1000,
			para:  2,
		},
		{
			name:  "parallel 10",
			times: 1000,
			para:  10,
		},
		{
			name:  "parallel 100",
			times: 1000,
			para:  100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tf := &testFirehose{}
			q := firequeue.New(tf, "env", firequeue.BatchInterval(1*time.Second), firequeue.Parallel(tc.para))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go q.Loop(ctx)

			time.Sleep(1000 * time.Millisecond)

			for i := 0; i < tc.times; i++ {
				err := q.Enqueue(&firehose.Record{
					Data: []byte("test"),
				})
				if err != nil {
					t.Errorf("error should not be occurred but: %s", err)
				}
			}
			time.Sleep(10 * time.Second)
			stats := q.Stats()
			if stats.Success != int64(tc.times) {
				t.Log(stats)
				t.Errorf("Not all records were success. expected: %d, actual: %d", tc.times, stats.Success)
			}
			t.Log(q.Stats())

		})
	}
}

func TestQueue_CallLoopTwoTimes(t *testing.T) {
	tf := &testFirehose{}
	q := firequeue.New(tf, "env")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.Loop(ctx)

	time.Sleep(1000 * time.Millisecond)
	err := q.Loop(ctx)
	if err == nil || !strings.Contains(err.Error(), "already initialized") {
		fmt.Println(err)
		t.Errorf("already initialized error should be occurred but: %s", err)
	}
}

func TestQueue_SendWithoutLoop(t *testing.T) {
	tf := &testFirehose{}
	q := firequeue.New(tf, "env")

	err := q.Enqueue(nil)
	if err == nil || !strings.Contains(err.Error(), "loop has not yet started") {
		t.Errorf("loop has not yet started error should be occurred but: %s", err)
	}
}

func TestQueue_ExceedBatchSize(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("panic should be occurred but not")
		}
	}()
	tf := &testFirehose{}
	firequeue.New(tf, "env", firequeue.BatchSize(501))
}

func TestQueue_QueueLength(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("panic should be occurred but not")
		}
	}()
	tf := &testFirehose{}
	firequeue.New(tf, "env", firequeue.MaxQueueLength(-1))
}

func TestQueue_DrainProcess(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{"10 records", 10},
		{"50 records", 50},
		{"100 records", 100},
		{"1000 records", 1000},
		{"2000 records", 2000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := &testFirehose{}
			q := firequeue.New(tf, "env", firequeue.BatchInterval(1*time.Second))
			ctx, cancel := context.WithCancel(context.Background())
			go q.Loop(ctx)

			time.Sleep(1000 * time.Millisecond)

			for i := 0; i < tt.count; i++ {
				err := q.Enqueue(&firehose.Record{
					Data: []byte("test"),
				})
				if err != nil {
					t.Errorf("error should not be occurred but: %s", err)
				}
			}
			cancel()
			time.Sleep(10 * time.Second)
			stats := q.Stats()
			if stats.Success != int64(tt.count) {
				t.Log(stats)
				t.Errorf("Not all records were success. expected: %d, actual: %d", tt.count, stats.Success)
			}
			t.Log(q.Stats())
		})
	}
}

func TestQueue_DrainProcessPara(t *testing.T) {
	tests := []struct {
		name  string
		count int
		para  int
	}{
		{"parallel 2", 1000, 2},
		{"parallel 10", 1000, 10},
		{"parallel 100", 1000, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := &testFirehose{}
			q := firequeue.New(tf, "env", firequeue.BatchInterval(1*time.Second))
			ctx, cancel := context.WithCancel(context.Background())
			go q.Loop(ctx)

			time.Sleep(1000 * time.Millisecond)

			for i := 0; i < tt.count; i++ {
				err := q.Enqueue(&firehose.Record{
					Data: []byte("test"),
				})
				if err != nil {
					t.Errorf("error should not be occurred but: %s", err)
				}
			}
			cancel()
			time.Sleep(10 * time.Second)
			stats := q.Stats()
			if stats.Success != int64(tt.count) {
				t.Log(stats)
				t.Errorf("Not all records were success. expected: %d, actual: %d", tt.count, stats.Success)
			}
			t.Log(q.Stats())
		})
	}
}

// Test for stats work correctly.
var qLength bool

func TestStats(t *testing.T) {
	tf := &testFirehose{}
	q := firequeue.New(tf, "env", firequeue.BatchInterval(1*time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go q.Loop(ctx)
	time.Sleep(1000 * time.Millisecond)

	for i := 0; i < 100; i++ {
		err := q.Enqueue(&firehose.Record{
			Data: []byte("test"),
		})
		if err != nil {
			t.Errorf("error should not be occurred but: %s", err)
		}
		if q.Stats().QueueLength > 0 {
			qLength = true
		}
	}
	time.Sleep(10 * time.Second)
	stats := q.Stats()
	if stats.Success != 100 {
		t.Log(stats)
		t.Errorf("Not all records were success. expected: %d, actual: %d", 100, stats.Success)
	}
	if stats.BatchLength == 0 {
		t.Errorf("BatchLength should be more than 0 but: %d", stats.BatchLength)
	}
	if !qLength {
		t.Errorf("QueueLength should be true: %t", qLength)
	}
	t.Log(q.Stats())
}

func TestStats_QueueFullError(t *testing.T) {
	tf := &testFirehose{}
	q := firequeue.New(tf, "env", firequeue.BatchInterval(10*time.Second), firequeue.MaxQueueLength(1))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go q.Loop(ctx)
	time.Sleep(1000 * time.Millisecond)

	err := q.Enqueue(&firehose.Record{})
	if err != nil {
		t.Errorf("error should not be occurred but: %s", err)
	}
	err = q.Enqueue(&firehose.Record{})
	if err == nil {
		t.Errorf("error should be occurred but not")
	}
	stats := q.Stats()
	if stats.QueueFullError != 1 {
		t.Errorf("QueueFullError should be 1 but: %d", stats.QueueFullError)
	}
	t.Log(stats)
}

func TestStat_RetryCount(t *testing.T) {
	// It means always return retryable error.
	fhErrorRate = 10
	tf := &testFirehose{}
	q := firequeue.New(tf, "env")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go q.Loop(ctx)
	time.Sleep(1000 * time.Millisecond)
	for i := 0; i < 10; i++ {
		err := q.Enqueue(&firehose.Record{})
		if err != nil {
			t.Errorf("error should not be occurred but: %s", err)
		}
	}
	time.Sleep(5 * time.Second)
	stats := q.Stats()
	if stats.RetryCount == 0 {
		t.Errorf("retryCount should be more than 0 but: %d", stats.RetryCount)
	}
	t.Log(stats)
}

func TestStat_UnRetryableErrorCount(t *testing.T) {
	// It means always return unretryable error.
	fhErrorRate = 10
	ecm = unretryable

	tf := &testFirehose{}
	q := firequeue.New(tf, "env", firequeue.BatchSize(1))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go q.Loop(ctx)
	time.Sleep(1000 * time.Millisecond)
	err := q.Enqueue(&firehose.Record{})
	if err != nil {
		t.Errorf("error should not be occurred but: %s", err)
	}
	time.Sleep(5 * time.Second)
	stats := q.Stats()
	if stats.UnretryableError == 0 {
		t.Errorf("retryCount should be more than 0 but: %d", stats.RetryCount)
	}
	t.Log(stats)
}
