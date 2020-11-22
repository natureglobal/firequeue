package firequeue_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/natureglobal/firequeue"
)

type testAWSError struct {
}

func (te *testAWSError) Error() string {
	return "retryable error!"
}

func (te *testAWSError) Code() string {
	return request.ErrCodeResponseTimeout
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

func (tf *testFirehose) PutRecord(*firehose.PutRecordInput) (*firehose.PutRecordOutput, error) {
	if rand.Intn(10) < 2 {
		// Letting them fail on purpose with a certain probability
		return nil, &testAWSError{}
	}
	atomic.AddUint32(&tf.counter, 1)
	return &firehose.PutRecordOutput{}, nil
}

func TestQueue(t *testing.T) {
	testCases := []struct {
		name string
		opts []firequeue.Option
	}{{
		name: "serial",
	}, {
		name: "parallel 10",
		opts: []firequeue.Option{
			firequeue.Parallel(10),
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tf := &testFirehose{}
			q := firequeue.New(tf, tc.opts...)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			errch := make(chan error)
			go func() {
				errch <- q.Loop(ctx)
			}()

			const trial = 10000
			go func() {
				for i := 0; i < trial; i++ {
					go q.Send(&firehose.PutRecordInput{})
				}
			}()

			if err := <-errch; err != nil {
				t.Errorf("error shoud be nil but: %s", err)
			}

			if trial != tf.counter {
				t.Errorf("got: %d, expect: %d", tf.counter, trial)
			}

			stats := q.Stats()
			valid := func(s firequeue.Stats) bool {
				if s.GiveupError > 0 || s.UnretryableError > 0 || s.QueueFullError > 0 || s.QueueLength > 0 {
					return false
				}
				if s.Success+s.RetrySuccess != trial {
					return false
				}
				return s.Success > s.RetrySuccess
			}
			if !valid(stats) {
				t.Errorf("invalid stats: %+v", stats)
			}
		})
	}
}

func TestQueue_Loop(t *testing.T) {
	tf := &testFirehose{}
	q := firequeue.New(tf,
		firequeue.MaxQueueLength(100),
		firequeue.ErrorHandler(func(err error, r *firehose.PutRecordInput) {}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.Loop(ctx)

	time.Sleep(50 * time.Millisecond)
	err := q.Loop(ctx)
	if err == nil || !strings.Contains(err.Error(), "already initialized") {
		t.Errorf("already initialized error should be occurred but: %s", err)
	}
}

func TestQueue_Send(t *testing.T) {
	tf := &testFirehose{}
	q := firequeue.New(tf)

	err := q.Send(nil)
	if err == nil || !strings.Contains(err.Error(), "loop has not yet started") {
		t.Errorf("loop has not yet started error should be occurred but: %s", err)
	}
}
