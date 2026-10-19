package firequeue_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
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

type mockFirehose struct {
	firehoseiface.FirehoseAPI
	send func(context.Context, *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error)
}

func (f *mockFirehose) PutRecordBatchWithContext(ctx context.Context, input *firehose.PutRecordBatchInput, _ ...request.Option) (*firehose.PutRecordBatchOutput, error) {
	return f.send(ctx, input)
}

func success(input *firehose.PutRecordBatchInput) *firehose.PutRecordBatchOutput {
	entries := make([]*firehose.PutRecordBatchResponseEntry, len(input.Records))
	for i := range entries {
		entries[i] = &firehose.PutRecordBatchResponseEntry{RecordId: aws.String("accepted")}
	}
	return &firehose.PutRecordBatchOutput{FailedPutCount: aws.Int64(0), RequestResponses: entries}
}

type runningQueue struct {
	queue      *firequeue.Queue
	cancel     context.CancelFunc
	done       chan struct{}
	err        error
	allowError bool
}

func start(t *testing.T, f *mockFirehose, opts ...firequeue.Option) *runningQueue {
	t.Helper()
	options := []firequeue.Option{
		firequeue.DrainTimeout(250 * time.Millisecond),
		firequeue.ErrorHandler(func(error, []*firehose.Record) {}),
	}
	options = append(options, opts...)
	ctx, cancel := context.WithCancel(context.Background())
	r := &runningQueue{queue: firequeue.New(f, "device-messages", options...), cancel: cancel, done: make(chan struct{})}
	go func() {
		r.err = r.queue.Loop(ctx)
		close(r.done)
	}()
	t.Cleanup(func() {
		if err := r.stop(t); err != nil && !r.allowError {
			t.Error(err)
		}
	})
	return r
}

func (r *runningQueue) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	select {
	case <-r.done:
		return r.err
	case <-time.After(2 * time.Second):
		t.Fatal("queue did not stop within two seconds")
		return nil
	}
}

func enqueue(t *testing.T, q *firequeue.Queue, data []byte) {
	t.Helper()
	if err := q.Enqueue(&firehose.Record{Data: data}); err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatal("condition did not become true")
		}
	}
}

func assertBalanced(t *testing.T, s firequeue.Stats) {
	t.Helper()
	if s.AcceptedEvents != s.SuccessfulEvents+s.FailedEvents+s.PendingEvents {
		t.Fatalf("unbalanced events: %+v", s)
	}
}

func TestParallelWorkersWakeForBacklog(t *testing.T) {
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	f := &mockFirehose{send: func(ctx context.Context, input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return success(input), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	r := start(t, f, firequeue.Parallel(4), firequeue.BatchSize(1))
	for i := 0; i < 4; i++ {
		enqueue(t, r.queue, []byte("event"))
	}
	for i := 0; i < 4; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("not all parallel workers joined the backlog")
		}
	}
	once.Do(func() { close(release) })
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
}

func TestAggregationDeadlineWhileDeliveryBlocked(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var calls int
	var waitsMu sync.Mutex
	var waits []time.Duration
	f := &mockFirehose{send: func(ctx context.Context, input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
		calls++
		if calls == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return success(input), nil
	}}
	r := start(t, f, firequeue.Aggregate(16, 20*time.Millisecond), firequeue.AggregationWaitObserver(func(d time.Duration) {
		waitsMu.Lock()
		waits = append(waits, d)
		waitsMu.Unlock()
	}))
	enqueue(t, r.queue, bytes.Repeat([]byte("x"), 16))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first request did not start")
	}
	enqueue(t, r.queue, []byte("y"))
	time.Sleep(150 * time.Millisecond)
	once.Do(func() { close(release) })
	waitFor(t, func() bool { return r.queue.Stats().SuccessfulEvents == 2 })
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	waitsMu.Lock()
	defer waitsMu.Unlock()
	if len(waits) != 2 || waits[1] >= 100*time.Millisecond {
		t.Fatalf("aggregation waited for blocked delivery: %v", waits)
	}
}

func TestAggregationPreservesPayloads(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			var mu sync.Mutex
			var got [][]byte
			f := &mockFirehose{send: func(_ context.Context, input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				mu.Lock()
				defer mu.Unlock()
				for _, record := range input.Records {
					got = append(got, append([]byte(nil), record.Data...))
				}
				return success(input), nil
			}}
			var opts []firequeue.Option
			if enabled {
				opts = append(opts, firequeue.Aggregate(4, time.Hour))
			}
			r := start(t, f, opts...)
			for _, data := range []string{"a\n", "b\n", "c\n"} {
				enqueue(t, r.queue, []byte(data))
			}
			if err := r.stop(t); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			wantRecords := 3
			if enabled {
				wantRecords = 2
			}
			if len(got) != wantRecords || string(bytes.Join(got, nil)) != "a\nb\nc\n" {
				t.Fatalf("unexpected records: %q", got)
			}
			s := r.queue.Stats()
			assertBalanced(t, s)
			if s.SuccessfulEvents != 3 || s.Success != int64(wantRecords) || s.PendingBytes != 0 {
				t.Fatalf("unexpected stats: %+v", s)
			}
		})
	}
}

func TestAggregationBoundariesAndOwnership(t *testing.T) {
	var got [][]byte
	f := &mockFirehose{send: func(_ context.Context, input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
		for _, record := range input.Records {
			got = append(got, append([]byte(nil), record.Data...))
		}
		return success(input), nil
	}}
	r := start(t, f, firequeue.Aggregate(65536, time.Hour))
	var want [][]byte
	for _, size := range []int{65535, 65536, 65537, 1024000} {
		payload := bytes.Repeat([]byte("x"), size)
		want = append(want, append([]byte(nil), payload...))
		enqueue(t, r.queue, payload)
		payload[0] = 'z'
	}
	for _, record := range []*firehose.Record{nil, {}, {Data: make([]byte, 1024001)}} {
		if err := r.queue.Enqueue(record); err == nil {
			t.Fatal("invalid input was accepted")
		}
	}
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d changed", i)
		}
	}
}

func TestAggregationDeadlineDoesNotSlide(t *testing.T) {
	delivered := make(chan struct{}, 1)
	f := &mockFirehose{send: func(_ context.Context, input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
		select {
		case delivered <- struct{}{}:
		default:
		}
		return success(input), nil
	}}
	r := start(t, f, firequeue.Aggregate(65536, 30*time.Millisecond))
	enqueue(t, r.queue, []byte("first\n"))
	producerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-producerCtx.Done():
				return
			case <-ticker.C:
				if err := r.queue.Enqueue(&firehose.Record{Data: []byte("next\n")}); err != nil {
					return
				}
			}
		}
	}()
	select {
	case <-delivered:
	case <-time.After(150 * time.Millisecond):
		t.Error("continuous input postponed the first flush")
	}
	cancel()
	<-producerDone
}

func TestHTTPPartialFailureRetriesOnlyFailedRecord(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls int
	var got []string
	f := &mockFirehose{send: func(ctx context.Context, input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
		calls++
		for _, record := range input.Records {
			got = append(got, string(record.Data))
		}
		if calls == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		resp := success(input)
		if calls == 2 {
			if len(input.Records) != 2 {
				return nil, fmt.Errorf("expected two records, got %d", len(input.Records))
			}
			resp.FailedPutCount = aws.Int64(1)
			resp.RequestResponses[1] = &firehose.PutRecordBatchResponseEntry{ErrorCode: aws.String("InternalFailure")}
		}
		return resp, nil
	}}
	r := start(t, f, firequeue.Aggregate(4, time.Hour))
	enqueue(t, r.queue, []byte("warm"))
	<-entered
	for _, s := range []string{"a\n", "b\n", "c\n", "d\n"} {
		enqueue(t, r.queue, []byte(s))
	}
	close(release)
	waitFor(t, func() bool { return r.queue.Stats().RetryCount == 1 })
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, "|") != "warm|a\nb\n|c\nd\n|c\nd\n" {
		t.Fatalf("unexpected retry: %q", got)
	}
	s := r.queue.Stats()
	assertBalanced(t, s)
	if s.SuccessfulEvents != 5 || s.Success != 3 || s.SentRecords != 4 || s.EstimatedBillableBytes != 4*5120 {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

func TestBatchLimits(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		size, count, largest int
	}{
		{"record count", 1, 502, 500},
		{"payload bytes", 65536, 64, 61},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			calls, largest := 0, 0
			f := &mockFirehose{send: func(ctx context.Context, input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				calls++
				if calls == 1 {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				if len(input.Records) > largest {
					largest = len(input.Records)
				}
				var size int
				for _, record := range input.Records {
					size += len(record.Data)
				}
				if size > 4000000 || len(input.Records) > 500 {
					t.Errorf("invalid request: %d records, %d bytes", len(input.Records), size)
				}
				return success(input), nil
			}}
			r := start(t, f)
			enqueue(t, r.queue, bytes.Repeat([]byte("x"), tc.size))
			<-entered
			for i := 1; i < tc.count; i++ {
				enqueue(t, r.queue, bytes.Repeat([]byte("x"), tc.size))
			}
			close(release)
			if err := r.stop(t); err != nil {
				t.Fatal(err)
			}
			if largest != tc.largest || r.queue.Stats().SuccessfulEvents != int64(tc.count) {
				t.Fatalf("largest batch %d, stats %+v", largest, r.queue.Stats())
			}
		})
	}
}

func TestCapacityIncludesInFlightAndConcurrentInput(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		events, bytes, capacity int
	}{
		{"event limit", 5, 1000, 5},
		{"byte limit", 1000, 8, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			f := &mockFirehose{send: func(ctx context.Context, input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				once.Do(func() { close(entered) })
				select {
				case <-release:
					return success(input), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}}
			r := start(t, f, firequeue.MaxQueueLength(tc.events), firequeue.MaxQueueBytes(tc.bytes), firequeue.Parallel(2))
			enqueue(t, r.queue, []byte("x\n"))
			<-entered
			var wg sync.WaitGroup
			var accepted int64
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := r.queue.Enqueue(&firehose.Record{Data: []byte("y\n")}); err == nil {
						atomic.AddInt64(&accepted, 1)
					}
				}()
			}
			wg.Wait()
			s := r.queue.Stats()
			assertBalanced(t, s)
			if accepted+1 != int64(tc.capacity) || s.PendingBytes != int64(2*tc.capacity) || s.QueueFullError != int64(21-tc.capacity) {
				t.Fatalf("capacity escaped: %+v, accepted %d", s, accepted)
			}
			close(release)
			if err := r.stop(t); err != nil {
				t.Fatal(err)
			}
			if err := r.queue.Enqueue(&firehose.Record{Data: []byte("late")}); err == nil {
				t.Fatal("accepted input after shutdown")
			}
		})
	}
}

func TestCancellationRetriesAmbiguousRequestDuringDrain(t *testing.T) {
	entered := make(chan struct{})
	var calls int
	f := &mockFirehose{send: func(ctx context.Context, input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
		calls++
		if calls == 1 {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if ctx.Err() != nil {
			t.Error("drain used the canceled producer context")
		}
		return success(input), nil
	}}
	r := start(t, f)
	enqueue(t, r.queue, []byte("payload\n"))
	<-entered
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	s := r.queue.Stats()
	assertBalanced(t, s)
	if calls != 2 || s.SuccessfulEvents != 1 || s.RetryCount != 1 {
		t.Fatalf("unexpected cancellation accounting: %+v", s)
	}
}

func TestDrainTimeoutRetainsUnacknowledgedEvents(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	f := &mockFirehose{send: func(ctx context.Context, _ *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	r := start(t, f, firequeue.DrainTimeout(30*time.Millisecond))
	r.allowError = true
	enqueue(t, r.queue, []byte("retained\n"))
	<-entered
	err := r.stop(t)
	if err == nil || !strings.Contains(err.Error(), "1 events, 9 bytes pending") {
		t.Fatalf("unexpected drain result: %v", err)
	}
	s := r.queue.Stats()
	assertBalanced(t, s)
	if s.PendingEvents != 1 || s.SuccessfulEvents != 0 || s.FailedEvents != 0 {
		t.Fatalf("unacknowledged event disappeared: %+v", s)
	}
}

func TestMalformedResponsesDoNotAcknowledgeEvents(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *firehose.PutRecordBatchOutput
	}{
		{"nil response", nil},
		{"missing count", &firehose.PutRecordBatchOutput{}},
		{"missing entry", &firehose.PutRecordBatchOutput{FailedPutCount: aws.Int64(0)}},
		{"nil entry", &firehose.PutRecordBatchOutput{FailedPutCount: aws.Int64(0), RequestResponses: []*firehose.PutRecordBatchResponseEntry{nil}}},
		{"inconsistent count", &firehose.PutRecordBatchOutput{FailedPutCount: aws.Int64(1), RequestResponses: []*firehose.PutRecordBatchResponseEntry{{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			f := &mockFirehose{send: func(_ context.Context, input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				calls++
				if calls == 1 {
					return tc.response, nil
				}
				return success(input), nil
			}}
			r := start(t, f)
			enqueue(t, r.queue, []byte("event\n"))
			waitFor(t, func() bool { return r.queue.Stats().RetryCount == 1 })
			if r.queue.Stats().SuccessfulEvents != 0 {
				t.Fatal("malformed response acknowledged an event")
			}
			if err := r.stop(t); err != nil {
				t.Fatal(err)
			}
			if r.queue.Stats().SuccessfulEvents != 1 {
				t.Fatal("retry did not recover")
			}
		})
	}
}

func TestRetryableAndTerminalErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"429", awserr.NewRequestFailure(awserr.New("throttled", "", nil), 429, ""), true},
		{"500", awserr.NewRequestFailure(awserr.New("failed", "", nil), 500, ""), true},
		{"timeout", awserr.New(request.ErrCodeResponseTimeout, "timeout", nil), true},
		{"connection reset", fmt.Errorf("read: connection reset by peer"), true},
		{"invalid argument", awserr.New("InvalidArgumentException", "invalid", nil), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			f := &mockFirehose{send: func(_ context.Context, input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				calls++
				if calls == 1 {
					return nil, tc.err
				}
				return success(input), nil
			}}
			r := start(t, f)
			enqueue(t, r.queue, []byte("event\n"))
			waitFor(t, func() bool { s := r.queue.Stats(); return s.RetryCount > 0 || s.FailedEvents > 0 })
			if err := r.stop(t); err != nil {
				t.Fatal(err)
			}
			s := r.queue.Stats()
			assertBalanced(t, s)
			if tc.retry && (s.SuccessfulEvents != 1 || calls != 2) || !tc.retry && (s.FailedEvents != 1 || calls != 1) {
				t.Fatalf("unexpected disposition: %+v, calls %d", s, calls)
			}
		})
	}
}

func TestLoopCannotRestart(t *testing.T) {
	f := &mockFirehose{send: func(_ context.Context, input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
		return success(input), nil
	}}
	r := start(t, f)
	enqueue(t, r.queue, []byte("event\n"))
	if err := r.queue.Loop(context.Background()); err == nil || !strings.Contains(err.Error(), "already initialized") {
		t.Fatalf("second Loop: %v", err)
	}
}
