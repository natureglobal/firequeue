firequeue
=======

[![Test Status](https://github.com/natureglobal/firequeue/workflows/test/badge.svg?branch=main)][actions]
[![Coverage Status](https://coveralls.io/repos/natureglobal/firequeue/badge.svg?branch=main)][coveralls]
[![MIT License](http://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)][license]
[![PkgGoDev](https://pkg.go.dev/badge/github.com/natureglobal/firequeue)][PkgGoDev]

[actions]: https://github.com/natureglobal/firequeue/actions?workflow=test
[coveralls]: https://coveralls.io/r/natureglobal/firequeue?branch=main
[license]: https://github.com/natureglobal/firequeue/blob/main/LICENSE
[PkgGoDev]: https://pkg.go.dev/github.com/natureglobal/firequeue

firequeue is to ensure putting items to Amazon Kinesis Data Firehose with an in-memory queue

## Synopsis

```go
import (
    "context"

    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/firehose"
)

func main() {
    sess := session.Must(session.NewSession())
    fh := firehose.New(sess)
    fq := firequeue.New(fh, "DELIVERY_STREAM_NAME")

    ctx := context.Background()
    go fq.Loop(ctx) // We should start looping before sending items

    err := fq.Enqueue(&firehose.Record{...})
    ...
}
```

## Description
firequeue owns an in-memory queue. `Enqueue` copies payloads; acceptance does not
guarantee persistence. Stop producers before cancelling `Loop`'s context. The queue
then rejects new input and drains with a fresh context, for at most 10 seconds by
default (`DrainTimeout` changes this). Always inspect `Loop`'s returned error: a
timeout reports the remaining event and payload-byte counts. Process loss can lose
pending data; ambiguous request outcomes can duplicate records.

By default each input remains a separate record. For newline-delimited JSON, callers
can opt into concatenation without re-encoding or adding delimiters:

```go
fq := firequeue.New(fh, "device-messages",
    firequeue.Aggregate(64*1024, 500*time.Millisecond),
)
```

The first event sets the aggregation deadline; later arrivals do not extend it.
Payloads larger than the target remain standalone. Delivery backlog and retries
can add latency after sealing. Each payload must contain 1–1,024,000 bytes. Each
request contains at most 500 records and 4,000,000 payload bytes, before Base64.

`MaxQueueLength` (default 100,000 events) and `MaxQueueBytes` (default 64 MiB of
payload, excluding allocation overhead) include aggregation, ready, retry, and
in-flight data. Capacity rejection is explicit. HTTP-success partial failures retry
only failed records with the same bytes. Invalid responses and ambiguous transport
failures retry all affected records. Retryable requests use exponential backoff.

`Stats` separates input events from attempted/acknowledged Firehose records.
`AcceptedEvents = SuccessfulEvents + FailedEvents + PendingEvents`.
Terminal API failures invoke `ErrorHandler` and increment `FailedEvents`; `Loop`
can still return nil once the queue is empty. Estimated billable bytes round each
attempt up to 5 KiB and do not represent an AWS invoice or SDK-internal retries.
`AggregationWaitObserver` measures creation-to-seal time once per record, when the
first send attempt starts; callbacks must be concurrency-safe and return promptly.

## Installation

```console
% go get github.com/natureglobal/firequeue
```

## Author

[Songmu](https://github.com/Songmu)
