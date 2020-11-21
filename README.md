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
    fq := firequeue.New(fh)

    ctx := context.Background()
    go fq.Loop(ctx) // We should start looping before sending items

    err := fq.Send(&firehose.PutRecordInput{...})
    ...
}
```

## Description
The firequeue utilizes an in-memory queue to ensure input to Amazon Kinesis Data Firehose.
When the looping process is cancelled by the context, the firequeue wait for the queue to be empty and then exit the loop.

## Installation

```console
% go get github.com/natureglobal/firequeue
```

## Author

[Songmu](https://github.com/Songmu)
