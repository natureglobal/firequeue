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

    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/firehose"
    "github.com/aws/aws-sdk-go-v2/service/firehose/types"
)

func main() {
    ctx := context.Background()
    cfg, err := config.LoadDefaultConfig(ctx)
    ...
    fh := firehose.NewFromConfig(cfg)
    fq := firequeue.New(fh, "DELIVERY_STREAM_NAME")

    go fq.Loop(ctx) // We should start looping before sending items

    err = fq.Enqueue(types.Record{...})
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
