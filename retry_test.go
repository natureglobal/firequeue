package firequeue

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestIsRetryable(t *testing.T) {
	operationError := func(err error) error {
		return &smithy.OperationError{ServiceID: "Firehose", OperationName: "PutRecordBatch", Err: err}
	}
	responseError := func(statusCode int, err error) error {
		return operationError(&awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: statusCode}},
				Err:      err,
			},
		})
	}
	apiError := func(statusCode int, code string) error {
		return responseError(statusCode, &smithy.GenericAPIError{Code: code, Message: "msg"})
	}
	sendError := func(err error) error {
		return operationError(&smithyhttp.RequestSendError{Err: err})
	}
	urlError := func(err error) error {
		return &url.Error{Op: "Post", URL: "https://firehose.ap-northeast-1.amazonaws.com/", Err: err}
	}
	opError := func(op string, err error) error {
		return &net.OpError{Op: op, Net: "tcp", Err: err}
	}

	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{"429", apiError(429, "TooManyRequests"), true},
		{"500", apiError(500, "ServiceUnavailableException"), true},
		{"502", apiError(502, "BadGateway"), true},
		{"503", apiError(503, "ServiceUnavailableException"), true},
		{"504", apiError(504, "GatewayTimeout"), true},
		{"501", apiError(501, "NotImplemented"), false},
		{"400 throttle code", apiError(400, "ThrottlingException"), true},
		{"400 retryable code", apiError(400, "RequestTimeout"), true},
		{"400 expired creds code", apiError(400, "ExpiredTokenException"), true},
		{"400 invalid argument", apiError(400, "InvalidArgumentException"), false},
		{"400 resource not found", apiError(400, "ResourceNotFoundException"), false},
		{"400 limit exceeded", apiError(400, "LimitExceededException"), false},
		{"503 after max attempts", &retry.MaxAttemptsError{Attempt: 3, Err: apiError(503, "ServiceUnavailableException")}, true},
		{"canceled", operationError(&smithy.CanceledError{Err: context.DeadlineExceeded}), false},
		{"canceled while retry backoff", operationError(&aws.RequestCanceledError{Err: context.Canceled}), false},
		{"context done before attempt", operationError(context.Canceled), false},
		{"validation", operationError(&smithy.InvalidParamsError{Context: "PutRecordBatchInput"}), false},
		{"dial", sendError(urlError(opError("dial", errors.New("no such host")))), true},
		{"connection refused", sendError(urlError(opError("dial", errors.New("connect: connection refused")))), true},
		{"read: connection reset by peer", sendError(urlError(opError("read", errors.New("connection reset by peer")))), true},
		{"write: connection reset by peer", sendError(urlError(opError("write", errors.New("connection reset by peer")))), true},
		{"write: broken pipe", sendError(urlError(opError("write", errors.New("broken pipe")))), true},
		{"use of closed network connection", sendError(urlError(opError("read", net.ErrClosed))), true},
		{"read: not temporary", sendError(urlError(opError("read", errors.New("no route to host")))), false},
		{"EOF", sendError(urlError(io.EOF)), true},
		{"request canceled", sendError(urlError(errors.New("net/http: request canceled"))), false},
		{"deserialization", responseError(200, &smithy.DeserializationError{Err: errors.New("unexpected end of JSON input")}), true},
		{"deserialization of 400", responseError(400, &smithy.DeserializationError{Err: errors.New("unexpected end of JSON input")}), true},
		{"deserialization without cause", responseError(200, &smithy.DeserializationError{}), false},
		{"send without cause", sendError(nil), true},
		{"unknown", errors.New("unknown"), false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryable(tc.err); got != tc.want {
				t.Errorf("isRetryable(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}
