package grpc

import (
	"context"
	"errors"
	"game-server/framework/gen"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func isServerStreamClosedErr(err error) bool {
	return err == io.EOF || errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

func newClusterMessage(from, to *gen.PID, msg *gen.Message) (*gen.ClusterMessage, error) {
	data, err := gen.Encode(msg)
	if err != nil {
		return nil, err
	}
	return &gen.ClusterMessage{
		CreatedAt: time.Now().Unix(),
		SourcePid: from,
		TargetPid: to,
		Data:      data,
	}, nil
}
