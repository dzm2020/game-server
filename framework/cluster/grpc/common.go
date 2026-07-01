package grpc

import (
	"context"
	"errors"
	"game-server/framework/gen"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
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

func newClusterMessageWithBin(from, to *gen.PID, data []byte) (*gen.ClusterMessage, error) {
	if from == nil || to == nil {
		return nil, gen.ErrActorPidNil
	}
	return &gen.ClusterMessage{
		CreatedAt: time.Now().Unix(),
		SourcePid: from,
		TargetPid: to,
		Data:      data,
	}, nil
}

func toDialOptions(options ClientOptions) []grpc.DialOption {
	keepaliveOptions := options.Keepalive

	permitWithoutStream := defaultClientPermitWithoutStream
	if keepaliveOptions.PermitWithoutStream != nil {
		permitWithoutStream = *keepaliveOptions.PermitWithoutStream
	}

	return []grpc.DialOption{
		// 生产环境建议替换为 TLS 凭证（credentials.NewTLS），
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		//空闲无流时也允许发送保活 PING，防止通道切 Idle
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveOptions.Time,
			Timeout:             keepaliveOptions.Timeout,
			PermitWithoutStream: permitWithoutStream,
		}),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"` + options.LoadBalancingPolicy + `"}`),
	}
}
