package store

import (
	"context"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func (s *store) HandleConsistencyInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if strings.HasSuffix(info.FullMethod, "/AppendEntries") {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get("consistency"); len(vals) > 0 {
				c, _ := strconv.ParseInt(vals[0], 10, 32)
				s.consistency.Store(c)
			}
		}
	}
	return handler(ctx, req)
}
func (s *store) SetConsistencyInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	cons := s.consistency.Load()
	if strings.HasSuffix(method, "/AppendEntries") {
		ctx = metadata.AppendToOutgoingContext(ctx, "consistency", strconv.FormatInt(cons, 10))
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}
