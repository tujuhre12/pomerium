package grpcutil

import (
	"context"

	"google.golang.org/grpc/metadata"
)

const ForwardedForKey = "x-forwarded-for"

func ForwardedForFromIncoming(ctx context.Context) []string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	return md[ForwardedForKey]
}

func WithOutgoingForwardedFor(ctx context.Context, forwardedFor []string) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = make(metadata.MD)
	}
	md[ForwardedForKey] = forwardedFor
	return metadata.NewOutgoingContext(ctx, md)
}
