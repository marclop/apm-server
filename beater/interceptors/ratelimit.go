// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package interceptors

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/elastic/apm-server/beater/ratelimit"
)

// AnonymousRateLimit returns a grpc.UnaryServerInterceptor that adds a rate limiter
// to the context of anonymous requests. RateLimit must be wrapped by the ClientMetadata
// and Authorization interceptor, as it requires the client's IP address and authorization.
func AnonymousRateLimit(store *ratelimit.Store) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		ctx, err := rateLimitContext(ctx, store)
		if err != nil {
			return nil, err
		}
		result, err := handler(ctx, req)
		return result, handleRateLimitError(err)
	}
}

// AnonymousRateLimitStream returns a grpc.StreamServerInterceptor that adds a rate limiter
// to the context of anonymous requests. RateLimit must be wrapped by the ClientMetadata
// and Authorization interceptor, as it requires the client's IP address and authorization.
func AnonymousRateLimitStream(store *ratelimit.Store) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		_ *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		ctx, err := rateLimitContext(ss.Context(), store)
		if err != nil {
			return err
		}
		return handleRateLimitError(handler(srv, wrapStream{ctx: ctx, ServerStream: ss}))
	}
}

func rateLimitContext(ctx context.Context, store *ratelimit.Store) (context.Context, error) {
	details, ok := AuthenticationDetailsFromContext(ctx)
	if !ok {
		return ctx, errors.New("authentication details not found in context")
	}
	if details.Method == "" {
		clientMetadata, ok := ClientMetadataFromContext(ctx)
		if !ok {
			return ctx, errors.New("client metadata not found in context")
		}
		limiter := store.ForIP(clientMetadata.ClientIP)
		if !limiter.Allow() {
			return ctx, status.Error(
				codes.ResourceExhausted,
				ratelimit.ErrRateLimitExceeded.Error(),
			)
		}
		ctx = ratelimit.ContextWithLimiter(ctx, limiter)
	}
	return ctx, nil
}

func handleRateLimitError(err error) error {
	if errors.Is(err, ratelimit.ErrRateLimitExceeded) {
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	return nil
}
