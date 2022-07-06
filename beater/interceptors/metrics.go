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

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/elastic/apm-server/beater/request"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/elastic/elastic-agent-libs/monitoring"
)

// Metrics returns a grpc.UnaryServerInterceptor that increments metrics
// for gRPC method calls. The full gRPC method name will be used to look
// up a monitoring map in any of the given maps; the last one wins.
func Metrics(
	logger *logp.Logger,
	methodMetrics ...map[string]map[request.ResultID]*monitoring.Int,
) grpc.UnaryServerInterceptor {
	allMethodMetrics := make(map[string]map[request.ResultID]*monitoring.Int)
	for _, methodMetrics := range methodMetrics {
		for method, metrics := range methodMetrics {
			allMethodMetrics[method] = metrics
		}
	}
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		var resp interface{}
		err := handleMetrics(func() (err error) {
			resp, err = handler(ctx, req)
			return err
		}, logger, info.FullMethod, allMethodMetrics)
		return resp, err
	}
}

// MetricsStream returns a grpc.StreamServerInterceptor that increments metrics
// for gRPC method calls. The full gRPC method name will be used to look up a
// monitoring map in any of the given maps; the last one wins.
func MetricsStream(
	logger *logp.Logger,
	methodMetrics ...map[string]map[request.ResultID]*monitoring.Int,
) grpc.StreamServerInterceptor {
	allMethodMetrics := make(map[string]map[request.ResultID]*monitoring.Int)
	for _, methodMetrics := range methodMetrics {
		for method, metrics := range methodMetrics {
			allMethodMetrics[method] = metrics
		}
	}
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		m, ok := allMethodMetrics[info.FullMethod]
		if !ok {
			logger.With(
				"grpc.request.method", info.FullMethod,
			).Error("metrics registry missing")
		}
		return handler(srv, metricsWrapper{ServerStream: ss, m: m})
	}
}

func handleMetrics(handler func() error, logger *logp.Logger, method string, allMethodMetrics map[string]map[request.ResultID]*monitoring.Int) error {
	m, ok := allMethodMetrics[method]
	if !ok {
		logger.With(
			"grpc.request.method", method,
		).Error("metrics registry missing")
		return handler()
	}

	m[request.IDRequestCount].Inc()
	defer m[request.IDResponseCount].Inc()
	return handleMetricsError(handler(), m)
}

func handleMetricsError(err error, m map[request.ResultID]*monitoring.Int) error {
	responseID := request.IDResponseValidCount
	if err != nil {
		responseID = request.IDResponseErrorsCount
		if s, ok := status.FromError(err); ok {
			switch s.Code() {
			case codes.Unauthenticated:
				m[request.IDResponseErrorsUnauthorized].Inc()
			case codes.DeadlineExceeded:
				m[request.IDResponseErrorsTimeout].Inc()
			case codes.ResourceExhausted:
				m[request.IDResponseErrorsRateLimit].Inc()
			}
		}
	}
	m[responseID].Inc()
	return err
}

type metricsWrapper struct {
	grpc.ServerStream
	m map[request.ResultID]*monitoring.Int
}

func (w metricsWrapper) RecvMsg(m interface{}) error {
	err := w.ServerStream.RecvMsg(m)
	handleMetricsError(err, w.m)
	return nil
}
