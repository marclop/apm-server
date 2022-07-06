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

package protointake

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/elastic/apm-server/model"
	v2 "github.com/elastic/apm-server/modelproto/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func newMockService(ctx context.Context, t testing.TB) *serviceMock {
	return &serviceMock{
		ctx:     ctx,
		eventsC: make(chan []byte),
		closed:  make(chan struct{}),
	}
}

type serviceMock struct {
	grpc.ServerStream
	ctx     context.Context
	eventsC chan []byte
	closed  chan struct{}
}

func (m *serviceMock) Context() context.Context { return m.ctx }

func (m *serviceMock) close() { close(m.closed) }

func (m *serviceMock) RecvMsg(msg interface{}) error {
	if event, ok := msg.(*v2.Event); ok {
		select {
		case <-m.closed:
			return io.EOF
		case incoming := <-m.eventsC:
			return event.UnmarshalVT(incoming)
		}
	}
	return errors.New("invalid type")
}

func (m *serviceMock) clientSend(event *v2.Event) error {
	data, err := event.MarshalVT()
	if err != nil {
		return err
	}
	select {
	case <-m.ctx.Done():
		return m.ctx.Err()
	case <-m.closed:
	case m.eventsC <- data:
	}
	return nil
}

func (m *serviceMock) clientSendBytes(in []byte) {
	select {
	case <-m.ctx.Done():
	case <-m.closed:
	case m.eventsC <- in:
	}
}

func (m *serviceMock) Recv() (*v2.Event, error) {
	select {
	case <-m.closed:
	case incoming := <-m.eventsC:
		event := v2.Event{}
		event.UnmarshalVT(incoming)
		return &event, nil
	}
	return nil, nil
}

// Send is used by the IntakeServer to send responses back to the client.
func (m *serviceMock) Send(resp *v2.IntakeResponse) error {
	select {
	case <-m.ctx.Done():
		return m.ctx.Err()
	case <-m.closed:
	}
	return errors.New(resp.String())
}

func Test_decodeEvent(t *testing.T) {
	var event model.APMEvent
	err := decodeMetadata(&v2.Event_Metadata{
		Service: &v2.Event_Metadata_Service{
			Name: "my-service",
		},
	}, &event)
	require.NoError(t, err)
	ok, err := decodeEvent(&v2.Event{
		Transaction: &v2.Event_Transaction{
			Id:        "1",
			TraceId:   "123",
			SpanCount: &v2.Event_Transaction_SpanCount{},
		},
	}, &event)
	assert.True(t, ok)
	assert.NoError(t, err)
}
