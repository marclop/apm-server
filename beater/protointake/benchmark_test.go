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
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elastic/apm-server/model"
	v2 "github.com/elastic/apm-server/modelproto/v2"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newMeta(t testing.TB) []byte {
	event := &v2.Event{
		Metadata: &v2.Event_Metadata{
			Cloud: &v2.Event_Metadata_Cloud{
				Project: &v2.IDName{
					Id:   "123",
					Name: "asd",
				},
				Account: &v2.IDName{
					Id:   "123",
					Name: "asd",
				},
				Instance: &v2.IDName{
					Id:   "123",
					Name: "asd",
				},
				AvailabilityZone: "xyz",
				Service:          &v2.Name{Name: "name"},
			},
			Service: &v2.Event_Metadata_Service{
				Agent: &v2.Agent{
					Name:    "apm-agent-go",
					Version: "2.0.0",
				},
				Environment: "production",
				Name:        "my-service",
				Id:          "abcxyz",
				Version:     "0.1.0",
				Language: &v2.NameVersion{
					Name:    "go",
					Version: "1.17.11",
				},
				Runtime: &v2.NameVersion{
					Name:    "gc", // stands for Go Compiler.
					Version: "1.17.11",
				},
				Node: &v2.Name{Name: "nodename"},
			},
			Process: &v2.Event_Metadata_Process{
				Argv:  []string{"-p", "80"},
				Pid:   10,
				Ppid:  1,
				Title: "wserver",
			},
			User: &v2.User{
				Domain: "elastic.co",
				Id:     &v2.User_IdInt{IdInt: 360},
				Email:  "uname@elastic.co",
				Name:   "uname",
			},
			Network: &v2.Network{Connection: &v2.Network_Connection{Type: "4G"}},
			System: &v2.Event_Metadata_System{
				Architecture:   "arm64",
				ConfiguredHost: "nexus",
				Container:      &v2.Event_Metadata_System_Container{Id: "123141"},
				Platform:       "linux",
				Kubernetes: &v2.Event_Metadata_System_Kubernetes{
					Namespace: "default",
					Node:      &v2.Name{Name: "1x7-1z"},
					Pod: &v2.Event_Metadata_System_Kubernetes_Pod{
						Name: "rs-wservice-prod",
						Uid:  "xysz1-21x123x-18231x-1231xm",
					},
				},
			},
			Labels: &structpb.Struct{Fields: map[string]*structpb.Value{
				"common":  newStructPbValue(t, "value"),
				"another": newStructPbValue(t, "value"),
				"last":    newStructPbValue(t, "value"),
				"number":  newStructPbValue(t, 1),
			}},
		},
	}
	raw, err := event.MarshalVT()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func newTransaction(t testing.TB) []byte {
	// Include a metadata struct in all events so the first event can have
	// its metadata read.
	duration := 10.1
	sampleRate := float32(1)
	trueP := true
	event := &v2.Event{
		Transaction: &v2.Event_Transaction{
			Id:         "2",
			ParentId:   "1",
			TraceId:    "123",
			SpanCount:  &v2.Event_Transaction_SpanCount{},
			Outcome:    v2.Outcome_OUTCOME_SUCCESS,
			Duration:   &duration,
			Sampled:    &trueP,
			SampleRate: &sampleRate,
			Result:     "HTTP2xx",
			Name:       "name",
			Type:       "type",
			Context: &v2.Context{
				Cloud: &v2.Context_Cloud{
					Origin: &v2.Context_Cloud_Origin{
						Region:   "region",
						Provider: "provider",
						Service: &v2.Context_Cloud_Origin_Service{
							Name: "name",
						},
						Account: &v2.Context_Cloud_Origin_Account{
							Id: "cloud-account-id",
						},
					},
				},
				Custom: &structpb.Struct{Fields: map[string]*structpb.Value{
					"key":  newStructPbValue(t, "value"),
					"key2": newStructPbValue(t, "value2"),
					"key3": newStructPbValue(t, "value3"),
					"key4": newStructPbValue(t, "value4"),
				}},
				Request: &v2.Context_CtxRequest{
					Body: &structpb.Struct{Fields: map[string]*structpb.Value{
						"raw_body": newStructPbValue(t, []byte("raw body")),
					}},
					Method: "GET",
					Url: &v2.Context_CtxRequest_CtxRequestURL{
						Raw:      "http://server/full/url?query=param&another=value",
						Full:     "http://server/full/url",
						Hostname: "server",
						Port:     &v2.Context_CtxRequest_CtxRequestURL_PortInt{PortInt: 80},
						Protocol: "http",
					},
					Headers: map[string]*v2.HTTPHeaderValue{
						"Content-Type": {Values: []string{"application/json"}},
					},
					HttpVersion: "1.1",
					Env: &structpb.Struct{Fields: map[string]*structpb.Value{
						"APP_SECRET": newStructPbValue(t, "redacted"),
					}},
					Socket: &v2.Context_CtxRequest_CtxRequestSocket{
						RemoteAddress: "https://aservice:443",
					},
				},
				Response: &v2.Context_CtxResponse{
					Finished:        &trueP,
					HeadersSent:     &trueP,
					StatusCode:      200,
					TransferSize:    1231312312,
					DecodedBodySize: 886621,
					EncodedBodySize: 12317,
					Headers: map[string]*v2.HTTPHeaderValue{
						"Content-Type": {Values: []string{"application/json"}},
					},
				},
			},
		},
	}
	raw, err := event.MarshalVT()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func newSpan(t testing.TB) []byte {
	// Include a metadata struct in all events so the first event can have
	// its metadata read.
	duration := 1.1
	sampleRate := float32(1)
	parentID := "2"
	event := &v2.Event{
		Span: &v2.Event_Span{
			Id:            "3",
			ParentId:      &parentID,
			TransactionId: parentID,
			TraceId:       "123",
			Outcome:       v2.Outcome_OUTCOME_SUCCESS,
			Duration:      &duration,
			SampleRate:    &sampleRate,
			Name:          "name",
			Type:          "type",
			Action:        "query",
			Composite: &v2.SpanComposite{
				Count:               2,
				Sum:                 100,
				CompressionStrategy: v2.CompressionStrategy_COMPRESSION_STRATEGY_EXACT_MATCH,
			},
			Context: &v2.SpanContext{
				Database: &v2.SpanContext_Database{
					Instance:     "mydb",
					Link:         "mysql://mydb/db",
					RowsAffected: 1200,
					Statement:    "SELECT * FROM clients;",
					Type:         "mysql",
					User:         "dbuser",
				},
				Destination: &v2.SpanContext_Destination{
					Address: "mysql://mydb:3306",
					Port:    3306,
				},
				Service: &v2.CtxService{
					Name: "wservice",
					Target: &v2.NameType{
						Name: "mysql",
						Type: "db",
					},
				},
				Tags: &structpb.Struct{Fields: map[string]*structpb.Value{
					"tag": newStructPbValue(t, "value"),
				}},
			},
			Timestamp: timestamppb.Now(),
		},
	}
	raw, err := event.MarshalVT()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func newStructPbValue(t testing.TB, val interface{}) *structpb.Value {
	v, err := structpb.NewValue(val)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func BenchmarkEvent(b *testing.B) {
	b.Run("normal", func(b *testing.B) {
		benchmarkEvents(b, false)
	})
	b.Run("pool", func(b *testing.B) {
		benchmarkEvents(b, true)
	})
}

func benchmarkEvents(b *testing.B, pool bool) {
	proc := mockProcessor{}
	srv := NewServer(&proc)
	srv.usePool = pool
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mockIntake := newMockService(ctx, b)
	defer mockIntake.close()
	go func() {
		if err := srv.IntakeEvents(mockIntake); err != nil {
			switch err {
			case context.Canceled, context.DeadlineExceeded, io.EOF:
			default:
				b.Fatal(err)
				cancel()
			}
		}
	}()
	rawMeta := newMeta(b)
	rawTx := newTransaction(b)
	rawSpan := newSpan(b)
	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		mockIntake.clientSendBytes(append(rawMeta))
		for i := 0; p.Next(); i++ {
			if i%3 == 0 {
				mockIntake.clientSendBytes(append(rawTx))
			} else {
				mockIntake.clientSendBytes(append(rawSpan))
			}
		}
	})
}

type mockProcessor struct {
	count uint64
}

func (p *mockProcessor) ProcessBatch(_ context.Context, b *model.Batch) error {
	atomic.AddUint64(&p.count, uint64(len(*b)))
	return nil
}
