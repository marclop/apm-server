package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	v2 "github.com/elastic/apm-server/modelproto/v2"
	"github.com/gofrs/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	serverURL string
	token     string
)

func main() {
	flag.StringVar(&serverURL, "server", "http://localhost:8200", "")
	flag.StringVar(&token, "secret-token", "", "")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	baseURL, err := url.Parse(serverURL)
	fatalErr(err)

	var opts []grpc.DialOption
	if baseURL.Scheme != "https" {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.Dial(baseURL.Host, opts...)
	fatalErr(err)
	defer conn.Close()

	if token != "" {
		ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("Authorization", fmt.Sprintf("Bearer %s", token)))
	}
	rpc, err := v2.NewIntakeServiceClient(conn).IntakeEvents(ctx)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			res, err := rpc.Recv()
			if err != io.EOF {
				fatalErr(err)
			}
			for _, e := range res.GetErrors() {
				log.Println("message:", e.GetMessage())
			}
		}
	}()
	fatalErr(err)

	before, err := processedEvents(serverURL)
	fatalErr(err)
	fatalErr(rpc.Send(newMeta()))
	traceID := uuid.Must(uuid.NewV4()).String()
	events := 10000
	wantEvents := events * 2
	t0 := time.Now()
	txID := uuid.Must(uuid.NewV4()).String()
	tx := newTransaction(traceID, txID)
	span := newSpan(traceID, txID, "")
	for i := 0; i < wantEvents/2; i++ {
		fatalErr(rpc.Send(tx))
		fatalErr(rpc.Send(span))
	}

	var after uint
	timeout := time.After(30 * time.Second)
	for after < uint(wantEvents) {
		select {
		case <-timeout:
			break
		default:
		}
		last, _ := processedEvents(serverURL)
		after = last - before
	}
	log.Printf("grpc: processed %d events in %s", wantEvents, time.Now().Sub(t0).String())
	fatalErr(rpc.CloseSend())

	var metaBuf bytes.Buffer
	err = json.NewEncoder(&metaBuf).Encode(newMeta())
	fatalErr(err)
	contents, err := os.ReadFile("out.ndjson")

	body := bytes.NewReader(contents)
	// var loopBuf bytes.Buffer
	// loopBufEnc := json.NewEncoder(&loopBuf)
	t0 = time.Now()

	for i := 0; i < wantEvents/10; i++ {
		// _, err = loopBuf.Write(metaBuf.Bytes())
		// fatalErr(err)
		// txID := uuid.Must(uuid.NewV4()).String()
		// err = loopBufEnc.Encode(newTransaction(traceID, txID))
		// fatalErr(err)
		// err = loopBufEnc.Encode(newSpan(traceID, txID, ""))
		// loopBuf.WriteString("\n")
		// fatalErr(err)
		req, err := http.NewRequest("POST", serverURL+"/intake/v2/events", body)
		fatalErr(err)

		// os.WriteFile("out.ndjson", loopBuf.Bytes(), 0666)
		res, err := http.DefaultClient.Do(req)
		fatalErr(err)
		defer res.Body.Close()
		if res.StatusCode != 202 {
			log.Printf("status code not 202: %d", res.StatusCode)
			io.Copy(os.Stdout, res.Body)
			break
		}
		body.Seek(0, io.SeekStart)
	}

	before, _ = processedEvents(serverURL)
	for after < uint(wantEvents) {
		select {
		case <-timeout:
			break
		default:
		}
		last, _ := processedEvents(serverURL)
		after = last - before
	}
	log.Printf("intake-v2: processed %d events in %s", wantEvents, time.Now().Sub(t0).String())
}

func fatalErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func newMeta() *v2.Event {
	return &v2.Event{
		Metadata: &v2.Event_Metadata{
			Cloud: &v2.Event_Metadata_Cloud{
				Provider: "cloudprovider",
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
				"common":  newStructPbValue("value"),
				"another": newStructPbValue("value"),
				"last":    newStructPbValue("value"),
				"number":  newStructPbValue(1),
			}},
		},
	}
}

func newTransaction(traceID, txID string) *v2.Event {
	// Include a metadata struct in all events so the first event can have
	// its metadata read.
	duration := 10.1
	sampleRate := float32(1)
	trueP := true
	return &v2.Event{
		Transaction: &v2.Event_Transaction{
			Id:      txID,
			TraceId: traceID,
			SpanCount: &v2.Event_Transaction_SpanCount{
				Started: 1,
			},
			Duration:   &duration,
			Sampled:    &trueP,
			SampleRate: &sampleRate,
			Result:     "HTTP2xx",
			Name:       "name",
			Type:       "type",
			Timestamp:  timestamppb.Now(),
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
					"key":  newStructPbValue("value"),
					"key2": newStructPbValue("value2"),
					"key3": newStructPbValue("value3"),
					"key4": newStructPbValue("value4"),
				}},
				Request: &v2.Context_CtxRequest{
					Body: &structpb.Struct{Fields: map[string]*structpb.Value{
						"raw_body": newStructPbValue([]byte("raw body")),
					}},
					Method: "GET",
					Url: &v2.Context_CtxRequest_CtxRequestURL{
						Raw:      "http://server/full/url?query=param&another=value",
						Full:     "http://server/full/url",
						Hostname: "server",
						Port:     &v2.Context_CtxRequest_CtxRequestURL_PortInt{PortInt: 80},
						Protocol: "http",
					},
					HttpVersion: "1.1",
					Env: &structpb.Struct{Fields: map[string]*structpb.Value{
						"APP_SECRET": newStructPbValue("redacted"),
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
				},
			},
		},
	}
}

func newSpan(traceID, txID, spanID string) *v2.Event {
	// Include a metadata struct in all events so the first event can have
	// its metadata read.
	duration := 1.1
	sampleRate := float32(1)
	if spanID == "" {
		spanID = uuid.Must(uuid.NewV4()).String()
	}
	return &v2.Event{
		Span: &v2.Event_Span{
			Id:            spanID,
			ParentId:      &txID,
			TransactionId: txID,
			TraceId:       traceID,
			Duration:      &duration,
			SampleRate:    &sampleRate,
			Name:          "name",
			Type:          "type",
			Action:        "query",
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
					"tag": newStructPbValue("value"),
				}},
			},
			Timestamp: timestamppb.Now(),
		},
	}
}

func newStructPbValue(val interface{}) *structpb.Value {
	v, err := structpb.NewValue(val)
	fatalErr(err)
	return v
}

func expvarMetric(srv string) (map[string]interface{}, error) {
	res, err := http.Get(srv + "/debug/vars")
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func processedEvents(srv string) (uint, error) {
	key := "libbeat.output.events.acked"
	vars, err := expvarMetric(srv)
	if err != nil {
		return 0, err
	}
	return uint(vars[key].(float64)), nil
}
