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

package systemtest_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.elastic.co/apm/v2"
	"golang.org/x/sync/errgroup"

	"github.com/elastic/apm-server/systemtest"
	"github.com/elastic/apm-server/systemtest/apmservertest"
	"github.com/elastic/apm-server/systemtest/estest"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

func TestDropUnsampled(t *testing.T) {
	systemtest.CleanupElasticsearch(t)
	srv := apmservertest.NewUnstartedServer(t)
	srv.Config.Monitoring = newFastMonitoringConfig()
	srv.Config.RUM = &apmservertest.RUMConfig{
		Enabled: true,
	}
	srv.Config.AgentAuth.Anonymous = &apmservertest.AnonymousAuthConfig{
		Enabled: true,
	}
	err := srv.Start()
	require.NoError(t, err)

	// Sampled transaction (should be stored)
	tracer := srv.Tracer()
	tx := tracer.StartTransactionOptions("sampled", "TestDropUnsampled", apm.TransactionOptions{
		Start: time.Unix(0, 0), // set timestamp for sorting purposes
	})
	tx.Duration = time.Second
	tx.End()
	tracer.Flush(nil)

	// Unsampled backend transaction (should be dropped)
	systemtest.SendBackendEventsLiteral(t, srv, `
{"metadata":{"service":{"name":"allowed","version":"1.0.0","agent":{"name":"backend","version":"0.0.0"}}}}
{"transaction":{"sampled":false,"trace_id":"xyz","id":"yz","type":"TestDropUnsampled","duration":0,"span_count":{"started":1},"context":{"service":{"name":"allowed"}}}}`[1:])

	// Unsampled RUM transaction (should be stored)
	systemtest.SendRUMEventsLiteral(t, srv, `
{"metadata":{"service":{"name":"allowed","version":"1.0.0","agent":{"name":"rum-js","version":"0.0.0"}}}}
{"transaction":{"sampled":false,"trace_id":"x","id":"y","type":"TestDropUnsampled","duration":0,"span_count":{"started":1},"context":{"service":{"name":"allowed"}}}}`[1:])

	result := systemtest.Elasticsearch.ExpectMinDocs(t, 2, "traces-apm*", estest.TermQuery{
		Field: "transaction.type",
		Value: "TestDropUnsampled",
	})
	assert.Len(t, result.Hits.Hits, 2)
	systemtest.ApproveEvents(t, t.Name(), result.Hits.Hits,
		// RUM timestamps are set by the server based on the time the payload is received.
		"@timestamp", "timestamp.us",
		// RUM events have the source port recorded, and in the tests it will be dynamic
		"source.port",
		// Ignore dynamically generated trace/transaction ID
		"trace.id", "transaction.id",
	)

	doc := getBeatsMonitoringStats(t, srv, nil)
	transactionsDropped := gjson.GetBytes(doc.RawSource, "beats_stats.metrics.apm-server.sampling.transactions_dropped")
	assert.Equal(t, int64(1), transactionsDropped.Int())
}

func TestTailSampling(t *testing.T) {
	systemtest.CleanupElasticsearch(t)

	apmIntegration1 := newAPMIntegration(t, map[string]interface{}{
		"tail_sampling_enabled":  true,
		"tail_sampling_interval": "1s",
		"tail_sampling_policies": []map[string]interface{}{{"sample_rate": 0.5}},
	})

	apmIntegration2 := newAPMIntegration(t, map[string]interface{}{
		"tail_sampling_enabled":  true,
		"tail_sampling_interval": "1s",
		"tail_sampling_policies": []map[string]interface{}{{"sample_rate": 0.5}},
	})

	const total = 200
	const expected = 100 // 50%

	tracer1 := apmIntegration1.Tracer
	tracer2 := apmIntegration2.Tracer
	for i := 0; i < total; i++ {
		parent := tracer1.StartTransaction("GET /", "parent")
		parent.Duration = time.Second * time.Duration(i+1)
		child := tracer2.StartTransactionOptions("GET /", "child", apm.TransactionOptions{
			TraceContext: parent.TraceContext(),
		})
		child.Duration = 500 * time.Millisecond * time.Duration(i+1)
		child.End()
		parent.End()
	}
	tracer1.Flush(nil)
	tracer2.Flush(nil)

	// Flush the data stream while the test is running, as we have no
	// control over the settings for the sampled traces index template.
	refreshPeriodically(t, 250*time.Millisecond, "traces-apm.sampled-*")

	for _, transactionType := range []string{"parent", "child"} {
		var result estest.SearchResult
		t.Logf("waiting for %d %q transactions", expected, transactionType)
		_, err := systemtest.Elasticsearch.Search("traces-*").WithQuery(estest.TermQuery{
			Field: "transaction.type",
			Value: transactionType,
		}).WithSize(total).Do(context.Background(), &result,
			estest.WithCondition(result.Hits.MinHitsCondition(expected)),
		)
		require.NoError(t, err)
		assert.Equal(t, expected, len(result.Hits.Hits), transactionType)
	}

	// Make sure apm-server.sampling.tail metrics are published. Metric values are unit tested.
	doc := apmIntegration1.getBeatsMonitoringStats(t, nil)
	assert.True(t, gjson.GetBytes(doc.RawSource, "apm-server.sampling.tail").Exists())

	// Check tail-sampling config is reported in telemetry.
	var state struct {
		APMServer struct {
			Sampling struct {
				Tail struct {
					Enabled  bool
					Policies int
				}
			}
		} `mapstructure:"apm-server"`
	}
	apmIntegration1.getBeatsMonitoringState(t, &state)
	assert.True(t, state.APMServer.Sampling.Tail.Enabled)
	assert.Equal(t, 1, state.APMServer.Sampling.Tail.Policies)
}

func TestTailSamplingUnlicensed(t *testing.T) {
	// Start an ephemeral Elasticsearch container with a Basic license to
	// test that tail-based sampling requires a platinum or trial license.
	es, err := systemtest.NewUnstartedElasticsearchContainer()
	require.NoError(t, err)
	es.Env["xpack.license.self_generated.type"] = "basic"
	require.NoError(t, es.Start())
	defer es.Close()

	// Data streams are required for tail-based sampling, but since we're using
	// an ephemeral Elasticsearch container it's not straightforward to install
	// the integration package. We won't be indexing anything, so just don't wait
	// for the integration package to be installed in this test.
	waitForIntegration := false
	srv := apmservertest.NewUnstartedServer(t)
	srv.Config.Output.Elasticsearch.Hosts = []string{es.Addr}
	srv.Config.WaitForIntegration = &waitForIntegration
	srv.Config.Sampling = &apmservertest.SamplingConfig{
		Tail: &apmservertest.TailSamplingConfig{
			Enabled:  true,
			Interval: time.Second,
			Policies: []apmservertest.TailSamplingPolicy{{SampleRate: 0.5}},
		},
	}
	require.NoError(t, srv.Start())

	// Send some transactions to trigger an indexing attempt.
	tracer := srv.Tracer()
	for i := 0; i < 100; i++ {
		tx := tracer.StartTransaction("GET /", "parent")
		tx.Duration = time.Second * time.Duration(i+1)
		tx.End()
	}
	tracer.Flush(nil)

	timeout := time.After(time.Minute)
	logs := srv.Logs.Iterator()
	var done bool
	for !done {
		select {
		case entry := <-logs.C():
			done = strings.Contains(entry.Message, "invalid license")
		case <-timeout:
			t.Fatal("timed out waiting for log message")
		}
	}

	// Due to the failing license check, APM Server will refuse to index anything.
	var result estest.SearchResult
	_, err = es.Client.Search("traces-apm*").Do(context.Background(), &result)
	assert.NoError(t, err)
	assert.Empty(t, result.Hits.Hits)

	// The server will wait for the enqueued events to be published before
	// shutting down gracefully, so shutdown forcefully.
	srv.Kill()
}

func TestTailSamplingReload(t *testing.T) {
	// This test aims to verify that when reconfiguring the Tail Based sampler,
	// there isn't any meaningful event loss.
	// Initially, I tried sending a continuous stream of events to verify there
	// wasn't any event loss but there appears to be event loss when going from
	// `tail_sampling_enabled: true` to `tail_sampling_enabled: false`. The main
	// hypothesis is that since the tail sampler has not been stopped, the events
	// processed and stored in the local sampled events (badgerDB), and when the
	// sampler is stopped those events aren't flushed unless the tail_sampling_interval
	// ticks and publishes the locally sampled events to Elasticsearch.
	systemtest.CleanupElasticsearch(t)

	const sampleRate = 0.5
	srv := newAPMIntegration(t, map[string]interface{}{
		"tail_sampling_enabled":  true,
		"tail_sampling_interval": "1s",
		"tail_sampling_policies": []map[string]interface{}{{"sample_rate": sampleRate}},
	})

	const initial = 200
	const remainder = 800
	const txType = "tx"
	expected := int(initial*sampleRate) + remainder
	// Send initial 200 transactions
	for i := 0; i < initial; i++ {
		parent := srv.Tracer.StartTransaction("GET /", txType)
		parent.Duration = time.Second * time.Duration(i+1)
		parent.End()
	}
	srv.Tracer.Flush(nil)
	time.Sleep(2 * time.Second)
	dumpStatsOut(t, srv)
	t.Log("updating the APM integration...")
	srv.updatePolicy(t, map[string]interface{}{"tail_sampling_enabled": false})
	t.Log("updated the APM integration")
	// Send transactions immediately, which should cause us to end up sampling
	// 50% of the 800 transactions sent since the processor hasn't been shut down.
	// 500 total sampled transactions at this point.
	for i := 0; i < remainder; i++ {
		parent := srv.Tracer.StartTransaction("GET /", txType)
		parent.Duration = time.Second * time.Duration(i+1)
		parent.End()
	}
	srv.Tracer.Flush(nil)
	dumpStatsOut(t, srv)
	defer func() {
		if t.Failed() {
			dumpStatsOut(t, srv)
		}
	}()
	// Wait for a second to allow the server to stop the sampler.
	time.Sleep(time.Second)
	// Send 400 transactions all of which should be sampled.
	// 900 total sampled transactions.
	for i := 0; i < remainder/2; i++ {
		parent := srv.Tracer.StartTransaction("GET /", txType)
		parent.Duration = time.Second * time.Duration(i+1)
		parent.End()
	}
	srv.Tracer.Flush(nil)
	var result estest.SearchResult
	t.Logf("waiting for %d %q transactions", expected, txType)
	total := initial + remainder
	_, err := systemtest.Elasticsearch.
		Search("traces-*").
		WithSize(total).
		WithQuery(estest.TermQuery{Field: "transaction.type", Value: txType}).
		Do(context.Background(), &result, estest.WithCondition(
			result.Hits.MinHitsCondition(expected)),
		)
	require.NoError(t, err)
	assert.Equal(t, expected, len(result.Hits.Hits), txType)
}

func dumpStatsOut(t testing.TB, srv apmIntegration) {
	fmt.Printf("%+v\n", srv.Tracer.Stats())
	out := make(map[string]interface{})
	srv.getBeatsMonitoringStats(t, &out)
	enc := json.NewEncoder(os.Stdout)
	enc.Encode(out)
}

func refreshPeriodically(t *testing.T, interval time.Duration, index ...string) {
	g, ctx := errgroup.WithContext(context.Background())
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		cancel()
		assert.NoError(t, g.Wait())
	})
	g.Go(func() error {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		allowNoIndices := true
		ignoreUnavailable := true
		request := esapi.IndicesRefreshRequest{
			Index:             index,
			AllowNoIndices:    &allowNoIndices,
			IgnoreUnavailable: &ignoreUnavailable,
		}
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
			if _, err := systemtest.Elasticsearch.Do(ctx, &request, nil); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
		}
	})
}
