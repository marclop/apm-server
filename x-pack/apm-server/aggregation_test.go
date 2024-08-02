// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/apm-server/internal/beater"
	"github.com/elastic/apm-server/internal/beater/config"
	"github.com/elastic/elastic-agent-libs/logp"
)

func Test_newAggregationProcessors(t *testing.T) {
	procs, err := newAggregationProcessors(beater.ServerParams{
		Logger: logp.NewLogger(t.Name()),
		Config: &config.Config{
			Aggregation: config.AggregationConfig{
				MaxServices:         10000,
				Transactions:        config.TransactionAggregationConfig{MaxGroups: 10000},
				ServiceTransactions: config.ServiceTransactionAggregationConfig{MaxGroups: 10000},
				ServiceDestinations: config.ServiceDestinationAggregationConfig{MaxGroups: 10000},
			},
		},
		BatchProcessor: modelpb.ProcessBatchFunc(func(context.Context, *modelpb.Batch) error { return nil }),
	})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() { // Start aggregator in the background.
		defer close(done)
		err := procs[0].Run()
		require.NoError(t, err)
	}()

	proc := modelpb.BatchProcessor(procs[0])
	events := 1000
	txBatch := make(modelpb.Batch, events)
	spanBatch := make(modelpb.Batch, events)
	for i := 0; i < events; i++ {
		txBatch[i] = newEvent(true, false)
		spanBatch[i] = newEvent(false, true)
	}
	err = proc.ProcessBatch(context.Background(), &txBatch)
	require.NoError(t, err)
	err = proc.ProcessBatch(context.Background(), &spanBatch)
	require.NoError(t, err)

	<-time.After(time.Second)
	require.NoError(t, procs[0].Stop(context.Background()))
	<-done
}

func newEvent(tx, span bool) *modelpb.APMEvent {
	sliceCount := 10000
	strSlice := make([]string, sliceCount)
	floatSlice := make([]float64, sliceCount)
	for i := 0; i < sliceCount; i++ {
		v := rand.Float64()
		strSlice = append(strSlice, fmt.Sprint(v))
		floatSlice = append(floatSlice, float64(v))
	}
	event := modelpb.APMEvent{
		Timestamp: uint64(time.Now().UnixNano()),
		Cloud:     &modelpb.Cloud{Provider: "aws", Region: "us-west-2"},
		Agent: &modelpb.Agent{
			Name:    "opentelemetry/java",
			Version: "1.15.0",
		},
		Service: &modelpb.Service{
			Name: "MCF",
			Framework: &modelpb.Framework{
				Name:    "io.opentelemetry.lettuce-5.1",
				Version: "11.0.23+9-LTS",
			},
			Language: &modelpb.Language{Name: "java"},
			Version:  "RETSMC-ZDWDEE-317",
		},
		Event: &modelpb.Event{
			Outcome: "success",
			SuccessCount: &modelpb.SummaryMetric{
				Count: 1,
			},
			Duration: 1097000,
		},
		Source: &modelpb.Source{
			Port: 6379,
		},
		Trace: &modelpb.Trace{
			Id: "a1d6d0e1833631f81d38d9add5d3f204",
		},
		NumericLabels: map[string]*modelpb.NumericLabelValue{
			"ab_otel_java_extension_duration":      {Value: 0, Global: true},
			"thread_id":                            {Value: 1178, Global: true},
			"otl_collector_ingest_timestamp_razor": {Value: 1722280211, Global: true},
			"slice_example":                        {Values: []float64{1, 2, 3, 4, 5, 6, 7}, Global: true},
			"big_slice":                            {Values: floatSlice, Global: true},
		},
		Labels: map[string]*modelpb.LabelValue{
			"big_slice":                             {Values: strSlice, Global: true},
			"slice_example":                         {Values: []string{"1", "2", "3", "4", "5", "6", "7"}, Global: true},
			"net_transport":                         {Value: "ip_tcp", Global: true},
			"aws_ecs_startedat":                     {Value: "2024-07-21T10:58:08.975968728Z", Global: true},
			"service_ecs_pipeline_version":          {Value: "1.3.606", Global: true},
			"db_statement":                          {Value: "ROLE", Global: true},
			"aws_ecs_cpu_model":                     {Value: "Intel(R)%20Xeon(R)%20Platinum%208259CL%20CPU", Global: true},
			"aws_ecs_task_arn":                      {Value: "arn:aws:ecs:us-west-2:123123131231:task/asdadbsadbasdhajdkahksdhsahdkashdajkhdasjakdhasda/3421039365e3405cb3ec5697729247e4", Global: true},
			"aws_ecs_cpu_threads_per_core":          {Value: "2", Global: true},
			"aws_ecs_cpu_cores":                     {Value: "1", Global: true},
			"aws_ecs_container_name":                {Value: "app", Global: true},
			"otl_collector_version":                 {Value: "0.42.0", Global: true},
			"service_language":                      {Value: "java11", Global: true},
			"service_appprefix":                     {Value: "mcf", Global: true},
			"otel_status_code":                      {Value: "OK", Global: true},
			"service_lob":                           {Value: "some", Global: true},
			"cloud_infrastructure_service":          {Value: "AWS_ECS_FARGATE", Global: true},
			"service_support_email":                 {Value: "Message_A_Llama@abc.com", Global: true},
			"otl_collector_url":                     {Value: "https://otelcollector.abc.c1.abc.com", Global: true},
			"telemetry_auto_version":                {Value: "1.15.0", Global: true},
			"otl_collector_ec2_instance":            {Value: "i-02e93a5768f12ee8e", Global: true},
			"otl_collector_aws_region":              {Value: "us-west-2", Global: true},
			"aws_ecs_task_family":                   {Value: "asdadbsadbasdhajdkahksdhsahdkashdajkhdasjakdhasda", Global: true},
			"thread_name":                           {Value: "lettuce-epollEventLoop-4-1", Global: true},
			"aws_ecs_limits_cpu":                    {Value: "1", Global: true},
			"aws_ecs_cpu_speed":                     {Value: "2.50GHz", Global: true},
			"service_runtime_environment":           {Value: "test", Global: true},
			"aws_ecs_cpu_cache_size":                {Value: "36608%20KB", Global: true},
			"otl_collector_distro":                  {Value: "opentelemetry-collector-contrib", Global: true},
			"otl_collector_aws_abaccount":           {Value: "abc-ABC-test", Global: true},
			"aws_ecs_cluster_arn":                   {Value: "arn:aws:ecs:us-west-2:123123131231:cluster/asdadbsadbasdhajdkahksdhsahdkashdajkhdasjakdhasda", Global: true},
			"otl_collector_forwarded_to_version":    {Value: "0.87.0", Global: true},
			"cloud_account_name":                    {Value: "abc-some-test", Global: true},
			"process_runtime_description":           {Value: "Red Hat, Inc. OpenJDK 64-Bit Server VM 11.0.23+9-LTS", Global: true},
			"aws_ecs_bamboo_image":                  {Value: "RETSMC-ZDWDEE-317", Global: true},
			"otl_collector_forwarded_from_name":     {Value: "ABC OpenTelemetry Collector - Rock", Global: true},
			"service_baseline":                      {Value: "green", Global: true},
			"otl_telemetry_type":                    {Value: "traces", Global: true},
			"otl_collector_forwarded_to_aws_region": {Value: "us-west-2", Global: true},
			"otl_collector_bamboo_buildresult":      {Value: "ABCDTR-VKJWCBT-122", Global: true},
			"service_instance":                      {Value: "7e3afdae-9e84-429c-adfe-9e51371362f2", Global: true},
			"otl_collector_platform":                {Value: "EC2", Global: true},
			"aws_ecs_platformversion":               {Value: "null", Global: true},
			"otl_loadbalancer":                      {Value: "ALB", Global: true},
			"service_family":                        {Value: "ConversationChannel", Global: true},
			"otl_collector_name":                    {Value: "ABC OpenTelemetry Collector - Rock", Global: true},
			"service_syslevel":                      {Value: "test", Global: true},
			"service_userlocation":                  {Value: "external", Global: true},
			"ab_otel_java_extension_version":        {Value: "beta-2", Global: true},
			"aws_ecs_capacityprovidername":          {Value: "FARGATE", Global: true},
			"db_system":                             {Value: "redis", Global: true},
			"otl_collector_ec2_hostname":            {Value: "ip-10-157-215-185.us-west-2.abc.c1.abc.com", Global: true},
			"aws_ecs_launchtype":                    {Value: "FARGATE", Global: true},
			"service_type":                          {Value: "ecs-v3", Global: true},
			"otl_collector_forwarded_to_name":       {Value: "ABC OpenTelemetry Collector - Razor", Global: true},
			"app_costcenter":                        {Value: "1234", Global: true},
			"otl_collector_facing":                  {Value: "private", Global: true},
			"aws_ecs_limits_memory_mb":              {Value: "2048", Global: true},
			"aws_ecs_cpu_siblings":                  {Value: "2", Global: true},
		},
	}
	if tx && span {
		panic("CANNOT!")
	}
	switch {
	case tx:
		event.Transaction = &modelpb.Transaction{
			Result:              "Success",
			Sampled:             true,
			RepresentativeCount: 1,
			Type:                "unknown",
			Name:                "ROLE",
			Id:                  "b8d5b259229df6e6",
		}
	case span:
		event.Span = &modelpb.Span{
			Id:                  "b8d5b259229df6e6",
			Type:                "app",
			Subtype:             "internal",
			Name:                "HealthcheckController.getHealthcheck",
			RepresentativeCount: 1,
		}
	}
	return &event
}
