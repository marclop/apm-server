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
	"encoding/json"
	"errors"
	fmt "fmt"
	io "io"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/elastic-agent-libs/mapstr"
	"go.opentelemetry.io/collector/model/pdata"
	"google.golang.org/grpc"

	"github.com/elastic/apm-server/internal/netutil"
	"github.com/elastic/apm-server/model"
	"github.com/elastic/apm-server/model/modeldecoder/modeldecoderutil"
	v2 "github.com/elastic/apm-server/modelproto/v2"
	otel_processor "github.com/elastic/apm-server/processor/otel"
)

var defaultTimerPeriod = time.Second

type Server struct {
	v2.UnimplementedIntakeServiceServer
	processor model.BatchProcessor
	// This field is only used for benchmarking purposes.
	usePool bool
}

func NewServer(processor model.BatchProcessor) *Server {
	return &Server{processor: processor}
}

func (s *Server) IntakeEvents(svc v2.IntakeService_IntakeEventsServer) (err error) {
	var batch model.Batch
	var metadata *model.APMEvent
	// TODO(marclop) Good error handling. The current protobuf validation
	// doesn't fully cover all our custom decoder validation.
	// TODO(marclop) explore sending some metadata back to the client, initially
	// so it could potentially make some decisions depending on the server version?
	ctx := svc.Context()
	timer := time.NewTimer(defaultTimerPeriod)
	for err == nil {
		select {
		// Context done, return immediately.
		case <-ctx.Done():
			// NOTE(marclop) Unsure if this will work given context cancellations.
			sendError(svc, ctx.Err())
			break
		default:
		}

		var ok bool
		var event model.APMEvent
		// Once the first metadata event has been decoded, use it as the base
		// event for the rest of events.
		if metadata == nil {
			if err := s.apmEventMetadata(svc, &event); err != nil {
				return err
			}
			metadata = &event
			continue
		}
		event = metadata.Copy()
		ok, err = s.apmEventFromStream(svc, &event)
		// If the stream has ended, break and process batch.
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			// Otherwise, stream the error back to the client immediately.
			// in case the error is successfully sent, continue reading events.
			ok, err := sendError(svc, err)
			if err != nil {
				return err
			}
			if ok {
				// Set the error to nil to continue reading events.
				err = nil
				continue
			}
			// If the error wasn't a validation error, return it.
			return err
		}
		// If the decodeEvent function returned false, ignore the event.
		if !ok {
			continue
		}
		// If the metadata event is empty, first populate it and keep receiving
		// events.
		if metadata == nil {
			metadata = &event
			continue
		}

		var timerFired bool
		select {
		case <-timer.C:
			timerFired = true
		default:
		}
		// Send 10 events to be processed at a time.
		if batch = append(batch, event); len(batch) >= 10 || timerFired {
			if err := s.processor.ProcessBatch(ctx, &batch); err != nil {
				return err
			}
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(defaultTimerPeriod)
			// Reuse the allocated memory for the slice for the next iteration
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		return s.processor.ProcessBatch(ctx, &batch)
	}
	return err
}

func sendError(stream v2.IntakeService_IntakeEventsServer, err error) (bool, error) {
	res := v2.IntakeResponseFromVTPool()
	defer res.ReturnToVTPool()
	ok := addError(err, res)
	return ok, stream.Send(res)
}

func addError(err error, to *v2.IntakeResponse) bool {
	errs := err.Error()
	for _, msg := range strings.Split(errs, ";") {
		to.Errors = append(to.Errors, &v2.IntakeError{
			Message: msg,
		})
	}
	return len(to.Errors) > 0
}

func (s *Server) apmEventFromStream(stream grpc.ServerStream, out *model.APMEvent) (bool, error) {
	var event *v2.Event
	if s.usePool {
		// Get an event from the pool and release it back into the pool after the
		// event has been decoded into model.APMEvent.
		event = v2.EventFromVTPool()
		defer event.ReturnToVTPool()
	} else {
		event = &v2.Event{}
	}

	if err := stream.RecvMsg(event); err != nil {
		return false, err
	}
	return decodeEvent(event, out)
}

func (s *Server) apmEventMetadata(stream grpc.ServerStream, out *model.APMEvent) error {
	var event *v2.Event
	if s.usePool {
		// Get an event from the pool and release it back into the pool after the
		// event has been decoded into model.APMEvent.
		event = v2.EventFromVTPool()
		defer event.ReturnToVTPool()
	} else {
		event = &v2.Event{}
	}
	if err := stream.RecvMsg(event); err != nil {
		return err
	}
	return decodeMetadata(event.Metadata, out)
}

func decodeEvent(event *v2.Event, out *model.APMEvent) (bool, error) {
	if event.Metricset != nil {
		if err := event.Metricset.ValidateAll(); err != nil {
			return false, err
		}
		return decodeMetricset(event.Metricset, out), nil
	}
	if event.Span != nil {
		if err := event.Span.ValidateAll(); err != nil {
			return false, err
		}
		decodeSpan(event.Span, out)
		return true, nil
	}
	if event.Error != nil {
		if err := event.Error.ValidateAll(); err != nil {
			return false, err
		}
		decodeError(event.Error, out)
		return true, nil
	}
	if event.Transaction != nil {
		if err := event.Transaction.ValidateAll(); err != nil {
			return false, err
		}
		decodeTransaction(event.Transaction, out)
		return true, nil
	}
	return true, nil
}

func decodeMetadata(meta *v2.Event_Metadata, out *model.APMEvent) error {
	if meta == nil {
		return errors.New("metadata cannot be unset")
	}
	if err := meta.ValidateAll(); err != nil {
		return err
	}
	// Cloud
	if cloud := meta.GetCloud(); cloud != nil {
		out.Cloud.AccountID = cloud.GetAccount().GetId()
		out.Cloud.AccountName = cloud.GetAccount().GetName()
		out.Cloud.AvailabilityZone = cloud.GetAvailabilityZone()
		out.Cloud.InstanceID = cloud.GetInstance().GetId()
		out.Cloud.InstanceName = cloud.GetInstance().GetName()
		out.Cloud.MachineType = cloud.GetMachine().GetType()
		out.Cloud.ProjectID = cloud.GetProject().GetId()
		out.Cloud.ProjectName = cloud.GetProject().GetName()
		out.Cloud.Provider = cloud.GetProvider()
		out.Cloud.Region = cloud.GetRegion()
		out.Cloud.ServiceName = cloud.GetService().GetName()
	}
	// Labels
	// TODO(marclop) there may be more efficient ways to obtain the map.
	if labels := meta.GetLabels().AsMap(); len(labels) > 0 {
		modeldecoderutil.LabelsFrom(labels, out)
	}
	// Process
	if process := meta.GetProcess(); process != nil {
		if args := process.GetArgv(); len(args) > 0 {
			out.Process.Argv = append(out.Process.Argv[:0], args...)
		}
		out.Process.Pid = int(process.GetPid())
		pid := int(process.GetPpid())
		out.Process.Ppid = &pid
		out.Process.Title = process.GetTitle()
	}
	// Service
	if svc := meta.GetService(); svc != nil {
		out.Agent.EphemeralID = svc.GetAgent().GetEphemeralId()
		out.Agent.Name = svc.GetAgent().GetName()
		out.Agent.Version = svc.GetAgent().GetVersion()
		out.Service.Environment = svc.GetEnvironment()
		out.Service.Framework.Name = svc.GetFramework().GetName()
		out.Service.Framework.Version = svc.GetFramework().GetVersion()
		out.Service.Language.Name = svc.GetLanguage().GetName()
		out.Service.Language.Version = svc.GetLanguage().GetVersion()
		out.Service.Name = svc.GetName()
		out.Service.Node.Name = svc.GetNode().GetName()
		out.Service.Runtime.Name = svc.GetRuntime().GetName()
		out.Service.Runtime.Version = svc.GetRuntime().GetVersion()
		out.Service.Version = svc.GetVersion()
	}
	// System
	if system := meta.GetSystem(); system != nil {
		out.Host.Architecture = system.GetArchitecture()
		out.Host.Name = system.GetConfiguredHost()
		out.Container.ID = system.GetContainer().GetId()
		out.Host.Hostname = system.GetDetectedHostname()
		out.Kubernetes.Namespace = system.GetKubernetes().GetNamespace()
		out.Kubernetes.NodeName = system.GetKubernetes().GetNode().GetName()
		out.Kubernetes.PodName = system.GetKubernetes().GetPod().GetName()
		out.Kubernetes.PodUID = system.GetKubernetes().GetPod().GetUid()
		out.Host.OS.Platform = system.GetPlatform()
	}
	// User
	if user := meta.GetUser(); user != nil {
		out.User.Domain = user.GetDomain()
		if i := user.GetIdInt(); i > 0 {
			out.User.ID = strconv.Itoa(int(i))
		}
		if v := user.GetIdStr(); v != "" {
			out.User.ID = v
		}
		out.User.Email = user.GetEmail()
		out.User.Name = user.GetName()
	}
	// Network
	out.Network.Connection.Type = meta.GetNetwork().GetConnection().GetType()
	return nil
}

func decodeMetricset(ms *v2.Event_Metricset, out *model.APMEvent) bool {
	out.Processor = model.MetricsetProcessor
	out.Metricset = &model.Metricset{}

	if ts := ms.GetTimestamp(); !ts.AsTime().IsZero() && ts.IsValid() {
		out.Timestamp = ts.AsTime()
	}

	if len(ms.Samples) > 0 {
		samples := make(map[string]model.MetricsetSample, len(ms.Samples))
		for name, sample := range ms.Samples {
			// Safeguard against theoretical empty sample?
			if sample == nil {
				continue
			}
			var counts []int64
			var values []float64
			if n := len(sample.Values); n > 0 {
				values = make([]float64, n)
				copy(values, sample.Values)
			}
			if n := len(sample.Counts); n > 0 {
				counts = make([]int64, n)
				copy(counts, sample.Counts)
			}
			samples[name] = model.MetricsetSample{
				Type:  model.MetricType(sample.Type),
				Unit:  sample.Unit,
				Value: sample.Value,
				Histogram: model.Histogram{
					Values: values,
					Counts: counts,
				},
			}
		}
		out.Metricset.Samples = samples
	}

	if tags := ms.GetTags().AsMap(); len(tags) > 0 {
		modeldecoderutil.MergeLabels(tags, out)
	}
	if span := ms.GetSpan(); span != nil {
		out.Span = &model.Span{
			Type:    span.GetType(),
			Subtype: span.GetSubtype(),
		}
	}
	ok := true
	if tx := ms.GetTransaction(); tx != nil {
		out.Transaction = &model.Transaction{
			Name: tx.GetName(),
			Type: tx.GetType(),
		}
		// Transaction fields specified: this is an APM-internal metricset.
		// If there are no known metric samples, we return false so the
		// metricset is not added to the batch.
		ok = modeldecoderutil.SetInternalMetrics(out)
	}

	if name := ms.GetService().GetName(); name != "" {
		out.Service.Name = name
	}
	if version := ms.GetService().GetVersion(); version != "" {
		out.Service.Name = version
	}

	decodeFaas(ms.GetFaas(), out)

	return ok
}

func decodeError(e *v2.Event_Error, event *model.APMEvent) {
	out := &model.Error{}
	event.Error = out
	event.Processor = model.ErrorProcessor

	// overwrite metadata with event specific information
	decodeServiceModel(e.GetContext().GetService(), &event.Service)
	decodeAgentModel(e.GetContext().GetService().GetAgent(), &event.Agent)
	overwriteUserInMetadataModel(e.GetContext().GetUser(), event)
	decodeUserAgentModel(e.GetContext().GetRequest().GetHeaders(), &event.UserAgent)
	decodeClientModel(e.GetContext().GetRequest(), &event.Source, &event.Client)

	// map errorEvent specific data
	if ctx := e.GetContext(); ctx != nil {
		if tags := ctx.GetTags().AsMap(); len(tags) > 0 {
			modeldecoderutil.MergeLabels(tags, event)
		}
		if req := ctx.GetRequest(); req != nil {
			event.HTTP.Request = &model.HTTPRequest{}
			mapToRequestModel(req, event.HTTP.Request)
			if req.GetHttpVersion() != "" {
				event.HTTP.Version = req.HttpVersion
			}
		}
		if res := ctx.GetResponse(); res != nil {
			event.HTTP.Response = &model.HTTPResponse{}
			mapToResponseModel(res, event.HTTP.Response)
		}
		if u := ctx.GetRequest().GetUrl(); u != nil {
			mapToRequestURLModel(u, &event.URL)
		}
		if page := ctx.GetPage(); page != nil {
			if pURL := page.GetUrl(); pURL != "" && ctx.GetRequest().GetUrl() == nil {
				event.URL = model.ParseURL(pURL, "", "")
			}
			if page.GetReferer() != "" {
				if event.HTTP.Request == nil {
					event.HTTP.Request = &model.HTTPRequest{}
				}
				if event.HTTP.Request.Referrer == "" {
					event.HTTP.Request.Referrer = page.Referer
				}
			}
		}
		if custom := ctx.Custom.AsMap(); len(custom) > 0 {
			out.Custom = modeldecoderutil.NormalizeLabelValues(mapstr.M(custom).Clone())
		}
	}
	if culprit := e.GetCulprit(); culprit != "" {
		out.Culprit = culprit
	}
	if exception := e.GetException(); exception != nil {
		out.Exception = &model.Exception{}
		mapToExceptionModel(exception, out.Exception)
	}
	if id := e.GetId(); id != "" {
		out.ID = id
	}
	if fromLog := e.GetLog(); fromLog != nil {
		log := model.ErrorLog{}
		if fromLog.Level != "" {
			log.Level = fromLog.Level
		}
		if fromLog.LoggerName != "" {
			log.LoggerName = fromLog.LoggerName
		}
		if fromLog.Message != "" {
			log.Message = fromLog.Message
		}
		if fromLog.ParamMessage != "" {
			log.ParamMessage = fromLog.ParamMessage
		}
		if len(fromLog.StackTrace) > 0 {
			log.Stacktrace = make(model.Stacktrace, len(fromLog.StackTrace))
			mapToStracktraceModel(fromLog.StackTrace, log.Stacktrace)
		}
		event.Error.Log = &log
	}
	if id := e.GetParentId(); id != "" {
		event.Parent.ID = id
	}
	if t := e.GetTimestamp().AsTime(); !t.IsZero() {
		event.Timestamp = t
	}
	if id := e.GetTraceId(); id != "" {
		event.Trace.ID = id
	}
	if tx := e.GetTransaction(); tx != nil {
		event.Transaction = &model.Transaction{
			Sampled: tx.Sampled,
		}
		if tx.Name != "" {
			event.Transaction.Name = tx.Name
		}
		if tx.Type != "" {
			event.Transaction.Type = tx.Type
		}
		if e.TransactionId != "" {
			event.Transaction.ID = e.TransactionId
		}
	}
}

func decodeTransaction(tx *v2.Event_Transaction, event *model.APMEvent) {
	out := &model.Transaction{}
	event.Processor = model.TransactionProcessor
	event.Transaction = out

	// overwrite metadata with event specific information
	decodeServiceModel(tx.GetContext().GetService(), &event.Service)
	decodeAgentModel(tx.GetContext().GetService().GetAgent(), &event.Agent)
	overwriteUserInMetadataModel(tx.GetContext().GetUser(), event)
	decodeUserAgentModel(tx.GetContext().GetRequest().GetHeaders(), &event.UserAgent)
	decodeClientModel(tx.GetContext().GetRequest(), &event.Source, &event.Client)
	decodeFaas(tx.GetFaas(), event)
	mapToCloudModel(tx.GetContext().GetCloud(), &event.Cloud)
	mapToDroppedSpansModel(tx.GetDroppedSpanStats(), event.Transaction)

	// map transaction specific data
	if ctx := tx.Context; ctx != nil {
		if custom := ctx.GetCustom().AsMap(); len(custom) > 0 {
			out.Custom = modeldecoderutil.NormalizeLabelValues(mapstr.M(custom).Clone())
		}
		if tags := ctx.GetTags().AsMap(); len(tags) > 0 {
			modeldecoderutil.MergeLabels(tags, event)
		}
		if msg := ctx.GetMessage(); msg != nil {
			out.Message = &model.Message{}
			if msg.Age != nil {
				val := int(msg.Age.Milliseconds)
				out.Message.AgeMillis = &val
			}
			if msg.Body != "" {
				out.Message.Body = msg.Body
			}
			if len(msg.Headers) > 0 {
				var headers http.Header
				protoHeadersToHTTP(msg.GetHeaders(), &headers)
				out.Message.Headers = headers
			}
			if msg.GetQueue().GetName() != "" {
				out.Message.QueueName = msg.Queue.Name
			}
			if msg.GetRoutingKey() != "" {
				out.Message.RoutingKey = msg.RoutingKey
			}
		}
		if req := ctx.Request; req != nil {
			event.HTTP.Request = &model.HTTPRequest{}
			mapToRequestModel(req, event.HTTP.Request)
			if req.HttpVersion != "" {
				event.HTTP.Version = req.HttpVersion
			}
		}
		if ctx.GetRequest().GetUrl() != nil {
			mapToRequestURLModel(ctx.GetRequest().GetUrl(), &event.URL)
		}
		if ctx.GetResponse() != nil {
			event.HTTP.Response = &model.HTTPResponse{}
			mapToResponseModel(ctx.Response, event.HTTP.Response)
		}
		if ctx.Page != nil {
			if ctx.Page.Url != "" && ctx.GetRequest().GetUrl() == nil {
				event.URL = model.ParseURL(ctx.Page.Url, "", "")
			}
			if ctx.Page.Referer != "" {
				if event.HTTP.Request == nil {
					event.HTTP.Request = &model.HTTPRequest{}
				}
				if event.HTTP.Request.Referrer == "" {
					event.HTTP.Request.Referrer = ctx.Page.Referer
				}
			}
		}
	}
	if tx.Duration != nil {
		duration := time.Duration(*tx.Duration * float64(time.Millisecond))
		event.Event.Duration = duration
	}
	if tx.Id != "" {
		out.ID = tx.Id
	}
	if len(tx.Marks) > 0 {
		out.Marks = make(model.TransactionMarks, len(tx.Marks))
		for event, val := range tx.Marks {
			if len(val.Measurements) > 0 {
				out.Marks[event] = model.TransactionMark(val.Measurements)
			}
		}
	}
	if tx.Name != "" {
		out.Name = tx.Name
	}
	if tx.GetOutcome().Number() > 0 {
		event.Event.Outcome = tx.Outcome.String()
	} else {
		if res := tx.GetContext().GetResponse(); res.GetStatusCode() != 0 {
			statusCode := res.StatusCode
			if statusCode >= http.StatusInternalServerError {
				event.Event.Outcome = "failure"
			} else {
				event.Event.Outcome = "success"
			}
		} else {
			event.Event.Outcome = "unknown"
		}
	}
	if tx.ParentId != "" {
		event.Parent.ID = tx.ParentId
	}
	if tx.Result != "" {
		out.Result = tx.Result
	}
	sampled := true
	if tx.Sampled != nil {
		sampled = *tx.Sampled
	}
	out.Sampled = sampled
	if tx.SampleRate != nil {
		out.RepresentativeCount = float64(1 / *tx.SampleRate)
	} else {
		out.RepresentativeCount = 1
	}
	if tx.GetSession().GetId() != "" {
		event.Session.ID = tx.Session.Id
		event.Session.Sequence = int(tx.Session.Sequence)
	}
	if sc := tx.GetSpanCount(); sc != nil {
		if sc.Dropped > 0 {
			dropped := int(sc.Dropped)
			out.SpanCount.Dropped = &dropped
		}
		if sc.Started > 0 {
			started := int(sc.Started)
			out.SpanCount.Started = &started
		}
	}
	if ts := tx.Timestamp.AsTime(); !ts.IsZero() {
		event.Timestamp = ts
	}
	if tx.TraceId != "" {
		event.Trace.ID = tx.TraceId
	}
	if tx.Type != "" {
		out.Type = tx.Type
	}
	if ux := tx.GetUserExperience(); ux != nil {
		out.UserExperience = &model.UserExperience{
			CumulativeLayoutShift: -1,
			FirstInputDelay:       -1,
			TotalBlockingTime:     -1,
			Longtask:              model.LongtaskMetrics{Count: -1},
		}
		if ux.Cls != 0 {
			out.UserExperience.CumulativeLayoutShift = ux.Cls
		}
		if ux.Fid != 0 {
			out.UserExperience.FirstInputDelay = ux.Fid

		}
		if ux.Tbt != 0 {
			out.UserExperience.TotalBlockingTime = ux.Tbt
		}
		if ux.Longtask != nil {
			out.UserExperience.Longtask = model.LongtaskMetrics{
				Count: int(ux.Longtask.Count),
				Sum:   ux.Longtask.Sum,
				Max:   ux.Longtask.Max,
			}
		}
	}

	if tx.Otel != nil {
		if event.Span == nil {
			event.Span = &model.Span{}
		}
		mapOTelAttributesTransaction(*tx.Otel, event)
	}

	if len(tx.Links) > 0 {
		if event.Span == nil {
			event.Span = &model.Span{}
		}
		mapSpanLinks(tx.Links, &event.Span.Links)
	}
}

func decodeSpan(from *v2.Event_Span, event *model.APMEvent) {
	out := &model.Span{}
	event.Span = out
	event.Processor = model.SpanProcessor

	// map span specific data
	if from.Action == "" && from.Subtype == "" {
		sep := "."
		typ := strings.Split(from.Type, sep)
		out.Type = typ[0]
		if len(typ) > 1 {
			out.Subtype = typ[1]
			if len(typ) > 2 {
				out.Action = strings.Join(typ[2:], sep)
			}
		}
	} else {
		if from.Action != "" {
			out.Action = from.Action
		}
		if from.Subtype != "" {
			out.Subtype = from.Subtype
		}
		if from.Type != "" {
			out.Type = from.Type
		}
	}
	if from.GetComposite() != nil {
		composite := model.Composite{}
		if from.Composite.Count > 0 {
			composite.Count = int(from.Composite.Count)
		}
		if from.Composite.Sum > 0 {
			composite.Sum = from.Composite.Sum
		}
		switch from.Composite.CompressionStrategy {
		case v2.CompressionStrategy_COMPRESSION_STRATEGY_EXACT_MATCH:
			composite.CompressionStrategy = "exact_match"
		case v2.CompressionStrategy_COMPRESSION_STRATEGY_SAME_KIND:
			composite.CompressionStrategy = "same_kind"
		}
		out.Composite = &composite
	}
	if len(from.ChildIds) > 0 {
		event.Child.ID = make([]string, len(from.ChildIds))
		copy(event.Child.ID, from.ChildIds)
	}
	if cdb := from.GetContext().GetDatabase(); cdb != nil {
		db := model.DB{}
		if cdb.Instance != "" {
			db.Instance = cdb.Instance
		}
		if cdb.Link != "" {
			db.Link = cdb.Link
		}
		if cdb.RowsAffected > 0 {
			val := int(cdb.RowsAffected)
			db.RowsAffected = &val
		}
		if cdb.Statement != "" {
			db.Statement = cdb.Statement
		}
		if cdb.Type != "" {
			db.Type = cdb.Type
		}
		if cdb.User != "" {
			db.UserName = cdb.User
		}
		out.DB = &db
	}
	if dst := from.GetContext().GetDestination(); dst != nil {
		if dst.Address != "" {
			event.Destination.Address = dst.Address
		}
		if dst.Port > 0 {
			event.Destination.Port = int(dst.Port)
		}
	}
	// DEPRECATED: target destination service.
	// if svc := from.GetContext().GetDestination().GetService(); svc != nil {
	// 	service := model.DestinationService{}
	// 	if from.Context.Destination.Service.Name.IsSet() {
	// 		service.Name = from.Context.Destination.Service.Name
	// 	}
	// 	if from.Context.Destination.Service.Resource.IsSet() {
	// 		service.Resource = from.Context.Destination.Service.Resource
	// 	}
	// 	if from.Context.Destination.Service.Type.IsSet() {
	// 		service.Type = from.Context.Destination.Service.Type
	// 	}
	// 	out.DestinationService = &service
	// }
	if ctxHttp := from.GetContext().GetHttp(); ctxHttp != nil {
		if ctxHttp.Method != "" {
			event.HTTP.Request = &model.HTTPRequest{}
			event.HTTP.Request.Method = ctxHttp.Method
		}
		if ctxHttp.Response != nil {
			response := model.HTTPResponse{}
			if ctxHttp.Response.DecodedBodySize > 0 {
				val := ctxHttp.Response.DecodedBodySize
				response.DecodedBodySize = &val
			}
			if ctxHttp.Response.EncodedBodySize > 0 {
				val := ctxHttp.Response.EncodedBodySize
				response.EncodedBodySize = &val
			}
			if len(ctxHttp.Response.Headers) > 0 {
				var headers http.Header
				protoHeadersToHTTP(ctxHttp.Response.Headers, &headers)
				response.Headers = modeldecoderutil.HTTPHeadersToMap(headers.Clone())
			}
			if ctxHttp.Response.StatusCode > 0 {
				response.StatusCode = int(ctxHttp.Response.StatusCode)
			}
			if ctxHttp.Response.TransferSize > 0 {
				val := ctxHttp.Response.TransferSize
				response.TransferSize = &val
			}
			event.HTTP.Response = &response
		}
		// Deprecated:
		// if ctxHttp.StatusCode > 0 {
		// 	if event.HTTP.Response == nil {
		// 		event.HTTP.Response = &model.HTTPResponse{}
		// 	}
		// 	event.HTTP.Response.StatusCode = from.Context.HTTP.StatusCode
		// }
		if ctxHttp.Url != "" {
			event.URL.Original = ctxHttp.Url
		}
	}
	if ctxMsg := from.GetContext().GetMessage(); ctxMsg != nil {
		message := model.Message{}
		if ctxMsg.Body != "" {
			message.Body = ctxMsg.Body
		}
		if len(ctxMsg.Headers) > 0 {
			var headers http.Header
			protoHeadersToHTTP(ctxMsg.Headers, &headers)
			message.Headers = headers.Clone()
		}
		if ctxMsg.GetAge().GetMilliseconds() > 0 {
			val := int(ctxMsg.Age.Milliseconds)
			message.AgeMillis = &val
		}
		if ctxMsg.GetQueue().GetName() != "" {
			message.QueueName = ctxMsg.Queue.Name
		}
		if ctxMsg.GetRoutingKey() != "" {
			message.RoutingKey = ctxMsg.RoutingKey
		}
		out.Message = &message
	}
	if svc := from.GetContext().GetService(); svc != nil {
		decodeServiceModel(svc, &event.Service)
		decodeAgentModel(svc.Agent, &event.Agent)
	}
	// DEPRECATED:
	// if from.GetContext().GetService().GetTarget().GetType() == "" && from.GetContext().GetDestination().Service.Resource.IsSet() {
	// 	outTarget := targetFromDestinationResource(from.Context.Destination.Service.Resource)
	// 	event.Service.Target = &outTarget
	// }
	if tags := from.GetContext().GetTags().AsMap(); len(tags) > 0 {
		modeldecoderutil.MergeLabels(tags, event)
	}
	if from.Duration != nil {
		duration := time.Duration(*from.Duration * float64(time.Millisecond))
		event.Event.Duration = duration
	}
	if from.Id != "" {
		out.ID = from.Id
	}
	if from.Name != "" {
		out.Name = from.Name
	}
	if from.Outcome.Number() > 0 {
		event.Event.Outcome = from.Outcome.String()
	} else {
		if sc := from.GetContext().GetHttp().GetResponse().GetStatusCode(); sc > 0 {
			if sc >= http.StatusBadRequest {
				event.Event.Outcome = "failure"
			} else {
				event.Event.Outcome = "success"
			}
		} else {
			event.Event.Outcome = "unknown"
		}
	}
	if from.ParentId != nil {
		event.Parent.ID = *from.ParentId
	}
	if from.SampleRate != nil {
		out.RepresentativeCount = float64(1 / *from.SampleRate)
	}
	if len(from.StackTrace) > 0 {
		out.Stacktrace = make(model.Stacktrace, len(from.StackTrace))
		mapToStracktraceModel(from.StackTrace, out.Stacktrace)
	}
	if from.Sync != nil {
		val := *from.Sync
		out.Sync = &val
	}
	if ts := from.GetTimestamp().AsTime(); !ts.IsZero() {
		event.Timestamp = ts
	} else if from.Start > 0 {
		// event.Timestamp is initialized to the time the payload was
		// received by apm-server; offset that by "start" milliseconds
		// for RUM.
		event.Timestamp = event.Timestamp.Add(
			time.Duration(float64(time.Millisecond) * from.Start),
		)
	}
	if from.TraceId != "" {
		event.Trace.ID = from.TraceId
	}
	if from.TransactionId != "" {
		event.Transaction = &model.Transaction{ID: from.TransactionId}
	}
	if from.Otel != nil {
		mapOTelAttributesSpan(from.Otel, event)
	}
	if len(from.Links) > 0 {
		mapSpanLinks(from.Links, &out.Links)
	}
}

// Helpers

func decodeServiceModel(from *v2.CtxService, out *model.Service) {
	if from == nil {
		return
	}
	if v := from.GetEnvironment(); v != "" {
		out.Environment = v
	}
	if v := from.GetFramework().GetName(); v != "" {
		out.Framework.Name = v
	}
	if v := from.GetFramework().GetVersion(); v != "" {
		out.Framework.Version = v
	}
	if v := from.GetLanguage().GetName(); v != "" {
		out.Language.Name = v
	}
	if v := from.GetLanguage().GetVersion(); v != "" {
		out.Language.Version = v
	}
	if from.GetName() != "" {
		out.Name = from.Name
		out.Version = from.Version
	}
	if v := from.GetNode().GetName(); v != "" {
		out.Node.Name = v
	}
	if v := from.GetRuntime().GetName(); v != "" {
		out.Runtime.Name = v
	}
	if v := from.GetRuntime().GetVersion(); v != "" {
		out.Runtime.Version = v
	}
	if origin := from.GetOrigin(); origin != nil {
		var outOrigin model.ServiceOrigin
		if v := origin.GetName(); v != "" {
			outOrigin.Name = v
		}
		if v := origin.GetVersion(); v != "" {
			outOrigin.Version = v
		}
		if v := origin.GetId(); v != "" {
			outOrigin.ID = v
		}
		out.Origin = &outOrigin
	}
	if trg := from.GetTarget(); trg != nil {
		var outTarget model.ServiceTarget
		if v := trg.GetName(); v != "" {
			outTarget.Name = v
		}
		if v := trg.GetType(); v != "" {
			outTarget.Type = v
		}
		out.Target = &outTarget
	}
}

func decodeAgentModel(from *v2.Agent, out *model.Agent) {
	if from == nil {
		return
	}
	if from.Name != "" {
		out.Name = from.Name
	}
	if from.Version != "" {
		out.Version = from.Version
	}
	if from.EphemeralId != "" {
		out.EphemeralID = from.EphemeralId
	}
}

func overwriteUserInMetadataModel(from *v2.User, out *model.APMEvent) {
	if from == nil {
		return
	}
	// overwrite User specific values if set
	// either populate all User fields or none to avoid mixing
	// different user data
	if from.Domain == "" && from.Id != nil && from.Email == "" && from.Name == "" {
		return
	}
	out.User = model.User{}
	if from.Domain != "" {
		out.User.Domain = from.Domain
	}
	if v := from.GetIdInt(); v > 0 {
		out.User.ID = strconv.Itoa(int(v))
	}
	if v := from.GetIdStr(); v != "" {
		out.User.ID = v
	}
	if from.Email != "" {
		out.User.Email = from.Email
	}
	if from.Name != "" {
		out.User.Name = from.Name
	}
}

func mapToRequestModel(from *v2.Context_CtxRequest, out *model.HTTPRequest) {
	if from == nil {
		return
	}
	if from.GetMethod() != "" {
		out.Method = from.Method
	}
	if env := from.GetEnv().AsMap(); len(env) > 0 {
		out.Env = mapstr.M(env).Clone()
	}
	// TODO(marclop) Unclear how this is handled in pb.
	if from.GetBody() != nil {
		out.Body = modeldecoderutil.NormalizeHTTPRequestBody(from.Body)
	}
	if cookies := from.GetCookies().AsMap(); len(cookies) > 0 {
		out.Cookies = mapstr.M(cookies).Clone()
	}
	if len(from.Headers) > 0 {
		var headers http.Header
		protoHeadersToHTTP(from.GetHeaders(), &headers)
		out.Headers = modeldecoderutil.HTTPHeadersToMap(headers)
	}
}

func mapToResponseModel(from *v2.Context_CtxResponse, out *model.HTTPResponse) {
	if from == nil {
		return
	}
	if from.Finished != nil {
		val := *from.Finished
		out.Finished = &val
	}
	if len(from.Headers) > 0 {
		var headers http.Header
		protoHeadersToHTTP(from.GetHeaders(), &headers)
		out.Headers = modeldecoderutil.HTTPHeadersToMap(headers.Clone())
	}
	if from.HeadersSent != nil {
		val := *from.HeadersSent
		out.HeadersSent = &val
	}
	if from.StatusCode > 0 {
		out.StatusCode = int(from.StatusCode)
	}
	if from.TransferSize != 0 {
		val := from.TransferSize
		out.TransferSize = &val
	}
	if from.EncodedBodySize != 0 {
		val := from.EncodedBodySize
		out.EncodedBodySize = &val
	}
	if from.DecodedBodySize != 0 {
		val := from.DecodedBodySize
		out.DecodedBodySize = &val
	}
}

func mapToRequestURLModel(from *v2.Context_CtxRequest_CtxRequestURL, out *model.URL) {
	if from == nil {
		return
	}
	if from.Raw != "" {
		out.Original = from.Raw
	}
	if from.Full != "" {
		out.Full = from.Full
	}
	if from.Hostname != "" {
		out.Domain = from.Hostname
	}
	if from.Pathname != "" {
		out.Path = from.Pathname
	}
	if from.Search != "" {
		out.Query = from.Search
	}
	if from.Hash != "" {
		out.Fragment = from.Hash
	}
	if from.Protocol != "" {
		out.Scheme = strings.TrimSuffix(from.Protocol, ":")
	}
	if from.Port != nil {
		// TODO(marclop) more validation is needed.
		// should never result in an error, type is checked when decoding
		port, err := strconv.Atoi(fmt.Sprint(from.Port))
		if err == nil {
			out.Port = port
		}
	}
}

func mapToExceptionModel(from *v2.Exception, out *model.Exception) {
	if from == nil {
		return
	}
	if attrs := from.GetAttributes().AsMap(); len(attrs) > 0 {
		out.Attributes = mapstr.M(attrs).Clone()
	}
	if val := from.GetCode(); val != nil {
		var val interface{}
		if v := from.GetCodeFloat(); v != 0 {
			val = v
		}
		if v := from.GetCodeInt(); v != 0 {
			val = v
		}
		if v := from.GetCodeStr(); v != "" {
			val = v
		}
		out.Code = modeldecoderutil.ExceptionCodeString(val)
	}
	if len(from.Cause) > 0 {
		out.Cause = make([]model.Exception, len(from.Cause))
		for i := 0; i < len(from.Cause); i++ {
			var ex model.Exception
			mapToExceptionModel(from.Cause[i], &ex)
			out.Cause[i] = ex
		}
	}
	if from.Handled != nil {
		val := *from.Handled
		out.Handled = &val
	}
	if from.Message != "" {
		out.Message = from.Message
	}
	if from.Module != "" {
		out.Module = from.Module
	}
	if len(from.StackTrace) > 0 {
		out.Stacktrace = make(model.Stacktrace, len(from.StackTrace))
		mapToStracktraceModel(from.StackTrace, out.Stacktrace)
	}
	if from.Type != "" {
		out.Type = from.Type
	}
}

func mapToStracktraceModel(from []*v2.StackTraceFrame, out model.Stacktrace) {
	for idx, eventFrame := range from {
		// NOTE(marclop) Theoretical nil, not sure it can be.
		if eventFrame == nil {
			continue
		}
		fr := model.StacktraceFrame{}
		if eventFrame.AbsPath != "" {
			fr.AbsPath = eventFrame.AbsPath
		}
		if eventFrame.ClassName != "" {
			fr.Classname = eventFrame.ClassName
		}
		if eventFrame.ColumnNumber > 0 {
			val := int(eventFrame.ColumnNumber)
			fr.Colno = &val
		}
		if eventFrame.ContextLine != "" {
			fr.ContextLine = eventFrame.ContextLine
		}
		if eventFrame.FileName != "" {
			fr.Filename = eventFrame.FileName
		}
		if eventFrame.Function != "" {
			fr.Function = eventFrame.Function
		}
		fr.LibraryFrame = eventFrame.LibraryFrame
		if eventFrame.LineNumber > 0 {
			val := int(eventFrame.LineNumber)
			fr.Lineno = &val
		}
		if eventFrame.Module != "" {
			fr.Module = eventFrame.Module
		}
		if len(eventFrame.PostContext) > 0 {
			fr.PostContext = make([]string, len(eventFrame.PostContext))
			copy(fr.PostContext, eventFrame.PostContext)
		}
		if len(eventFrame.PreContext) > 0 {
			fr.PreContext = make([]string, len(eventFrame.PreContext))
			copy(fr.PreContext, eventFrame.PreContext)
		}
		if vars := eventFrame.Vars.AsMap(); len(vars) > 0 {
			fr.Vars = mapstr.M(vars).Clone()
		}
		out[idx] = &fr
	}
}

func decodeUserAgentModel(from map[string]*v2.HTTPHeaderValue, out *model.UserAgent) {
	if len(from) == 0 {
		return
	}
	var headers http.Header
	protoHeadersToHTTP(from, &headers)
	if h := headers.Values(textproto.CanonicalMIMEHeaderKey("User-Agent")); len(h) > 0 {
		out.Original = strings.Join(h, ", ")
	}
}

func decodeClientModel(from *v2.Context_CtxRequest, source *model.Source, client *model.Client) {
	if from == nil {
		return
	}
	// http.Request.Headers and http.Request.Socket are only set for backend events.
	if source.IP == nil {
		ip, port := netutil.ParseIPPort(
			netutil.MaybeSplitHostPort(from.GetSocket().GetRemoteAddress()),
		)
		source.IP, source.Port = ip, int(port)
	}
	if client.IP == nil {
		client.IP = source.IP
		var headers http.Header
		protoHeadersToHTTP(from.GetHeaders(), &headers)
		if ip, port := netutil.ClientAddrFromHeaders(headers); ip != nil {
			source.NAT = &model.NAT{IP: source.IP}
			client.IP, client.Port = ip, int(port)
			source.IP, source.Port = client.IP, client.Port
		}
	}
}

func protoHeadersToHTTP(in map[string]*v2.HTTPHeaderValue, out *http.Header) {
	if len(in) == 0 {
		return
	}
	headers := make(http.Header, len(in))
	for k, v := range in {
		headers[k] = v.GetValues()
	}
	*out = headers
}

func decodeFaas(faas *v2.Faas, out *model.APMEvent) {
	if faas == nil {
		return
	}

	if v := faas.GetId(); v != "" {
		out.FAAS.ID = v
	}
	if v := faas.GetColdstart(); faas.Coldstart != nil {
		out.FAAS.Coldstart = &v
	}
	if v := faas.GetExecution(); v != "" {
		out.FAAS.Execution = v
	}
	if v := faas.GetTrigger(); v != nil {
		if v := v.GetRequestId(); v != "" {
			out.FAAS.TriggerRequestID = v
		}
		if v := v.GetType(); v != "" {
			out.FAAS.TriggerType = v
		}
	}
	if v := faas.GetName(); v != "" {
		out.FAAS.Name = v
	}
	if v := faas.GetVersion(); v != "" {
		out.FAAS.Version = v
	}
}

func mapSpanLinks(from []*v2.SpanLink, out *[]model.SpanLink) {
	*out = make([]model.SpanLink, len(from))
	for i, link := range from {
		(*out)[i] = model.SpanLink{
			Span:  model.Span{ID: link.SpanId},
			Trace: model.Trace{ID: link.TraceId},
		}
	}
}

func mapOTelAttributesTransaction(from v2.OTel, out *model.APMEvent) {
	library := pdata.NewInstrumentationLibrary()
	m := otelAttributeMap(&from)
	if from.SpanKind != "" {
		out.Span.Kind = from.SpanKind
	}
	if out.Labels == nil {
		out.Labels = make(model.Labels)
	}
	if out.NumericLabels == nil {
		out.NumericLabels = make(model.NumericLabels)
	}
	// TODO: Does this work? Is there a way we can infer the status code,
	// potentially in the actual attributes map?
	spanStatus := pdata.NewSpanStatus()
	spanStatus.SetCode(pdata.StatusCodeUnset)
	otel_processor.TranslateTransaction(m, spanStatus, library, out)

	if out.Span.Kind == "" {
		switch out.Transaction.Type {
		case "messaging":
			out.Span.Kind = "CONSUMER"
		case "request":
			out.Span.Kind = "SERVER"
		default:
			out.Span.Kind = "INTERNAL"
		}
	}
}

const spanKindStringPrefix = "SPAN_KIND_"

func mapOTelAttributesSpan(from *v2.OTel, out *model.APMEvent) {
	m := otelAttributeMap(from)
	if out.Labels == nil {
		out.Labels = make(model.Labels)
	}
	if out.NumericLabels == nil {
		out.NumericLabels = make(model.NumericLabels)
	}
	var spanKind pdata.SpanKind
	if from.SpanKind != "" {
		switch from.SpanKind {
		case pdata.SpanKindInternal.String()[len(spanKindStringPrefix):]:
			spanKind = pdata.SpanKindInternal
		case pdata.SpanKindServer.String()[len(spanKindStringPrefix):]:
			spanKind = pdata.SpanKindServer
		case pdata.SpanKindClient.String()[len(spanKindStringPrefix):]:
			spanKind = pdata.SpanKindClient
		case pdata.SpanKindProducer.String()[len(spanKindStringPrefix):]:
			spanKind = pdata.SpanKindProducer
		case pdata.SpanKindConsumer.String()[len(spanKindStringPrefix):]:
			spanKind = pdata.SpanKindConsumer
		default:
			spanKind = pdata.SpanKindUnspecified
		}
		out.Span.Kind = from.SpanKind
	}
	otel_processor.TranslateSpan(spanKind, m, out)

	if spanKind == pdata.SpanKindUnspecified {
		switch out.Span.Type {
		case "db", "external", "storage":
			out.Span.Kind = "CLIENT"
		default:
			out.Span.Kind = "INTERNAL"
		}
	}
}

func otelAttributeMap(o *v2.OTel) pdata.AttributeMap {
	m := pdata.NewAttributeMap()
	for k, v := range o.Attributes.AsMap() {
		if attr, ok := otelAttributeValue(k, v); ok {
			m.Insert(k, attr)
		}
	}
	return m
}

func otelAttributeValue(k string, v interface{}) (pdata.AttributeValue, bool) {
	// According to the spec, these are the allowed primitive types
	// Additionally, homogeneous arrays (single type) of primitive types are allowed
	// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/common/common.md#attributes
	switch v := v.(type) {
	case string:
		return pdata.NewAttributeValueString(v), true
	case bool:
		return pdata.NewAttributeValueBool(v), true
	case json.Number:
		// Semantic conventions have specified types, and we rely on this
		// in processor/otel when mapping to our data model. For example,
		// `http.status_code` is expected to be an int.
		if !isOTelDoubleAttribute(k) {
			if v, err := v.Int64(); err == nil {
				return pdata.NewAttributeValueInt(v), true
			}
		}
		if v, err := v.Float64(); err == nil {
			return pdata.NewAttributeValueDouble(v), true
		}
	case []interface{}:
		array := pdata.NewAttributeValueArray()
		array.SliceVal().EnsureCapacity(len(v))
		for i := range v {
			if elem, ok := otelAttributeValue(k, v[i]); ok {
				elem.CopyTo(array.SliceVal().AppendEmpty())
			}
		}
		return array, true
	}
	return pdata.AttributeValue{}, false
}

// isOTelDoubleAttribute indicates whether k is an OpenTelemetry semantic convention attribute
// known to have type "double". As this list grows over time, we should consider generating
// the mapping with OpenTelemetry's semconvgen build tool.
//
// For the canonical semantic convention definitions, see
// https://github.com/open-telemetry/opentelemetry-specification/tree/main/semantic_conventions/trace
func isOTelDoubleAttribute(k string) bool {
	switch k {
	case "aws.dynamodb.provisioned_read_capacity":
		return true
	case "aws.dynamodb.provisioned_write_capacity":
		return true
	}
	return false
}

func mapToCloudModel(from *v2.Context_Cloud, cloud *model.Cloud) {
	if from == nil {
		return
	}
	cloudOrigin := model.CloudOrigin{}
	if v := from.GetOrigin().GetAccount().GetId(); v != "" {
		cloudOrigin.AccountID = v
	}
	if v := from.GetOrigin().GetProvider(); v != "" {
		cloudOrigin.Provider = v
	}
	if v := from.GetOrigin().GetRegion(); v != "" {
		cloudOrigin.Region = v
	}
	if v := from.GetOrigin().GetService().GetName(); v != "" {
		cloudOrigin.ServiceName = v
	}
	cloud.Origin = &cloudOrigin
}

func mapToDroppedSpansModel(from []*v2.DroppedSpanStats, tx *model.Transaction) {
	for _, f := range from {
		if f == nil {
			continue
		}
		var to model.DroppedSpanStats
		if f.DestinationServiceResource != "" {
			to.DestinationServiceResource = f.DestinationServiceResource
		}
		if f.Outcome.Number() > 0 {
			to.Outcome = f.Outcome.String()
		}
		if duration := f.GetDuration(); duration != nil {
			to.Duration.Count = int(duration.Count)
			if sum := duration.Sum; sum != nil {
				to.Duration.Sum = time.Duration(sum.Us) * time.Microsecond
			}
		}
		if f.ServiceTargetType != "" {
			to.ServiceTargetType = f.ServiceTargetType
		}
		if f.ServiceTargetName != "" {
			to.ServiceTargetName = f.ServiceTargetName
		}
		tx.DroppedSpansStats = append(tx.DroppedSpansStats, to)
	}
}
