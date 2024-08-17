package rocketmq

import (
	"context"
	"strings"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.8.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

func producerDefaultInterceptor(broker *kBroker) primitive.Interceptor {
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})
	// rcopts := make([]consumer.Option, 0)
	// rcopts = append(rcopts, consumer.WithNameServer(k.addrs))

	tracingEnabled, _ := broker.opts.Context.Value(tracingEnabledConfigKey{}).(bool)
	groupName, _ := broker.opts.Context.Value(groupNameConfigKey{}).(string)

	tracer := otel.Tracer("rocketmq")

	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("rocketmq"),
		semconv.MessagingRocketmqClientGroupKey.String(groupName),
		// semconv.MessagingRocketmqClientIDKey.String(InstanceName),
	}

	return func(ctx context.Context, req, reply interface{}, next primitive.Invoker) error {
		realReq := req.(*primitive.Message)
		realReply := reply.(*primitive.SendResult)

		var (
			span trace.Span
		)

		if tracingEnabled {
			ctx, span = tracer.Start(ctx, realReq.Topic, trace.WithAttributes(attrs...), trace.WithSpanKind(trace.SpanKindProducer))
			md := metadata.New(nil)
			carrier := propagation.HeaderCarrier(md)
			propagator.Inject(ctx, carrier)

			defer span.End()

			for k, v := range md {
				realReq.WithProperty(strings.ToLower(k), strings.Join(v, ","))
			}
		}

		err := next(ctx, realReq, realReply)
		if realReply == nil || realReply.MessageQueue == nil {
			return err
		}

		if tracingEnabled {
			if err != nil {
				span := trace.SpanFromContext(ctx)
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
				}
				span.End()
			}
		}

		return err
	}
}
