// Package kafka provides a kafka broker using sarama cluster
package rocketmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/apache/rocketmq-client-go/v2"
	_rocketmq "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/duolacloud/broker-core"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
)

type kBroker struct {
	addrs []string

	producer _rocketmq.Producer
	consumer _rocketmq.PushConsumer

	connected bool
	scMutex   sync.Mutex
	opts      broker.Options
}

type subscriber struct {
	k    *kBroker
	t    string
	opts broker.SubscribeOptions
}

type publication struct {
	t        string
	err      error
	km       *primitive.MessageExt
	m        *broker.Message
	consumer rocketmq.PushConsumer
}

func (p *publication) Topic() string {
	return p.t
}

func (p *publication) Message() *broker.Message {
	return p.m
}

func (p *publication) Ack() error {
	// rocketmq can't ack one message
	return nil
}

func (p *publication) Error() error {
	return p.err
}

func (s *subscriber) Options() broker.SubscribeOptions {
	return s.opts
}

func (s *subscriber) Topic() string {
	return s.t
}

func (s *subscriber) Unsubscribe() error {
	return s.k.consumer.Unsubscribe(s.t)
}

func (k *kBroker) Address() string {
	if len(k.addrs) > 0 {
		return k.addrs[0]
	}
	return "127.0.0.1:9876"
}

func (k *kBroker) Connect() error {
	if k.connected {
		return nil
	}

	rpopts := make([]producer.Option, 0)
	rpopts = append(rpopts, producer.WithNameServer(k.addrs))

	if queueNums, ok := k.opts.Context.Value(queueNumsConfigKey{}).(int); ok {
		rpopts = append(rpopts, producer.WithDefaultTopicQueueNums(queueNums))
	}

	if retry, ok := k.opts.Context.Value(retryConfigKey{}).(int); ok {
		rpopts = append(rpopts, producer.WithRetry(retry))
	}

	interceptors, _ := k.opts.Context.Value(producerInterceptorsConfigKey{}).([]primitive.Interceptor)
	interceptors = append(interceptors, producerDefaultInterceptor(k))
	rpopts = append(rpopts, producer.WithInterceptor(interceptors...))

	producer, err := _rocketmq.NewProducer(
		rpopts...,
	)
	if err != nil {
		return err
	}

	rcopts := make([]consumer.Option, 0)
	rcopts = append(rcopts, consumer.WithNameServer(k.addrs))

	groupName, _ := k.opts.Context.Value(groupNameConfigKey{}).(string)
	rcopts = append(rcopts, consumer.WithGroupName(groupName))

	if consumeGoroutineNums, ok := k.opts.Context.Value(consumeGoroutineNumsConfigKey{}).(int); ok {
		rcopts = append(rcopts, consumer.WithConsumeGoroutineNums(consumeGoroutineNums))
	}

	if interceptors, ok := k.opts.Context.Value(consumerInterceptorsConfigKey{}).([]primitive.Interceptor); ok {
		rcopts = append(rcopts, consumer.WithInterceptor(interceptors...))
	}

	consumer, err := _rocketmq.NewPushConsumer(
		rcopts...,
	)
	if err != nil {
		return err
	}

	k.scMutex.Lock()
	k.producer = producer
	k.consumer = consumer

	if err := k.producer.Start(); err != nil {
		return err
	}

	if err := k.consumer.Start(); err != nil {
		return err
	}

	k.connected = true

	k.scMutex.Unlock()

	return nil
}

func (k *kBroker) Disconnect() error {
	k.scMutex.Lock()
	defer k.scMutex.Unlock()

	if k.producer != nil {
		k.producer.Shutdown()
	}
	if k.consumer != nil {
		k.consumer.Shutdown()
	}

	k.connected = false
	return nil
}

func (k *kBroker) Init(opts ...broker.Option) error {
	for _, o := range opts {
		o(&k.opts)
	}
	var cAddrs []string
	for _, addr := range k.opts.Addrs {
		if len(addr) == 0 {
			continue
		}
		cAddrs = append(cAddrs, addr)
	}
	if len(cAddrs) == 0 {
		cAddrs = []string{"127.0.0.1:9876"}
	}
	k.addrs = cAddrs
	return nil
}

func (k *kBroker) Options() broker.Options {
	return k.opts
}

func (k *kBroker) Publish(ctx context.Context, topic string, msg *broker.Message, o ...broker.PublishOption) error {
	pubopts := broker.PublishOptions{
		Context: context.Background(),
	}
	for _, opt := range o {
		opt(&pubopts)
	}

	var err error
	var body []byte
	if k.opts.Codec != nil {
		body, err = k.opts.Codec.Marshal(msg)
		if err != nil {
			fmt.Printf("[kafka] failed to marshal: %v", err)
			return err
		}
	} else {
		body = msg.Body
	}

	m := primitive.NewMessage(topic, body)
	m.WithProperties(msg.Header)

	delayLevelValue := pubopts.Context.Value(delayLevelConfigKey{})
	if delayLevelValue != nil {
		delayLevel := delayLevelValue.(int)
		m.WithDelayTimeLevel(delayLevel)
	}

	if shardingKeyConfig := k.opts.Context.Value(shardingKeyConfigKey{}); shardingKeyConfig != nil {
		shardingKey, ok := shardingKeyConfig.(string)
		if ok {
			m.WithShardingKey(shardingKey)
		}
	}

	if k.producer != nil {
		_, err = k.producer.SendSync(ctx, m)
		return err
	}

	return errors.New(`no connection resources available`)
}

func (k *kBroker) Subscribe(topic string, handler broker.Handler, o ...broker.SubscribeOption) (broker.Subscriber, error) {
	subopts := broker.SubscribeOptions{
		AutoAck: true,
		Queue:   strconv.Itoa(0),
		Context: context.Background(),
	}
	for _, opt := range o {
		opt(&subopts)
	}

	groupName, _ := k.opts.Context.Value(groupNameConfigKey{}).(string)

	tracer := otel.Tracer("rocketmq")
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("rocketmq"),
		semconv.MessagingRocketmqClientGroupKey.String(groupName),
		// semconv.MessagingRocketmqClientIDKey.String(cc.InstanceName),
		// semconv.MessagingRocketmqConsumptionModelKey.String(cc.MessageModel),
	}

	fn := func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		tracingEnabled, _ := k.opts.Context.Value(tracingEnabledConfigKey{}).(bool)

		for _, msg := range msgs {
			var (
				span trace.Span
			)

			if tracingEnabled {
				carrier := propagation.MapCarrier{}

				for key, value := range msg.GetProperties() {
					carrier[key] = value
				}

				propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})
				ctx = propagator.Extract(ctx, carrier)
				opts := []trace.SpanStartOption{}
				opts = append(opts, trace.WithAttributes(attrs...))
				opts = append(opts, trace.WithSpanKind(trace.SpanKindConsumer))

				ctx, span = tracer.Start(ctx, msg.Topic, opts...)

				span.SetAttributes(
					semconv.MessagingRocketmqNamespaceKey.String(msg.Topic),
					semconv.MessagingRocketmqMessageTagKey.String(msg.GetTags()),
				)

				defer span.End()
			}

			var m broker.Message

			p := &publication{m: &m, t: msg.Topic, km: msg, consumer: k.consumer}

			eh := k.opts.ErrorHandler

			if k.opts.Codec != nil {
				if err := k.opts.Codec.Unmarshal(msg.Body, &m); err != nil {
					p.err = err
					p.m.Body = msg.Body

					if eh != nil {
						eh(ctx, p)
					} else {
						log.Printf("[rocketmq] failed to unmarshal: %v\n", err)
					}
				}
			}

			if p.m.Body == nil {
				p.m.Body = msg.Body
			}

			if p.m.Header == nil {
				p.m.Header = msg.GetProperties()
			}

			if msg.Queue != nil {
				m.Partition = int32(msg.Queue.QueueId)
			}

			err := handler(ctx, p)
			if err == nil && subopts.AutoAck {
				// continue
			} else if err != nil {
				if tracingEnabled && span != nil {
					span.RecordError(err)
				}

				p.err = err
				if eh != nil {
					eh(ctx, p)
				}

				// at lease once, some message may reconsume
				return consumer.ConsumeRetryLater, nil
			}
		}

		return consumer.ConsumeSuccess, nil
	}

	err := k.consumer.Subscribe(topic, consumer.MessageSelector{}, fn)
	if err != nil {
		return nil, err
	}

	return &subscriber{k: k, opts: subopts, t: topic}, nil
}

func (k *kBroker) String() string {
	return "rocketmq"
}

func NewBroker(opts ...broker.Option) broker.Broker {
	options := defaultOptions()

	for _, o := range opts {
		o(&options)
	}

	var cAddrs []string
	for _, addr := range options.Addrs {
		if len(addr) == 0 {
			continue
		}
		cAddrs = append(cAddrs, addr)
	}
	if len(cAddrs) == 0 {
		cAddrs = []string{"127.0.0.1:9876"}
	}

	return &kBroker{
		addrs: cAddrs,
		opts:  options,
	}
}
