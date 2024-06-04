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

func (k *kBroker) Publish(topic string, msg *broker.Message, o ...broker.PublishOption) error {
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
		_, err = k.producer.SendSync(context.Background(), m)
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

	err := k.consumer.Subscribe(topic, consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			var m broker.Message

			p := &publication{m: &m, t: msg.Topic, km: msg, consumer: k.consumer}

			eh := k.opts.ErrorHandler

			if k.opts.Codec != nil {
				if err := k.opts.Codec.Unmarshal(msg.Body, &m); err != nil {
					p.err = err
					p.m.Body = msg.Body
					if eh != nil {
						eh(p)
					} else {
						log.Printf("[rocketmq] failed to unmarshal: %v\n", err)
					}
				}
			}

			if p.m.Body == nil {
				p.m.Body = msg.Body
			}

			// if we don't have headers, create empty map
			if m.Header == nil {
				m.Header = make(map[string]string)
			}

			if msg.Queue != nil {
				m.Partition = int32(msg.Queue.QueueId)
			}

			err := handler(p)
			if err == nil && subopts.AutoAck {
				// continue
			} else if err != nil {
				p.err = err
				if eh != nil {
					eh(p)
				}

				// at lease once, some message may reconsume
				return consumer.ConsumeRetryLater, nil
			}
		}

		return consumer.ConsumeSuccess, nil
	})
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
