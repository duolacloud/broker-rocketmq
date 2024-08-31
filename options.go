package rocketmq

import (
	"context"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/duolacloud/broker-core"
)

type producerInterceptorsConfigKey struct{}

func WithProducerInterceptors(interceptors []primitive.Interceptor) broker.Option {
	return setBrokerOption(producerInterceptorsConfigKey{}, interceptors)
}

type consumerInterceptorsConfigKey struct{}

func WithConsumerInterceptors(interceptors []primitive.Interceptor) broker.Option {
	return setBrokerOption(consumerInterceptorsConfigKey{}, interceptors)
}

type consumeGoroutineNumsConfigKey struct{}

func WithConsumeGoroutineNums(consumeGoroutineNums int) broker.Option {
	return setBrokerOption(consumeGoroutineNumsConfigKey{}, consumeGoroutineNums)
}

type groupNameConfigKey struct{}

func WithGroupName(groupName string) broker.Option {
	return setBrokerOption(groupNameConfigKey{}, groupName)
}

type retryConfigKey struct{}

func WithRetry(retry int) broker.Option {
	return setBrokerOption(retryConfigKey{}, retry)
}

type tracingEnabledConfigKey struct{}

func WithTracingEnabled(v bool) broker.Option {
	return setBrokerOption(tracingEnabledConfigKey{}, v)
}

type queueNumsConfigKey struct{}

func WithQueueNums(queueNums int) broker.Option {
	return setBrokerOption(queueNumsConfigKey{}, queueNums)
}

type subscribeContextKey struct{}

// SubscribeContext set the context for broker.SubscribeOption
func SubscribeContext(ctx context.Context) broker.SubscribeOption {
	return setSubscribeOption(subscribeContextKey{}, ctx)
}

type delayLevelConfigKey struct{}

func WithDelayLevel(level int) broker.PublishOption {
	return setPublishOption(delayLevelConfigKey{}, level)
}

func defaultOptions() broker.Options {
	opts := broker.Options{
		Context: context.Background(),
	}

	opts.Context = context.WithValue(opts.Context, retryConfigKey{}, 1)
	opts.Context = context.WithValue(opts.Context, groupNameConfigKey{}, "default")
	opts.Context = context.WithValue(opts.Context, tracingEnabledConfigKey{}, true)

	return opts
}
