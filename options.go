package rocketmq

import (
	"context"

	"github.com/duolacloud/broker-core"
)

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

type queueNumsConfigKey struct{}

func WithQueueNums(queueNums int) broker.Option {
	return setBrokerOption(queueNumsConfigKey{}, queueNums)
}

type subscribeContextKey struct{}

// SubscribeContext set the context for broker.SubscribeOption
func SubscribeContext(ctx context.Context) broker.SubscribeOption {
	return setSubscribeOption(subscribeContextKey{}, ctx)
}

type shardingKeyConfigKey struct{}

func WithShardingKey(shardingKey string) broker.PublishOption {
	return setPublishOption(shardingKeyConfigKey{}, shardingKey)
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

	return opts
}
