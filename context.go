package rocketmq

import (
	"context"

	"github.com/duolacloud/broker-core"
)

// setSubscribeOption returns a function to setup a context with given value
func setSubscribeOption(k, v any) broker.SubscribeOption {
	return func(o *broker.SubscribeOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, k, v)
	}
}

// setBrokerOption returns a function to setup a context with given value
func setBrokerOption(k, v any) broker.Option {
	return func(o *broker.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, k, v)
	}
}

func setPublishOption(k, v interface{}) broker.PublishOption {
	return func(o *broker.PublishOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, k, v)
	}
}
