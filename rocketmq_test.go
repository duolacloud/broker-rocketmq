package rocketmq_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/duolacloud/broker-core"
	rocketmq "github.com/duolacloud/broker-rocketmq"
)

func TestRocketmq(t *testing.T) {
	ctx := context.Background()

	b := rocketmq.NewBroker(
		broker.Addrs("127.0.0.1:9876"),
		rocketmq.WithRetry(3),
	)
	if err := b.Connect(); err != nil {
		t.Fatal(err)
		return
	}

	topic := "test"

	_, err := b.Subscribe(topic, func(ctx context.Context, e broker.Event) error {
		m := e.Message()
		t.Logf("subscribe event: %v\n ", m.Body)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()

	m := map[string]any{
		"url":       "https://www.openai.com",
		"timestamp": now.String(),
	}

	body, _ := json.Marshal(m)

	if err := b.Publish(ctx, topic, &broker.Message{
		Body: body,
	}, rocketmq.WithShardingKey("a")); err != nil {
		t.Fatal(err)
	}

	time.Sleep(60 * time.Second)
}
