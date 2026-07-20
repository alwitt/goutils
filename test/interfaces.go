// Package test - various support components used in unit-testing.
package test

import (
	"context"

	"github.com/alwitt/goutils/redis"
)

// UnitTestCallbackCollector unit-testing interface for collecting callbacks
type UnitTestCallbackCollector interface {
	// CollectRedisSubscribeMsgs called when REDIS PubSub subscriber has a message to forward
	CollectRedisSubscribeMsgs(ctx context.Context, msg redis.PubSubMessage)
}
