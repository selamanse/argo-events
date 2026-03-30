//go:build cgo

package eventsource

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/resource"

	"github.com/argoproj/argo-events/pkg/eventbus/common"
	"github.com/argoproj/argo-events/pkg/eventbus/solace/base"
)

type SolaceSourceConnection struct {
	*base.SolaceConnection
	Topic     string
	Service   solace.MessagingService
	Publisher solace.DirectMessagePublisher
	closed    bool
}

func (c *SolaceSourceConnection) Publish(ctx context.Context, msg common.Message) error {
	key := base.EventKey(msg.EventSourceName, msg.EventName)
	topic := resource.TopicOf(fmt.Sprintf("%s/%s", c.Topic, key))

	if err := c.Publisher.PublishBytes(msg.Body, topic); err != nil {
		return fmt.Errorf("failed to publish to Solace topic %s: %w", topic.GetName(), err)
	}

	c.Logger.Infow("Published message to Solace",
		zap.String("topic", topic.GetName()),
		zap.String("key", key))

	return nil
}

func (c *SolaceSourceConnection) Close() error {
	c.closed = true
	c.Publisher.Terminate(0)
	c.Service.Disconnect()
	return nil
}

func (c *SolaceSourceConnection) IsClosed() bool {
	return c.closed || !c.Service.IsConnected()
}
