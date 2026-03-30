//go:build cgo

package solace

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Knetic/govaluate"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"
	solaceapi "solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"

	"github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
	eventbuscommon "github.com/argoproj/argo-events/pkg/eventbus/common"
	"github.com/argoproj/argo-events/pkg/eventbus/solace/base"
)

type SolaceSensor struct {
	*base.Solace
	*sync.Mutex
	sensor *v1alpha1.Sensor

	// solace details
	topic    string
	service  solaceapi.MessagingService
	hostname string

	// trigger handlers
	triggers  Triggers
	connected bool
}

func NewSolaceSensor(solaceConfig *v1alpha1.SolaceBus, sensor *v1alpha1.Sensor, hostname string, logger *zap.SugaredLogger) *SolaceSensor {
	return &SolaceSensor{
		Solace:   base.NewSolace(solaceConfig, logger),
		Mutex:    &sync.Mutex{},
		sensor:   sensor,
		topic:    solaceConfig.Topic,
		hostname: hostname,
		triggers: Triggers{},
	}
}

type Triggers map[string]*SolaceTriggerConnection

func (t Triggers) Ready() bool {
	for _, trigger := range t {
		if !trigger.Ready() {
			return false
		}
	}
	return true
}

func (s *SolaceSensor) Initialize() error {
	service, err := s.NewMessagingService()
	if err != nil {
		return err
	}

	if err := service.Connect(); err != nil {
		return err
	}

	s.service = service
	return nil
}

func (s *SolaceSensor) Connect(ctx context.Context, triggerName string, depExpression string, dependencies []eventbuscommon.Dependency, atLeastOnce bool) (eventbuscommon.TriggerConnection, error) {
	s.Lock()
	defer s.Unlock()

	if !s.connected {
		go s.Listen(ctx)
		s.connected = true
	}

	if _, ok := s.triggers[triggerName]; !ok {
		expr, err := govaluate.NewEvaluableExpression(strings.ReplaceAll(depExpression, "-", "\\-"))
		if err != nil {
			return nil, err
		}

		sourceDepMap := make(map[string][]string)
		for _, d := range dependencies {
			key := base.EventKey(d.EventSourceName, d.EventName)
			_, found := sourceDepMap[key]
			if !found {
				sourceDepMap[key] = make([]string, 0)
			}
			sourceDepMap[key] = append(sourceDepMap[key], d.Name)
		}

		s.triggers[triggerName] = &SolaceTriggerConnection{
			SolaceConnection: base.NewSolaceConnection(s.Logger),
			sensorName:       s.sensor.Name,
			triggerName:      triggerName,
			depExpression:    expr,
			sourceDepMap:     sourceDepMap,
			atLeastOnce:      atLeastOnce,
			close:            s.Close,
			isClosed:         s.IsClosed,
		}
	}

	return s.triggers[triggerName], nil
}

func (s *SolaceSensor) Listen(ctx context.Context) {
	defer s.Disconnect()

	for {
		if len(s.triggers) != len(s.sensor.Spec.Triggers) || !s.triggers.Ready() {
			s.Logger.Info("Not ready to consume, waiting...")
			time.Sleep(3 * time.Second)
			continue
		}

		// Subscribe to all events on the topic tree using Solace wildcard
		subscriptionTopic := fmt.Sprintf("%s/>", s.topic)
		s.Logger.Infow("Subscribing", zap.String("topic", subscriptionTopic))

		subscriber, err := s.service.CreateDirectMessageReceiverBuilder().
			WithSubscriptions(resource.TopicSubscriptionOf(subscriptionTopic)).
			Build()
		if err != nil {
			s.Logger.Errorw("Failed to create subscriber", zap.Error(err))
			return
		}

		if err := subscriber.Start(); err != nil {
			s.Logger.Errorw("Failed to start subscriber", zap.Error(err))
			return
		}

		for {
			// Use a 5-second timeout so we can check context cancellation
			msg, err := subscriber.ReceiveMessage(5 * time.Second)
			if err != nil {
				if ctx.Err() != nil {
					s.Logger.Info("Context cancelled, stopping listener")
					subscriber.Terminate(0)
					return
				}
				// Timeout is expected, loop and check context again
				continue
			}

			s.handleMessage(msg)
		}
	}
}

func (s *SolaceSensor) handleMessage(msg message.InboundMessage) {
	payload, ok := msg.GetPayloadAsBytes()
	if !ok {
		s.Logger.Error("Failed to get message payload as bytes")
		return
	}

	var event cloudevents.Event
	if err := json.Unmarshal(payload, &event); err != nil {
		s.Logger.Errorw("Failed to deserialize cloudevent, skipping", zap.Error(err))
		return
	}

	for _, trigger := range s.triggers {
		depNames, ok := trigger.DependsOn(&event)
		if !ok {
			continue
		}

		for _, depName := range depNames {
			transformed, err := trigger.Transform(depName, &event)
			if err != nil {
				s.Logger.Errorw("Failed to transform cloudevent, skipping", zap.Error(err))
				continue
			}

			if !trigger.Filter(depName, transformed) {
				s.Logger.Debug("Filter condition not satisfied, skipping")
				continue
			}

			if trigger.OneAndDone() {
				eventMap := map[string]cloudevents.Event{
					depName: *transformed,
				}
				trigger.action(eventMap)
			} else {
				events, err := trigger.Update(transformed, depName)
				if err != nil {
					s.Logger.Errorw("Failed to update trigger, skipping", zap.Error(err))
					continue
				}
				if events != nil {
					eventMap := map[string]cloudevents.Event{}
					for name, ev := range events {
						eventMap[name] = *ev
					}
					trigger.action(eventMap)
				}
			}
		}
	}
}

func (s *SolaceSensor) Disconnect() {
	s.Lock()
	defer s.Unlock()
	s.connected = false
}

func (s *SolaceSensor) Close() error {
	s.Lock()
	defer s.Unlock()

	if s.IsClosed() {
		return nil
	}

	s.connected = false
	if s.service != nil {
		s.service.Disconnect()
	}
	return nil
}

func (s *SolaceSensor) IsClosed() bool {
	return !s.connected || s.service == nil || !s.service.IsConnected()
}
