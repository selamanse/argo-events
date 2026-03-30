//go:build cgo

package solace

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/resource"

	"github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
	eventsourcecommon "github.com/argoproj/argo-events/pkg/eventsources/common"
	"github.com/argoproj/argo-events/pkg/eventsources/events"
	"github.com/argoproj/argo-events/pkg/eventsources/sources"
	metrics "github.com/argoproj/argo-events/pkg/metrics"
	"github.com/argoproj/argo-events/pkg/shared/logging"
	sharedutil "github.com/argoproj/argo-events/pkg/shared/util"
)

// EventListener implements Eventing for Solace event source
type EventListener struct {
	EventSourceName   string
	EventName         string
	SolaceEventSource v1alpha1.SolaceEventSource
	Metrics           *metrics.Metrics
}

// GetEventSourceName returns name of event source
func (el *EventListener) GetEventSourceName() string {
	return el.EventSourceName
}

// GetEventName returns name of event
func (el *EventListener) GetEventName() string {
	return el.EventName
}

// GetEventSourceType return type of event server
func (el *EventListener) GetEventSourceType() v1alpha1.EventSourceType {
	return v1alpha1.SolaceEvent
}

// StartListening starts listening events
func (el *EventListener) StartListening(ctx context.Context, dispatch func([]byte, ...eventsourcecommon.Option) error) error {
	log := logging.FromContext(ctx).
		With(logging.LabelEventSourceType, el.GetEventSourceType(), logging.LabelEventName, el.GetEventName())
	defer sources.Recover(el.GetEventName())

	log.Info("start solace event source...")
	solaceEventSource := &el.SolaceEventSource

	brokerProps := config.ServicePropertyMap{
		config.TransportLayerPropertyHost: solaceEventSource.URL,
	}

	vpn := "default"
	if solaceEventSource.VPN != "" {
		vpn = solaceEventSource.VPN
	}
	brokerProps[config.ServicePropertyVPNName] = vpn

	if solaceEventSource.Auth != nil {
		if solaceEventSource.Auth.Username != nil {
			username, err := sharedutil.GetSecretFromVolume(solaceEventSource.Auth.Username)
			if err != nil {
				return fmt.Errorf("failed to get Solace username: %w", err)
			}
			brokerProps[config.AuthenticationPropertySchemeBasicUserName] = username
		}
		if solaceEventSource.Auth.Password != nil {
			password, err := sharedutil.GetSecretFromVolume(solaceEventSource.Auth.Password)
			if err != nil {
				return fmt.Errorf("failed to get Solace password: %w", err)
			}
			brokerProps[config.AuthenticationPropertySchemeBasicPassword] = password
		}
	}

	builder := messaging.NewMessagingServiceBuilder().
		FromConfigurationProvider(brokerProps)

	if solaceEventSource.TLS != nil {
		tss := config.NewTransportSecurityStrategy()
		if solaceEventSource.TLS.InsecureSkipVerify {
			tss = tss.WithoutCertificateValidation()
		} else {
			tss = tss.WithCertificateValidation(true, false, "", "")
		}
		builder = builder.WithTransportSecurityStrategy(tss)
	}

	service, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed to build Solace messaging service: %w", err)
	}

	if err := service.Connect(); err != nil {
		return fmt.Errorf("failed to connect to Solace broker: %w", err)
	}
	defer service.Disconnect()

	// Use wildcard subscription to catch all messages under the topic tree
	subscriptionTopic := solaceEventSource.Topic
	log.Infow("subscribing to Solace topic", zap.String("topic", subscriptionTopic))

	receiver, err := service.CreateDirectMessageReceiverBuilder().
		WithSubscriptions(resource.TopicSubscriptionOf(subscriptionTopic)).
		Build()
	if err != nil {
		return fmt.Errorf("failed to create Solace receiver: %w", err)
	}

	if err := receiver.Start(); err != nil {
		return fmt.Errorf("failed to start Solace receiver: %w", err)
	}
	defer receiver.Terminate(0)

	log.Info("listening for Solace messages...")

	for {
		select {
		case <-ctx.Done():
			log.Info("stopping Solace event source")
			return nil
		default:
			msg, err := receiver.ReceiveMessage(5 * time.Second)
			if err != nil {
				// timeout is expected, check context and loop
				continue
			}

			func() {
				defer func(start time.Time) {
					el.Metrics.EventProcessingDuration(el.GetEventSourceName(), el.GetEventName(), float64(time.Since(start)/time.Millisecond))
				}(time.Now())

				payload, ok := msg.GetPayloadAsBytes()
				if !ok {
					log.Error("failed to get message payload as bytes")
					return
				}

				topic := msg.GetDestinationName()

				eventData := &events.SolaceEventData{
					Topic:    topic,
					Metadata: solaceEventSource.Metadata,
				}

				if solaceEventSource.JSONBody {
					eventData.Body = (*json.RawMessage)(&payload)
				} else {
					eventData.Body = payload
				}

				eventBody, err := json.Marshal(eventData)
				if err != nil {
					log.Errorw("failed to marshal event data", zap.Error(err))
					return
				}

				solaceID := fmt.Sprintf("%s:%s:%s:%s", el.GetEventSourceName(), el.GetEventName(), solaceEventSource.URL, topic)

				if err = dispatch(eventBody, eventsourcecommon.WithID(solaceID)); err != nil {
					log.Errorw("failed to dispatch Solace event", zap.Error(err))
				}
			}()
		}
	}
}
