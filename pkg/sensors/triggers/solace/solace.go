//go:build cgo

package solace

import (
	"context"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"
	"solace.dev/go/messaging/pkg/solace/resource"

	"github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
	solacebase "github.com/argoproj/argo-events/pkg/eventbus/solace/base"
	"github.com/argoproj/argo-events/pkg/sensors/triggers"
	"github.com/argoproj/argo-events/pkg/shared/logging"
	sharedutil "github.com/argoproj/argo-events/pkg/shared/util"
)

type messagePublisher interface {
	PublishBytes(message []byte, destination *resource.Topic) error
}

// SolaceTrigger describes the trigger to publish messages to a Solace topic.
type SolaceTrigger struct {
	Sensor    *v1alpha1.Sensor
	Trigger   *v1alpha1.Trigger
	Publisher messagePublisher
	Logger    *zap.SugaredLogger
}

func NewSolaceTrigger(sensor *v1alpha1.Sensor, trigger *v1alpha1.Trigger, solacePublishers sharedutil.StringKeyedMap[any], logger *zap.SugaredLogger) (*SolaceTrigger, error) {
	triggerLogger := logger.With(logging.LabelTriggerType, v1alpha1.TriggerTypeSolace)
	solaceTrigger := trigger.Template.Solace

	publisherValue, ok := solacePublishers.Load(trigger.Template.Name)
	var publisher messagePublisher
	if ok {
		var typeOK bool
		publisher, typeOK = publisherValue.(messagePublisher)
		if !typeOK {
			return nil, fmt.Errorf("stored Solace publisher for trigger %q has unexpected type %T", trigger.Template.Name, publisherValue)
		}
	}

	if !ok {
		bus := &v1alpha1.SolaceBus{
			URL:   solaceTrigger.URL,
			Topic: solaceTrigger.Topic,
			VPN:   solaceTrigger.VPN,
			TLS:   solaceTrigger.TLS,
			Auth:  solaceTrigger.Auth,
		}
		service, err := solacebase.NewSolace(bus, triggerLogger).NewMessagingService()
		if err != nil {
			return nil, err
		}
		if err := service.Connect(); err != nil {
			return nil, fmt.Errorf("failed to connect to Solace broker: %w", err)
		}

		directPublisher, err := service.CreateDirectMessagePublisherBuilder().Build()
		if err != nil {
			return nil, fmt.Errorf("failed to create Solace publisher: %w", err)
		}
		if err := directPublisher.Start(); err != nil {
			return nil, fmt.Errorf("failed to start Solace publisher: %w", err)
		}

		publisher = directPublisher
		solacePublishers.Store(trigger.Template.Name, publisher)
	}

	return &SolaceTrigger{
		Sensor:    sensor,
		Trigger:   trigger,
		Publisher: publisher,
		Logger:    triggerLogger,
	}, nil
}

// GetTriggerType returns the type of the trigger.
func (t *SolaceTrigger) GetTriggerType() v1alpha1.TriggerType {
	return v1alpha1.TriggerTypeSolace
}

// FetchResource fetches the trigger resource.
func (t *SolaceTrigger) FetchResource(ctx context.Context) (interface{}, error) {
	return t.Trigger.Template.Solace, nil
}

// ApplyResourceParameters applies parameters to the trigger resource.
func (t *SolaceTrigger) ApplyResourceParameters(events map[string]*v1alpha1.Event, resource interface{}) (interface{}, error) {
	fetchedResource, ok := resource.(*v1alpha1.SolaceTrigger)
	if !ok {
		return nil, fmt.Errorf("failed to interpret the fetched trigger resource")
	}

	resourceBytes, err := json.Marshal(fetchedResource)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal the solace trigger resource, %w", err)
	}

	parameters := fetchedResource.Parameters
	if parameters != nil {
		updatedResourceBytes, err := triggers.ApplyParams(resourceBytes, parameters, events)
		if err != nil {
			return nil, err
		}
		var st *v1alpha1.SolaceTrigger
		if err := json.Unmarshal(updatedResourceBytes, &st); err != nil {
			return nil, fmt.Errorf("failed to unmarshal the updated solace trigger resource after applying resource parameters, %w", err)
		}
		return st, nil
	}
	return resource, nil
}

// Execute executes the trigger.
func (t *SolaceTrigger) Execute(ctx context.Context, events map[string]*v1alpha1.Event, resourceObj interface{}) (interface{}, error) {
	trigger, ok := resourceObj.(*v1alpha1.SolaceTrigger)
	if !ok {
		return nil, fmt.Errorf("failed to interpret the trigger resource")
	}
	if trigger.Payload == nil {
		return nil, fmt.Errorf("payload parameters are not specified")
	}

	payload, err := triggers.ConstructPayload(events, trigger.Payload)
	if err != nil {
		return nil, err
	}

	if err := t.Publisher.PublishBytes(payload, resource.TopicOf(trigger.Topic)); err != nil {
		return nil, fmt.Errorf("failed to publish message to solace topic %q, %w", trigger.Topic, err)
	}

	t.Logger.Infow("successfully published message to solace", zap.String("topic", trigger.Topic))
	return nil, nil
}

// ApplyPolicy applies policy on the trigger.
func (t *SolaceTrigger) ApplyPolicy(ctx context.Context, resource interface{}) error {
	return nil
}
