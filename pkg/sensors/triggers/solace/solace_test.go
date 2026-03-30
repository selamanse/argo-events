package solace

import (
	"context"
	"fmt"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"solace.dev/go/messaging/pkg/solace/resource"

	"github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
	"github.com/argoproj/argo-events/pkg/shared/logging"
)

type mockPublisher struct {
	expectedTopic   string
	expectedPayload []byte
}

func (m *mockPublisher) PublishBytes(message []byte, destination *resource.Topic) error {
	if destination == nil {
		return fmt.Errorf("destination is nil")
	}
	if destination.GetName() != m.expectedTopic {
		return fmt.Errorf("unexpected topic: %s", destination.GetName())
	}
	if string(message) != string(m.expectedPayload) {
		return fmt.Errorf("unexpected payload: %s", string(message))
	}
	return nil
}

var sensorObj = &v1alpha1.Sensor{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "fake-sensor",
		Namespace: "fake",
	},
	Spec: v1alpha1.SensorSpec{
		Triggers: []v1alpha1.Trigger{
			{
				Template: &v1alpha1.TriggerTemplate{
					Name: "fake-trigger",
					Solace: &v1alpha1.SolaceTrigger{
						URL:   "tcp://fake-solace:55555",
						Topic: "fake/topic",
					},
				},
			},
		},
	},
}

func getFakeSolaceTrigger() *SolaceTrigger {
	return &SolaceTrigger{
		Sensor:    sensorObj.DeepCopy(),
		Trigger:   sensorObj.Spec.Triggers[0].DeepCopy(),
		Publisher: &mockPublisher{},
		Logger:    logging.NewArgoEventsLogger(),
	}
}

func TestSolaceTrigger_FetchResource(t *testing.T) {
	trigger := getFakeSolaceTrigger()
	obj, err := trigger.FetchResource(context.TODO())
	assert.NoError(t, err)
	assert.NotNil(t, obj)

	resource, ok := obj.(*v1alpha1.SolaceTrigger)
	assert.True(t, ok)
	assert.Equal(t, "tcp://fake-solace:55555", resource.URL)
	assert.Equal(t, "fake/topic", resource.Topic)
}

func TestSolaceTrigger_ApplyResourceParameters(t *testing.T) {
	trigger := getFakeSolaceTrigger()

	testEvents := map[string]*v1alpha1.Event{
		"fake-dependency": {
			Context: &v1alpha1.EventContext{
				ID:              "1",
				Type:            "webhook",
				Source:          "webhook-gateway",
				DataContentType: "application/json",
				SpecVersion:     cloudevents.VersionV1,
				Subject:         "example-1",
			},
			Data: []byte(`{"topic":"another/topic","url":"tcp://another-solace:55555"}`),
		},
	}

	defaultValue := "default"
	trigger.Trigger.Template.Solace.Parameters = []v1alpha1.TriggerParameter{
		{
			Src: &v1alpha1.TriggerParameterSource{
				DependencyName: "fake-dependency",
				DataKey:        "topic",
				Value:          &defaultValue,
			},
			Dest: "topic",
		},
		{
			Src: &v1alpha1.TriggerParameterSource{
				DependencyName: "fake-dependency",
				DataKey:        "url",
				Value:          &defaultValue,
			},
			Dest: "url",
		},
	}

	resource, err := trigger.ApplyResourceParameters(testEvents, trigger.Trigger.Template.Solace)
	assert.NoError(t, err)

	updatedTrigger, ok := resource.(*v1alpha1.SolaceTrigger)
	assert.True(t, ok)
	assert.Equal(t, "another/topic", updatedTrigger.Topic)
	assert.Equal(t, "tcp://another-solace:55555", updatedTrigger.URL)
}

func TestSolaceTrigger_Execute(t *testing.T) {
	trigger := getFakeSolaceTrigger()
	trigger.Publisher = &mockPublisher{
		expectedTopic:   "fake/topic",
		expectedPayload: []byte(`{"message":"world"}`),
	}

	testEvents := map[string]*v1alpha1.Event{
		"fake-dependency": {
			Context: &v1alpha1.EventContext{
				ID:              "1",
				Type:            "webhook",
				Source:          "webhook-gateway",
				DataContentType: "application/json",
				SpecVersion:     cloudevents.VersionV1,
				Subject:         "example-1",
			},
			Data: []byte(`{"message":"world"}`),
		},
	}

	defaultValue := "hello"
	trigger.Trigger.Template.Solace.Payload = []v1alpha1.TriggerParameter{
		{
			Src: &v1alpha1.TriggerParameterSource{
				DependencyName: "fake-dependency",
				DataKey:        "message",
				Value:          &defaultValue,
			},
			Dest: "message",
		},
	}

	result, err := trigger.Execute(context.TODO(), testEvents, trigger.Trigger.Template.Solace)
	assert.NoError(t, err)
	assert.Nil(t, result)
}
