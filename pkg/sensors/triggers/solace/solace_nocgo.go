//go:build !cgo

package solace

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
	sharedutil "github.com/argoproj/argo-events/pkg/shared/util"
)

type SolaceTrigger struct {
	Sensor  *v1alpha1.Sensor
	Trigger *v1alpha1.Trigger
	Logger  *zap.SugaredLogger
}

func NewSolaceTrigger(sensor *v1alpha1.Sensor, trigger *v1alpha1.Trigger, solacePublishers sharedutil.StringKeyedMap[any], logger *zap.SugaredLogger) (*SolaceTrigger, error) {
	return &SolaceTrigger{
		Sensor:  sensor,
		Trigger: trigger,
		Logger:  logger,
	}, nil
}

func (t *SolaceTrigger) GetTriggerType() v1alpha1.TriggerType {
	return v1alpha1.TriggerTypeSolace
}

func (t *SolaceTrigger) FetchResource(ctx context.Context) (interface{}, error) {
	return t.Trigger.Template.Solace, nil
}

func (t *SolaceTrigger) ApplyResourceParameters(events map[string]*v1alpha1.Event, resource interface{}) (interface{}, error) {
	return resource, nil
}

func (t *SolaceTrigger) Execute(ctx context.Context, events map[string]*v1alpha1.Event, resourceObj interface{}) (interface{}, error) {
	return nil, fmt.Errorf("Solace trigger requires a CGO-enabled build")
}

func (t *SolaceTrigger) ApplyPolicy(ctx context.Context, resource interface{}) error {
	return nil
}
