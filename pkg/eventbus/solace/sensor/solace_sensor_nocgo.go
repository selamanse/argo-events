//go:build !cgo

package solace

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
	eventbuscommon "github.com/argoproj/argo-events/pkg/eventbus/common"
)

type SolaceSensor struct{}

func NewSolaceSensor(solaceConfig *v1alpha1.SolaceBus, sensor *v1alpha1.Sensor, hostname string, logger *zap.SugaredLogger) *SolaceSensor {
	return &SolaceSensor{}
}

func (s *SolaceSensor) Initialize() error {
	return fmt.Errorf("Solace event bus requires a CGO-enabled build")
}

func (s *SolaceSensor) Connect(ctx context.Context, triggerName string, depExpression string, dependencies []eventbuscommon.Dependency, atLeastOnce bool) (eventbuscommon.TriggerConnection, error) {
	return nil, fmt.Errorf("Solace event bus requires a CGO-enabled build")
}
