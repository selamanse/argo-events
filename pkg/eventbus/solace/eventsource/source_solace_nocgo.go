//go:build !cgo

package eventsource

import (
	"fmt"

	"go.uber.org/zap"

	eventbusv1alpha1 "github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
	eventbuscommon "github.com/argoproj/argo-events/pkg/eventbus/common"
)

type SolaceSource struct {
	config *eventbusv1alpha1.SolaceBus
	logger *zap.SugaredLogger
}

func NewSolaceSource(config *eventbusv1alpha1.SolaceBus, logger *zap.SugaredLogger) *SolaceSource {
	return &SolaceSource{config: config, logger: logger}
}

func (s *SolaceSource) Initialize() error {
	return nil
}

func (s *SolaceSource) Connect(clientID string) (eventbuscommon.EventSourceConnection, error) {
	return nil, fmt.Errorf("Solace event bus requires a CGO-enabled build")
}
