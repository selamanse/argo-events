//go:build cgo

package eventsource

import (
	"go.uber.org/zap"

	eventbusv1alpha1 "github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
	eventbuscommon "github.com/argoproj/argo-events/pkg/eventbus/common"
	"github.com/argoproj/argo-events/pkg/eventbus/solace/base"
)

type SolaceSource struct {
	*base.Solace
	topic string
}

func NewSolaceSource(config *eventbusv1alpha1.SolaceBus, logger *zap.SugaredLogger) *SolaceSource {
	return &SolaceSource{
		Solace: base.NewSolace(config, logger),
		topic:  config.Topic,
	}
}

func (s *SolaceSource) Initialize() error {
	return nil
}

func (s *SolaceSource) Connect(clientID string) (eventbuscommon.EventSourceConnection, error) {
	service, err := s.NewMessagingService()
	if err != nil {
		return nil, err
	}

	if err := service.Connect(); err != nil {
		return nil, err
	}

	publisher, err := service.CreateDirectMessagePublisherBuilder().Build()
	if err != nil {
		service.Disconnect()
		return nil, err
	}

	if err := publisher.Start(); err != nil {
		service.Disconnect()
		return nil, err
	}

	conn := &SolaceSourceConnection{
		SolaceConnection: base.NewSolaceConnection(s.Logger),
		Topic:            s.topic,
		Service:          service,
		Publisher:        publisher,
	}

	return conn, nil
}
