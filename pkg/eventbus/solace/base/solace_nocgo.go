//go:build !cgo

package base

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
)

type Solace struct {
	Logger *zap.SugaredLogger
	config *v1alpha1.SolaceBus
}

func NewSolace(cfg *v1alpha1.SolaceBus, logger *zap.SugaredLogger) *Solace {
	return &Solace{
		Logger: logger,
		config: cfg,
	}
}

func (s *Solace) URL() string {
	return s.config.URL
}

func (s *Solace) Topic() string {
	return s.config.Topic
}

func (s *Solace) VPN() string {
	if s.config.VPN != "" {
		return s.config.VPN
	}
	return "default"
}

func (s *Solace) NewMessagingService() (any, error) {
	return nil, fmt.Errorf("Solace event bus requires a CGO-enabled build")
}

func EventKey(source string, subject string) string {
	return fmt.Sprintf("%s.%s", source, subject)
}
