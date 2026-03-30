//go:build cgo

package base

import (
	"fmt"

	"go.uber.org/zap"
	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"

	"github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
	sharedutil "github.com/argoproj/argo-events/pkg/shared/util"
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

// NewMessagingService creates a configured Solace messaging service.
func (s *Solace) NewMessagingService() (solace.MessagingService, error) {
	brokerProps := config.ServicePropertyMap{
		config.TransportLayerPropertyHost: s.config.URL,
		config.ServicePropertyVPNName:     s.VPN(),
	}

	if s.config.Auth != nil {
		if s.config.Auth.Username != nil {
			username, err := sharedutil.GetSecretFromVolume(s.config.Auth.Username)
			if err != nil {
				return nil, fmt.Errorf("failed to get Solace username: %w", err)
			}
			brokerProps[config.AuthenticationPropertySchemeBasicUserName] = username
		}
		if s.config.Auth.Password != nil {
			password, err := sharedutil.GetSecretFromVolume(s.config.Auth.Password)
			if err != nil {
				return nil, fmt.Errorf("failed to get Solace password: %w", err)
			}
			brokerProps[config.AuthenticationPropertySchemeBasicPassword] = password
		}
	}

	builder := messaging.NewMessagingServiceBuilder().
		FromConfigurationProvider(brokerProps)

	if s.config.TLS != nil {
		tss := config.NewTransportSecurityStrategy()
		if s.config.TLS.InsecureSkipVerify {
			tss = tss.WithoutCertificateValidation()
		} else {
			trustStorePath := ""
			tss = tss.WithCertificateValidation(true, false, trustStorePath, "")
		}
		builder = builder.WithTransportSecurityStrategy(tss)
	}

	service, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build Solace messaging service: %w", err)
	}

	return service, nil
}

// EventKey creates a consistent key from source and subject.
func EventKey(source string, subject string) string {
	return fmt.Sprintf("%s.%s", source, subject)
}
