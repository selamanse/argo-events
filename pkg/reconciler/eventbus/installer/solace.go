package installer

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
)

// exoticSolaceInstaller is an installer implementation of exotic solace config.
type exoticSolaceInstaller struct {
	eventBus *v1alpha1.EventBus

	logger *zap.SugaredLogger
}

// NewExoticSolaceInstaller returns a new exoticSolaceInstaller
func NewExoticSolaceInstaller(eventBus *v1alpha1.EventBus, logger *zap.SugaredLogger) Installer {
	return &exoticSolaceInstaller{
		eventBus: eventBus,
		logger:   logger.Named("exotic-solace"),
	}
}

func (i *exoticSolaceInstaller) Install(ctx context.Context) (*v1alpha1.BusConfig, error) {
	solaceObj := i.eventBus.Spec.Solace
	if solaceObj == nil {
		return nil, fmt.Errorf("invalid request")
	}
	if solaceObj.Topic == "" {
		solaceObj.Topic = fmt.Sprintf("%s-%s", i.eventBus.Namespace, i.eventBus.Name)
	}

	i.eventBus.Status.MarkDeployed("Skipped", "Skip deployment because of using exotic config.")
	i.logger.Info("use exotic config")
	busConfig := &v1alpha1.BusConfig{
		Solace: solaceObj,
	}
	return busConfig, nil
}

func (i *exoticSolaceInstaller) Uninstall(ctx context.Context) error {
	i.logger.Info("nothing to uninstall")
	return nil
}
