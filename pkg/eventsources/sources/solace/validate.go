package solace

import (
	"context"
	"fmt"

	"github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
)

// ValidateEventSource validates the Solace event source
func (el *EventListener) ValidateEventSource(ctx context.Context) error {
	return validate(&el.SolaceEventSource)
}

func validate(eventSource *v1alpha1.SolaceEventSource) error {
	if eventSource == nil {
		return v1alpha1.ErrNilEventSource
	}
	if eventSource.URL == "" {
		return fmt.Errorf("url must be specified")
	}
	if eventSource.Topic == "" {
		return fmt.Errorf("topic must be specified")
	}
	if eventSource.TLS != nil {
		return v1alpha1.ValidateTLSConfig(eventSource.TLS)
	}
	return nil
}
