//go:build !cgo

package solace

import (
	"context"
	"fmt"

	"github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
	eventsourcecommon "github.com/argoproj/argo-events/pkg/eventsources/common"
	metrics "github.com/argoproj/argo-events/pkg/metrics"
)

// EventListener implements Eventing for Solace event source.
type EventListener struct {
	EventSourceName   string
	EventName         string
	SolaceEventSource v1alpha1.SolaceEventSource
	Metrics           *metrics.Metrics
}

func (el *EventListener) GetEventSourceName() string {
	return el.EventSourceName
}

func (el *EventListener) GetEventName() string {
	return el.EventName
}

func (el *EventListener) GetEventSourceType() v1alpha1.EventSourceType {
	return v1alpha1.SolaceEvent
}

func (el *EventListener) StartListening(context.Context, func([]byte, ...eventsourcecommon.Option) error) error {
	return fmt.Errorf("Solace event source requires a CGO-enabled build")
}
