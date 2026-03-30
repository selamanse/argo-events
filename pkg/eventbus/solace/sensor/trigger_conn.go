package solace

import (
	"context"
	"fmt"
	"time"

	"github.com/Knetic/govaluate"
	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/argoproj/argo-events/pkg/eventbus/solace/base"
)

type SolaceTriggerConnection struct {
	*base.SolaceConnection

	sensorName    string
	triggerName   string
	depExpression *govaluate.EvaluableExpression
	atLeastOnce   bool
	sourceDepMap  map[string][]string

	// functions
	close     func() error
	isClosed  func() bool
	transform func(string, cloudevents.Event) (*cloudevents.Event, error)
	filter    func(string, cloudevents.Event) bool
	action    func(map[string]cloudevents.Event)

	// state
	events        map[string]*cloudevents.Event
	lastResetTime time.Time
}

func (c *SolaceTriggerConnection) String() string {
	return fmt.Sprintf("SolaceTriggerConnection{Sensor:%s,Trigger:%s}", c.sensorName, c.triggerName)
}

func (c *SolaceTriggerConnection) Close() error {
	if c.close == nil {
		return fmt.Errorf("can't close Solace trigger connection, close function is nil")
	}
	return c.close()
}

func (c *SolaceTriggerConnection) IsClosed() bool {
	return c.isClosed == nil || c.isClosed()
}

func (c *SolaceTriggerConnection) Subscribe(
	ctx context.Context,
	closeCh <-chan struct{},
	resetConditionsCh <-chan struct{},
	lastResetTime time.Time,
	transform func(depName string, event cloudevents.Event) (*cloudevents.Event, error),
	filter func(string, cloudevents.Event) bool,
	action func(map[string]cloudevents.Event),
	topic *string) error {
	c.transform = transform
	c.filter = filter
	c.action = action
	c.lastResetTime = lastResetTime
	c.events = make(map[string]*cloudevents.Event)

	for {
		select {
		case <-ctx.Done():
			return c.Close()
		case <-closeCh:
			return nil
		case <-resetConditionsCh:
			c.lastResetTime = time.Now()
			c.events = make(map[string]*cloudevents.Event)
		}
	}
}

func (c *SolaceTriggerConnection) Ready() bool {
	return c.transform != nil && c.filter != nil && c.action != nil
}

func (c *SolaceTriggerConnection) DependsOn(event *cloudevents.Event) ([]string, bool) {
	depNames, ok := c.sourceDepMap[base.EventKey(event.Source(), event.Subject())]
	return depNames, ok
}

func (c *SolaceTriggerConnection) OneAndDone() bool {
	for _, token := range c.depExpression.Tokens() {
		if token.Kind == govaluate.LOGICALOP && token.Value == "&&" {
			return false
		}
	}
	return true
}

func (c *SolaceTriggerConnection) Transform(depName string, event *cloudevents.Event) (*cloudevents.Event, error) {
	return c.transform(depName, *event)
}

func (c *SolaceTriggerConnection) Filter(depName string, event *cloudevents.Event) bool {
	return c.filter(depName, *event)
}

func (c *SolaceTriggerConnection) Update(event *cloudevents.Event, depName string) (map[string]*cloudevents.Event, error) {
	c.events[depName] = event

	satisfied, err := c.satisfied()
	if err != nil {
		return nil, err
	}

	if satisfied == true {
		result := c.events
		c.events = make(map[string]*cloudevents.Event)
		return result, nil
	}

	return nil, nil
}

func (c *SolaceTriggerConnection) satisfied() (interface{}, error) {
	parameters := Parameters{}
	for depName := range c.events {
		parameters[depName] = true
	}
	return c.depExpression.Eval(parameters)
}

type Parameters map[string]bool

func (p Parameters) Get(name string) (interface{}, error) {
	return p[name], nil
}
