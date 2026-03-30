package installer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/argoproj/argo-events/pkg/apis/events/v1alpha1"
	"github.com/argoproj/argo-events/pkg/shared/logging"
)

const (
	testSolaceName = "test-solace"
	testSolaceURL  = "tcp://solace:55555"
)

var (
	testSolaceExoticBus = &v1alpha1.EventBus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "EventBus",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testSolaceName,
		},
		Spec: v1alpha1.EventBusSpec{
			Solace: &v1alpha1.SolaceBus{
				URL: testSolaceURL,
			},
		},
	}
)

func TestInstallationSolaceExotic(t *testing.T) {
	t.Run("installation with exotic solace config", func(t *testing.T) {
		installer := NewExoticSolaceInstaller(testSolaceExoticBus, logging.NewArgoEventsLogger())
		conf, err := installer.Install(context.TODO())
		assert.NoError(t, err)
		assert.NotNil(t, conf.Solace)
		assert.Equal(t, conf.Solace.URL, testSolaceURL)
	})

	t.Run("installation sets default topic", func(t *testing.T) {
		eb := testSolaceExoticBus.DeepCopy()
		eb.Spec.Solace.Topic = ""
		installer := NewExoticSolaceInstaller(eb, logging.NewArgoEventsLogger())
		conf, err := installer.Install(context.TODO())
		assert.NoError(t, err)
		assert.NotNil(t, conf.Solace)
		assert.Equal(t, conf.Solace.Topic, testNamespace+"-"+testSolaceName)
	})
}

func TestUninstallationSolaceExotic(t *testing.T) {
	t.Run("uninstallation with exotic solace config", func(t *testing.T) {
		installer := NewExoticSolaceInstaller(testSolaceExoticBus, logging.NewArgoEventsLogger())
		err := installer.Uninstall(context.TODO())
		assert.NoError(t, err)
	})
}
