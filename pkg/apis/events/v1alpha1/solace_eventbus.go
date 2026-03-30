package v1alpha1

import corev1 "k8s.io/api/core/v1"

// SolaceBus holds the Solace EventBus information
type SolaceBus struct {
	// URL to Solace broker, e.g. tcp://localhost:55555
	URL string `json:"url,omitempty" protobuf:"bytes,1,opt,name=url"`
	// Topic prefix for publishing/subscribing, defaults to {namespace_name}-{eventbus_name}
	// +optional
	Topic string `json:"topic,omitempty" protobuf:"bytes,2,opt,name=topic"`
	// VPN is the Solace message VPN name
	// +optional
	VPN string `json:"vpn,omitempty" protobuf:"bytes,3,opt,name=vpn"`
	// TLS configuration for the Solace client
	// +optional
	TLS *TLSConfig `json:"tls,omitempty" protobuf:"bytes,4,opt,name=tls"`
	// Auth contains the authentication configuration
	// +optional
	Auth *SolaceAuth `json:"auth,omitempty" protobuf:"bytes,5,opt,name=auth"`
}

// SolaceAuth contains authentication configuration for Solace
type SolaceAuth struct {
	// Username secret for Solace authentication
	// +optional
	Username *corev1.SecretKeySelector `json:"username,omitempty" protobuf:"bytes,1,opt,name=username"`
	// Password secret for Solace authentication
	// +optional
	Password *corev1.SecretKeySelector `json:"password,omitempty" protobuf:"bytes,2,opt,name=password"`
}

// SolaceEventSource refers to event-source for Solace related events
type SolaceEventSource struct {
	// URL to Solace broker, e.g. tcp://solace:55555
	URL string `json:"url" protobuf:"bytes,1,opt,name=url"`
	// Topic to subscribe to
	Topic string `json:"topic" protobuf:"bytes,2,opt,name=topic"`
	// VPN is the Solace message VPN name
	// +optional
	VPN string `json:"vpn,omitempty" protobuf:"bytes,3,opt,name=vpn"`
	// TLS configuration for the Solace client
	// +optional
	TLS *TLSConfig `json:"tls,omitempty" protobuf:"bytes,4,opt,name=tls"`
	// Auth contains the authentication configuration
	// +optional
	Auth *SolaceAuth `json:"auth,omitempty" protobuf:"bytes,5,opt,name=auth"`
	// JSONBody specifies that all event body payload coming from this
	// source will be JSON
	// +optional
	JSONBody bool `json:"jsonBody,omitempty" protobuf:"varint,6,opt,name=jsonBody"`
	// Metadata holds the user defined metadata which will passed along the event payload.
	// +optional
	Metadata map[string]string `json:"metadata,omitempty" protobuf:"bytes,7,rep,name=metadata"`
	// Filter
	// +optional
	Filter *EventSourceFilter `json:"filter,omitempty" protobuf:"bytes,8,opt,name=filter"`
}
