package kwok_integration

import (
	"context"

	"sigs.k8s.io/e2e-framework/pkg/envconf"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func createNode(ctx context.Context, cfg *envconf.Config, nodeName string) error {
	node := newNode(nodeName)
	return cfg.Client().Resources().Create(ctx, node)
}

func newNode(nodeName string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Labels: map[string]string{
				"beta.kubernetes.io/arch": "amd64",
				"beta.kubernetes.io/os":   "linux",
				"type":                    "kwok",
			},
		},
		Spec: corev1.NodeSpec{},
	}
}
