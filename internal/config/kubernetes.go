package config

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func KubernetesRestConfig(c *Config) (*rest.Config, error) {
	var config *rest.Config
	var err error

	if c.UseKubeconfig {
		config, err = clientcmd.BuildConfigFromFlags("", c.KubeconfigPath)
	} else {
		config, err = rest.InClusterConfig()
	}

	if err != nil {
		return nil, err
	}

	// TODO This is a hack, need to review this
	config.QPS = 100
	config.Burst = 200

	return config, nil
}
