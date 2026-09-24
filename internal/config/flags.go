//go:generate go run github.com/octopusdeploy/kubernetes-monitor/pkg/genflags flags_gen.go
package config

type FlagSetter interface {
	InitFlags()
}
