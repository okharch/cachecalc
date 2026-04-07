//go:build !k8s

package cluster

import "fmt"

func newKubernetesLeaderElector(cfg Config) (LeaderElector, error) {
	_ = cfg
	return nil, fmt.Errorf("k8s mode requires building with -tags k8s")
}
