//go:build !k8s

package cluster

import "fmt"

// newKubernetesLeaderElector is a stub in default builds so the package can be
// used without pulling Kubernetes-specific behavior into non-k8s deployments.
func newKubernetesLeaderElector(cfg Config) (LeaderElector, error) {
	return nil, fmt.Errorf("kubernetes leader election requires building with -tags k8s")
}
