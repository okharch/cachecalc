//go:build k8s

package cluster

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type KubernetesLeaderElector struct {
	client     kubernetes.Interface
	leaseName  string
	namespace  string
	identity   string
	leaderAddr atomic.Value
	isLeader   atomic.Bool
}

func newKubernetesLeaderElector(cfg Config) (LeaderElector, error) {
	namespace := envOrDefault("POD_NAMESPACE", "default")
	leaseName := envOrDefault("LEADER_LEASE_NAME", "cachecalc-leader")
	identity := cfg.LeaderAddress
	if identity == "" {
		identity = envOrDefault("POD_IP", os.Getenv("HOSTNAME"))
		if identity == "" {
			return nil, fmt.Errorf("LEADER_ADDR or POD_IP must be set for k8s mode")
		}
		if !strings.Contains(identity, ":") {
			_, port, err := splitHostPortLoose(cfg.GRPCListenAddress)
			if err != nil {
				return nil, err
			}
			identity = identity + ":" + port
		}
	}
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	return &KubernetesLeaderElector{
		client:    kubernetes.NewForConfigOrDie(restCfg),
		leaseName: leaseName,
		namespace: namespace,
		identity:  identity,
	}, nil
}

func (e *KubernetesLeaderElector) Start(ctx context.Context, onStartLeading func(), onStopLeading func()) error {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: e.leaseName, Namespace: e.namespace},
		Client:    e.client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: e.identity,
		},
	}
	go e.pollLeader(ctx)
	go leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(context.Context) {
				e.isLeader.Store(true)
				e.leaderAddr.Store(e.identity)
				onStartLeading()
			},
			OnStoppedLeading: func() {
				e.isLeader.Store(false)
				onStopLeading()
			},
			OnNewLeader: func(identity string) {
				if identity != "" {
					e.leaderAddr.Store(identity)
				}
			},
		},
	})
	return nil
}

func (e *KubernetesLeaderElector) IsLeader() bool { return e.isLeader.Load() }
func (e *KubernetesLeaderElector) LeaderAddress() string {
	value := e.leaderAddr.Load()
	if value == nil {
		return ""
	}
	return value.(string)
}

func (e *KubernetesLeaderElector) pollLeader(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		e.refreshLeaderAddress(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (e *KubernetesLeaderElector) refreshLeaderAddress(ctx context.Context) {
	lease, err := e.client.CoordinationV1().Leases(e.namespace).Get(ctx, e.leaseName, metav1.GetOptions{})
	if err != nil {
		return
	}
	record := lease.Spec.HolderIdentity
	if record != nil && *record != "" {
		e.leaderAddr.Store(*record)
	}
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func splitHostPortLoose(addr string) (string, string, error) {
	if strings.HasPrefix(addr, ":") {
		return "", strings.TrimPrefix(addr, ":"), nil
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", "", err
	}
	return host, port, nil
}
