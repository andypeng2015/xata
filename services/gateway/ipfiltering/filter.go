package ipfiltering

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	// ConfigMapKey is the key in the ConfigMap data that contains IP filtering configuration
	ConfigMapKey = "ipfiltering.json"
)

// IPFilteringConfig represents the IP filtering configuration for a branch
type IPFilteringConfig struct {
	Enabled bool     `json:"enabled"`
	Allowed []string `json:"allowed"`
}

// Filter provides thread-safe access to IP filtering rules
type Filter struct {
	mu    sync.RWMutex
	rules map[string]*branchRules // branchID -> rules
}

type branchRules struct {
	enabled  bool
	prefixes []netip.Prefix
}

// NewFilter creates a new IP filter that watches a ConfigMap via the Kubernetes API
func NewFilter(ctx context.Context, kubeClient kubernetes.Interface, namespace, configMapName string) (*Filter, error) {
	f := &Filter{
		rules: make(map[string]*branchRules),
	}

	log.Info().
		Str("namespace", namespace).
		Str("configmap", configMapName).
		Msg("initializing IP filter, watching ConfigMap via Kubernetes API")

	fieldSelector := fields.OneTermEqualSelector("metadata.name", configMapName).String()
	lw := cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = fieldSelector
			return kubeClient.CoreV1().ConfigMaps(namespace).List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			options.FieldSelector = fieldSelector
			return kubeClient.CoreV1().ConfigMaps(namespace).Watch(ctx, options)
		},
	}, kubeClient)

	_, informer := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: lw,
		ObjectType:    &corev1.ConfigMap{},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				cm, ok := obj.(*corev1.ConfigMap)
				if !ok {
					return
				}
				f.handleConfigMap(cm)
			},
			UpdateFunc: func(_, newObj any) {
				cm, ok := newObj.(*corev1.ConfigMap)
				if !ok {
					return
				}
				f.handleConfigMap(cm)
			},
			DeleteFunc: func(_ any) {
				log.Info().Msg("ConfigMap deleted, clearing all IP filtering rules")
				f.mu.Lock()
				f.rules = make(map[string]*branchRules)
				f.mu.Unlock()
			},
		},
	})

	stopCh := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(stopCh)
	}()
	go informer.Run(stopCh)

	syncCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if !cache.WaitForCacheSync(syncCtx.Done(), informer.HasSynced) {
		return nil, fmt.Errorf("timed out waiting for ConfigMap informer cache sync")
	}

	log.Info().Msg("IP filter ConfigMap informer synced")

	return f, nil
}

func (f *Filter) handleConfigMap(cm *corev1.ConfigMap) {
	raw, ok := cm.Data[ConfigMapKey]
	if !ok {
		log.Warn().Msg("ConfigMap does not contain ipfiltering.json key, clearing rules")
		f.mu.Lock()
		f.rules = make(map[string]*branchRules)
		f.mu.Unlock()
		return
	}

	var data map[string]IPFilteringConfig
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		log.Error().Err(err).Msg("parse ConfigMap ipfiltering.json")
		return
	}

	f.updateRules(data)
	log.Info().Int("branches", len(data)).Msg("loaded IP filtering rules from ConfigMap")
}

// updateRules updates the internal rules map from parsed ConfigMap data
func (f *Filter) updateRules(data map[string]IPFilteringConfig) {
	newRules := make(map[string]*branchRules)

	for branchID, config := range data {
		if !config.Enabled {
			continue
		}

		prefixes := make([]netip.Prefix, 0, len(config.Allowed))
		for _, cidr := range config.Allowed {
			var prefix netip.Prefix
			var err error

			prefix, err = netip.ParsePrefix(cidr)
			if err != nil {
				addr, addrErr := netip.ParseAddr(cidr)
				if addrErr != nil {
					log.Warn().Err(err).Str("branchID", branchID).Str("cidr", cidr).Msg("failed to parse CIDR or IP address")
					continue
				}
				prefix = netip.PrefixFrom(addr, addr.BitLen())
			}
			prefixes = append(prefixes, prefix)
		}

		newRules[branchID] = &branchRules{
			enabled:  true,
			prefixes: prefixes,
		}
	}

	f.mu.Lock()
	f.rules = newRules
	f.mu.Unlock()
}

// IsAllowed checks if the given client address is allowed for the specified branch.
func (f *Filter) IsAllowed(branchID string, clientAddr string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	rules, exists := f.rules[branchID]
	if !exists || !rules.enabled {
		return true
	}

	ipStr, _, err := net.SplitHostPort(clientAddr)
	if err != nil {
		ipStr = clientAddr
	}

	ip, err := netip.ParseAddr(ipStr)
	if err != nil {
		log.Error().Err(err).Str("ip", ipStr).Str("branchID", branchID).Msg("failed to parse client IP")
		return false
	}

	for _, prefix := range rules.prefixes {
		if prefix.Contains(ip) {
			return true
		}
	}

	return false
}
