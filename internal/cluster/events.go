package cluster

// CacheEvent describes which cluster path served a cache operation. It is
// optional observability intended for diagnostics and examples, not correctness.
type CacheEvent struct {
	Operation string
	Role      string
	Source    string
	Key       string
	Hit       bool
}
