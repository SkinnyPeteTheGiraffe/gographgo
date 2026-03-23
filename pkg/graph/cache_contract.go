package graph

import (
	"context"
	"time"
)

// CacheLookupKey identifies one cache entry by namespace and key.
type CacheLookupKey struct {
	Namespace []string
	Key       string
}

// CacheHit is one retrieved cache entry.
type CacheHit struct {
	Key   CacheLookupKey
	Value any
}

// CacheSetPair is one cache value write with an optional TTL.
type CacheSetPair struct {
	Key   CacheLookupKey
	Value any
	TTL   *time.Duration
}

// BatchCache defines cache capabilities used by runtime and standalone callers.
type BatchCache interface {
	GetMany(ctx context.Context, keys []CacheLookupKey) ([]CacheHit, error)
	SetMany(ctx context.Context, pairs []CacheSetPair) error
	Clear(ctx context.Context, namespaces [][]string) error
}
