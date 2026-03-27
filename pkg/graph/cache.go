package graph

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"
)

// InMemoryCache is a process-local Cache implementation for testing and
// single-process deployments.
type InMemoryCache struct {
	entries map[string]map[string]cacheEntry
	mu      sync.RWMutex
}

type cacheEntry struct {
	value   any
	expires *time.Time
}

// NewInMemoryCache creates an empty in-memory cache.
func NewInMemoryCache() *InMemoryCache {
	return &InMemoryCache{entries: make(map[string]map[string]cacheEntry)}
}

// Get returns a cached value and whether it exists.
func (c *InMemoryCache) Get(ctx context.Context, key CacheKey) (any, bool, error) {
	hits, err := c.GetMany(ctx, []CacheLookupKey{{Namespace: key.NS, Key: key.Key}})
	if err != nil {
		return nil, false, err
	}
	if len(hits) == 0 {
		return nil, false, nil
	}
	return hits[0].Value, true, nil
}

// Set stores a cached value.
func (c *InMemoryCache) Set(ctx context.Context, key CacheKey, value any) error {
	var ttl *time.Duration
	if key.TTL != nil && *key.TTL > 0 {
		d := time.Duration(*key.TTL) * time.Second
		ttl = &d
	}
	return c.SetMany(ctx, []CacheSetPair{{
		Key:   CacheLookupKey{Namespace: key.NS, Key: key.Key},
		Value: value,
		TTL:   ttl,
	}})
}

// GetMany returns all found entries for requested keys.
func (c *InMemoryCache) GetMany(ctx context.Context, keys []CacheLookupKey) ([]CacheHit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c == nil || len(keys) == 0 {
		return nil, nil
	}
	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	hits := make([]CacheHit, 0, len(keys))
	for _, lookup := range keys {
		nsKey := flattenCacheNamespace(lookup.Namespace)
		nsEntries, ok := c.entries[nsKey]
		if !ok {
			continue
		}
		entry, ok := nsEntries[lookup.Key]
		if !ok {
			continue
		}
		if entry.expires != nil && !now.Before(*entry.expires) {
			delete(nsEntries, lookup.Key)
			if len(nsEntries) == 0 {
				delete(c.entries, nsKey)
			}
			continue
		}
		hits = append(hits, CacheHit{
			Key:   CacheLookupKey{Namespace: append([]string(nil), lookup.Namespace...), Key: lookup.Key},
			Value: entry.value,
		})
	}
	return hits, nil
}

// SetMany writes multiple entries with optional per-entry TTL.
func (c *InMemoryCache) SetMany(ctx context.Context, pairs []CacheSetPair) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil || len(pairs) == 0 {
		return nil
	}
	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, pair := range pairs {
		nsKey := flattenCacheNamespace(pair.Key.Namespace)
		nsEntries, ok := c.entries[nsKey]
		if !ok {
			nsEntries = make(map[string]cacheEntry)
			c.entries[nsKey] = nsEntries
		}
		entry := cacheEntry{value: pair.Value}
		if pair.TTL != nil && *pair.TTL > 0 {
			expires := now.Add(*pair.TTL)
			entry.expires = &expires
		}
		nsEntries[pair.Key.Key] = entry
	}
	return nil
}

// Clear removes entries under explicit namespaces. If namespaces is nil or
// empty, all cache data is removed.
func (c *InMemoryCache) Clear(ctx context.Context, namespaces [][]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(namespaces) == 0 {
		c.entries = make(map[string]map[string]cacheEntry)
		return nil
	}
	for _, ns := range namespaces {
		delete(c.entries, flattenCacheNamespace(ns))
	}
	return nil
}

func flattenCacheNamespace(namespace []string) string {
	if len(namespace) == 0 {
		return ""
	}
	var b strings.Builder
	for _, part := range namespace {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
		b.WriteByte('|')
	}
	return b.String()
}
