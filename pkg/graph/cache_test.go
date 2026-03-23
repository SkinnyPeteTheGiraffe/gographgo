package graph

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestInMemoryCacheGetSetCompatibility(t *testing.T) {
	cache := NewInMemoryCache()
	ttl := 5
	key := CacheKey{NS: []string{"workflow", "node"}, Key: "k1", TTL: &ttl}

	if err := cache.Set(context.Background(), key, "value-1"); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, ok, err := cache.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok || got != "value-1" {
		t.Fatalf("get = (%v, %v), want (value-1, true)", got, ok)
	}
}

func TestInMemoryCacheSetManyGetManyAndClear(t *testing.T) {
	cache := NewInMemoryCache()
	short := 30 * time.Millisecond
	long := 5 * time.Second

	err := cache.SetMany(context.Background(), []CacheSetPair{
		{Key: CacheLookupKey{Namespace: []string{"a", "x"}, Key: "one"}, Value: 1, TTL: &long},
		{Key: CacheLookupKey{Namespace: []string{"a", "x"}, Key: "two"}, Value: 2, TTL: &short},
		{Key: CacheLookupKey{Namespace: []string{"a", "y"}, Key: "three"}, Value: 3},
	})
	if err != nil {
		t.Fatalf("set many: %v", err)
	}

	hits, err := cache.GetMany(context.Background(), []CacheLookupKey{
		{Namespace: []string{"a", "x"}, Key: "one"},
		{Namespace: []string{"a", "x"}, Key: "two"},
		{Namespace: []string{"a", "y"}, Key: "three"},
	})
	if err != nil {
		t.Fatalf("get many initial: %v", err)
	}
	if len(hits) != 3 {
		t.Fatalf("len(hits) = %d, want 3", len(hits))
	}

	time.Sleep(40 * time.Millisecond)
	hits, err = cache.GetMany(context.Background(), []CacheLookupKey{
		{Namespace: []string{"a", "x"}, Key: "one"},
		{Namespace: []string{"a", "x"}, Key: "two"},
		{Namespace: []string{"a", "y"}, Key: "three"},
	})
	if err != nil {
		t.Fatalf("get many after ttl: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("len(hits) after TTL = %d, want 2", len(hits))
	}

	if err := cache.Clear(context.Background(), [][]string{{"a", "x"}}); err != nil {
		t.Fatalf("clear namespace: %v", err)
	}
	hits, err = cache.GetMany(context.Background(), []CacheLookupKey{
		{Namespace: []string{"a", "x"}, Key: "one"},
		{Namespace: []string{"a", "y"}, Key: "three"},
	})
	if err != nil {
		t.Fatalf("get many after namespace clear: %v", err)
	}
	if len(hits) != 1 || !slices.Equal(hits[0].Key.Namespace, []string{"a", "y"}) || hits[0].Key.Key != "three" {
		t.Fatalf("remaining hits = %+v, want only a/y three", hits)
	}

	if err := cache.Clear(context.Background(), nil); err != nil {
		t.Fatalf("clear all: %v", err)
	}
	hits, err = cache.GetMany(context.Background(), []CacheLookupKey{{Namespace: []string{"a", "y"}, Key: "three"}})
	if err != nil {
		t.Fatalf("get many after clear all: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("len(hits) after clear all = %d, want 0", len(hits))
	}
}

func TestInMemoryCacheContextCancellation(t *testing.T) {
	cache := NewInMemoryCache()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := cache.GetMany(ctx, []CacheLookupKey{{Namespace: []string{"n"}, Key: "k"}}); err == nil {
		t.Fatal("expected get many to fail on canceled context")
	}
	if err := cache.SetMany(ctx, []CacheSetPair{{Key: CacheLookupKey{Namespace: []string{"n"}, Key: "k"}, Value: "v"}}); err == nil {
		t.Fatal("expected set many to fail on canceled context")
	}
	if err := cache.Clear(ctx, nil); err == nil {
		t.Fatal("expected clear to fail on canceled context")
	}
}
