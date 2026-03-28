package graph

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

type unsupportedStoreOp struct{}

func (unsupportedStoreOp) isStoreOp() {}

func TestInMemoryStoreBatchOperations(t *testing.T) {
	store := NewInMemoryStore()
	refreshFalse := false
	depth := 2

	results, err := store.Batch(context.Background(), []StoreOp{
		StorePutOp{Namespace: []string{"users", "u1", "prefs"}, Key: "theme", Value: map[string]any{"name": "dark", "priority": 2}},
		StorePutOp{Namespace: []string{"users", "u2", "prefs"}, Key: "theme", Value: map[string]any{"name": "light", "priority": 1}},
		StoreGetOp{Namespace: []string{"users", "u1", "prefs"}, Key: "theme", RefreshTTL: &refreshFalse},
		StoreSearchOp{NamespacePrefix: []string{"users"}, Filter: map[string]any{"priority": map[string]any{"$gte": 1}}, Limit: 10},
		StoreListNamespacesOp{MatchConditions: []NamespaceMatchCondition{{MatchType: NamespaceMatchPrefix, Path: []string{"users", "*"}}}, MaxDepth: &depth, Limit: 10},
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("len(results) = %d, want 5", len(results))
	}

	item, ok := results[2].(*StoreItem)
	if !ok || item == nil {
		t.Fatalf("results[2] type = %T, want *StoreItem", results[2])
	}
	if item.Key != "theme" || !slices.Equal(item.Namespace, []string{"users", "u1", "prefs"}) {
		t.Fatalf("unexpected get result: %+v", item)
	}

	search, ok := results[3].([]StoreSearchItem)
	if !ok {
		t.Fatalf("results[3] type = %T, want []StoreSearchItem", results[3])
	}
	if len(search) != 2 {
		t.Fatalf("len(search) = %d, want 2", len(search))
	}

	namespaces, ok := results[4].([][]string)
	if !ok {
		t.Fatalf("results[4] type = %T, want [][]string", results[4])
	}
	if len(namespaces) != 2 {
		t.Fatalf("len(namespaces) = %d, want 2", len(namespaces))
	}
}

func TestInMemoryStoreSearchWithFilterOperatorsAndQuery(t *testing.T) {
	store := NewInMemoryStore()
	ns := []string{"docs", "tech"}

	if err := store.PutItem(context.Background(), ns, "d1", map[string]any{"title": "Go runtime", "score": 9, "meta": map[string]any{"stable": true}}, StorePutOptions{}); err != nil {
		t.Fatalf("put d1: %v", err)
	}
	if err := store.PutItem(context.Background(), ns, "d2", map[string]any{"title": "Java runtime", "score": 4, "meta": map[string]any{"stable": false}}, StorePutOptions{}); err != nil {
		t.Fatalf("put d2: %v", err)
	}

	results, err := store.SearchItems(context.Background(), StoreSearchRequest{
		NamespacePrefix: []string{"docs"},
		Query:           "runtime",
		Filter: map[string]any{
			"score": map[string]any{"$gt": 5},
			"meta":  map[string]any{"stable": true},
		},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("len(results) = %d, want 1", len(results))
	}
	if results[0].Key != "d1" {
		t.Fatalf("results[0].Key = %q, want d1", results[0].Key)
	}
}

func TestInMemoryStoreTTLRefreshBehavior(t *testing.T) {
	store := NewInMemoryStore()
	ns := []string{"ttl", "items"}
	ttl := 50 * time.Millisecond

	if err := store.PutItem(context.Background(), ns, "k", map[string]any{"v": 1}, StorePutOptions{TTL: &ttl}); err != nil {
		t.Fatalf("put: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	refreshTrue := true
	item, err := store.GetItem(context.Background(), ns, "k", StoreGetOptions{RefreshTTL: &refreshTrue})
	if err != nil {
		t.Fatalf("get refresh=true: %v", err)
	}
	if item == nil {
		t.Fatal("expected item to exist after first refresh")
	}

	time.Sleep(30 * time.Millisecond)
	item, err = store.GetItem(context.Background(), ns, "k", StoreGetOptions{})
	if err != nil {
		t.Fatalf("get after refresh extension: %v", err)
	}
	if item == nil {
		t.Fatal("expected item to remain after ttl refresh")
	}

	refreshFalse := false
	if _, err := store.GetItem(context.Background(), ns, "k", StoreGetOptions{RefreshTTL: &refreshFalse}); err != nil {
		t.Fatalf("get refresh=false: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	item, err = store.GetItem(context.Background(), ns, "k", StoreGetOptions{RefreshTTL: &refreshFalse})
	if err != nil {
		t.Fatalf("final get: %v", err)
	}
	if item != nil {
		t.Fatal("expected item to expire when refresh is disabled")
	}
}

func TestInMemoryStoreListNamespacesMatchConditions(t *testing.T) {
	store := NewInMemoryStore()
	inputs := []struct {
		key       string
		namespace []string
	}{
		{key: "a", namespace: []string{"users", "u1", "prefs"}},
		{key: "b", namespace: []string{"users", "u2", "prefs"}},
		{key: "c", namespace: []string{"sessions", "u1", "v1"}},
	}
	for _, in := range inputs {
		if err := store.PutItem(context.Background(), in.namespace, in.key, map[string]any{"ok": true}, StorePutOptions{}); err != nil {
			t.Fatalf("put %v/%s: %v", in.namespace, in.key, err)
		}
	}

	namespaces, err := store.ListNamespaces(context.Background(), StoreNamespaceListRequest{
		MatchConditions: []NamespaceMatchCondition{{MatchType: NamespaceMatchSuffix, Path: []string{"*", "prefs"}}},
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("list namespaces: %v", err)
	}
	if len(namespaces) != 2 {
		t.Fatalf("len(namespaces) = %d, want 2", len(namespaces))
	}
}

func TestInMemoryStoreVectorSearchHonorsIndexConfiguration(t *testing.T) {
	store := NewInMemoryStore()
	ns := []string{"semantic", "memory"}

	if err := store.PutItem(context.Background(), ns, "indexed", map[string]any{"text": "quantum retrieval memory"}, StorePutOptions{Index: map[string]any{"fields": []string{"text"}}}); err != nil {
		t.Fatalf("put indexed: %v", err)
	}
	if err := store.PutItem(context.Background(), ns, "plain", map[string]any{"text": "quantum retrieval memory"}, StorePutOptions{Index: false}); err != nil {
		t.Fatalf("put plain: %v", err)
	}

	results, err := store.SearchItems(context.Background(), StoreSearchRequest{
		NamespacePrefix: ns,
		Query:           "quantum retrieval",
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("len(results) = %d, want 1", len(results))
	}
	if results[0].Key != "indexed" {
		t.Fatalf("results[0].Key = %q, want indexed", results[0].Key)
	}
	if results[0].Score == nil || *results[0].Score <= 0 {
		t.Fatalf("expected positive vector score, got %#v", results[0].Score)
	}
}

func TestInMemoryStoreBatchRejectsUnsupportedOperation(t *testing.T) {
	store := NewInMemoryStore()
	_, err := store.Batch(context.Background(), []StoreOp{unsupportedStoreOp{}})
	if err == nil {
		t.Fatal("expected unsupported operation error")
	}
	var invalid *InvalidUpdateError
	if !errors.As(err, &invalid) {
		t.Fatalf("err type = %T, want *InvalidUpdateError", err)
	}
}

func TestStoreFilterAndIndexPathHelpers(t *testing.T) {
	t.Run("compareFilterValues nested and operators", func(t *testing.T) {
		item := map[string]any{"score": 8, "meta": map[string]any{"tier": "gold"}}
		if !compareFilterValues(item["score"], map[string]any{"$gte": 7}) {
			t.Fatal("expected numeric operator comparison to match")
		}
		if !compareFilterValues(item["meta"], map[string]any{"tier": "gold"}) {
			t.Fatal("expected nested map comparison to match")
		}
		if compareFilterValues(item["score"], map[string]any{"$lt": 3}) {
			t.Fatal("expected mismatch for failing comparison")
		}
	})

	t.Run("resolveStoreIndexFields and path extraction", func(t *testing.T) {
		fields, enabled, err := resolveStoreIndexFields(map[string]any{"fields": []any{"title", " ", "meta.tier"}})
		if err != nil {
			t.Fatalf("resolveStoreIndexFields: %v", err)
		}
		if !enabled {
			t.Fatal("expected indexing to be enabled")
		}
		if !slices.Equal(fields, []string{"title", "meta.tier"}) {
			t.Fatalf("fields = %v, want [title meta.tier]", fields)
		}

		value := map[string]any{
			"items": []any{
				map[string]any{"meta": map[string]any{"tier": "gold"}},
				map[string]any{"meta": map[string]any{"tier": "silver"}},
			},
		}
		tokens, ok := parseStorePath("items[*].meta.tier")
		if !ok {
			t.Fatal("expected valid path tokens")
		}
		out := extractStorePathValues(value, tokens)
		if len(out) != 2 || out[0] != "gold" || out[1] != "silver" {
			t.Fatalf("extractStorePathValues = %#v, want [gold silver]", out)
		}
	})
}
