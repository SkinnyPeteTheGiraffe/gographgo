package graph

import (
	"fmt"
	"slices"
	"sync"
	"testing"
)

func TestInMemoryStoreCRUD(t *testing.T) {
	store := NewInMemoryStore()
	ns := []string{"assistant", "a1"}

	if err := store.Set(ns, "session", "value-1"); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, ok, err := store.Get(ns, "session")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok || got != "value-1" {
		t.Fatalf("got (%v, %v), want (value-1, true)", got, ok)
	}

	if _, ok, err := store.Get([]string{"assistant", "a2"}, "session"); err != nil {
		t.Fatalf("get other namespace: %v", err)
	} else if ok {
		t.Fatal("expected key to be isolated by namespace")
	}

	if err := store.Delete(ns, "session"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, err := store.Get(ns, "session"); err != nil {
		t.Fatalf("get after delete: %v", err)
	} else if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestInMemoryStoreList(t *testing.T) {
	store := NewInMemoryStore()
	ns := []string{"user", "u1"}
	for _, key := range []string{"mem-2", "mem-1", "other", "mem-10"} {
		if err := store.Set(ns, key, key+"-value"); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}
	if err := store.Set([]string{"user", "u2"}, "mem-0", "isolated"); err != nil {
		t.Fatalf("set in other namespace: %v", err)
	}

	keys, err := store.List(ns, "mem-")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []string{"mem-1", "mem-10", "mem-2"}
	if !slices.Equal(keys, want) {
		t.Fatalf("keys = %v, want %v", keys, want)
	}
}

func TestInMemoryStoreSearch(t *testing.T) {
	store := NewInMemoryStore()
	ns := []string{"thread", "t1"}
	for _, key := range []string{"alpha", "alphabet", "beta", "zeta"} {
		if err := store.Set(ns, key, key+"-value"); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}

	got, err := store.Search(ns, "ALP", 1)
	if err != nil {
		t.Fatalf("search with limit: %v", err)
	}
	want := []string{"alpha"}
	if !slices.Equal(got, want) {
		t.Fatalf("search = %v, want %v", got, want)
	}

	got, err = store.Search(ns, "ta", 0)
	if err != nil {
		t.Fatalf("search without limit: %v", err)
	}
	want = []string{"beta", "zeta"}
	if !slices.Equal(got, want) {
		t.Fatalf("search = %v, want %v", got, want)
	}
}

func TestInMemoryStoreNamespaceEncodingNoCollision(t *testing.T) {
	store := NewInMemoryStore()
	if err := store.Set([]string{"a", "b"}, "k", "v1"); err != nil {
		t.Fatalf("set [a b]: %v", err)
	}
	if err := store.Set([]string{"a:b"}, "k", "v2"); err != nil {
		t.Fatalf("set [a:b]: %v", err)
	}

	v1, ok, err := store.Get([]string{"a", "b"}, "k")
	if err != nil {
		t.Fatalf("get [a b]: %v", err)
	}
	if !ok || v1 != "v1" {
		t.Fatalf("get [a b] = (%v, %v), want (v1, true)", v1, ok)
	}

	v2, ok, err := store.Get([]string{"a:b"}, "k")
	if err != nil {
		t.Fatalf("get [a:b]: %v", err)
	}
	if !ok || v2 != "v2" {
		t.Fatalf("get [a:b] = (%v, %v), want (v2, true)", v2, ok)
	}
}

func TestInMemoryStoreConcurrentAccess(t *testing.T) {
	store := NewInMemoryStore()
	ns := []string{"dev"}

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k-%03d", i)
			if err := store.Set(ns, key, i); err != nil {
				t.Errorf("set %q: %v", key, err)
			}
		}(i)
	}
	wg.Wait()

	keys, err := store.List(ns, "k-")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(keys) != 100 {
		t.Fatalf("len(keys) = %d, want 100", len(keys))
	}
}
