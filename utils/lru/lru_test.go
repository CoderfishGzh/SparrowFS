package lru

import (
	"SparrowFS/storage"
	"testing"
)

func TestGet(t *testing.T) {
	lru := New(int64(0), nil)
	lru.Add([]byte("key"), []byte("value"))

	if entry, ok := lru.Get([]byte("key")); !ok || string(entry.Value) != "value" {
		t.Fatalf("got %v, %v", entry, ok)
	}

	if _, ok := lru.Get([]byte("key2")); ok {
		t.Fatal("should be missing")
	}
}

func TestRemoveOldest(t *testing.T) {
	k1, k2, k3 := []byte("key1"), []byte("key2"), []byte("key3")
	v1, v2, v3 := []byte("value1"), []byte("value2"), []byte("value3")

	size := storage.NewEntry(k1, v1, storage.PUT).GetSize()
	cap := int64(2 * size)

	lru := New(cap, nil)

	lru.Add(k1, v1) // add k1 to cache
	lru.Add(k2, v2) // add k2 to cache
	lru.Add(k3, v3) // add k3 to cache

	if _, ok := lru.Get(k1); ok || lru.Len() != 2 {
		t.Fatalf("remove oldest failed key1")
	}

}


