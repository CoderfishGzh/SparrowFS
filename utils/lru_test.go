package utils

import (
	"SparrowFS/storage"
	"testing"
)

func TestGet(t *testing.T) {
	lru := New(int64(0))
	entry := storage.NewEntry([]byte("key"), []byte("value"), storage.PUT)

	lru.Add(*entry)

	if lru.Len() != 1 {
		t.Errorf("lru len should be 1 after add one entry, but got %d", lru.Len())
	}


	v, ok := lru.Get([]byte("key"));
	if !ok {
		t.Fatalf("cache hite key1 = key faile ")
	} 

	if string(v) != "value" {
		t.Fatalf("cache hit key1 = key faile ")
	}

}

func TestRemoveoldest(t *testing.T) {
	entry1 := storage.NewEntry([]byte("key1"), []byte("value1"), storage.PUT)
	entry2 := storage.NewEntry([]byte("key2"), []byte("value2"), storage.PUT)
	entry3 := storage.NewEntry([]byte("key3"), []byte("value3"), storage.PUT)

	cap := int64(entry1.GetSize()) + int64(entry2.GetSize())

	lru := New(cap)
	lru.Add(*entry1)
	lru.Add(*entry2)
	lru.Add(*entry3)

	_, ok := lru.Get([]byte("key1"))
	if ok == true {
		t.Fatalf("cache hit key1 = key faile ")
	}

	if lru.Len() != 2 {
		t.Fatalf("lru len should be 2 after add one entry, but got %d", lru.Len())
	}
}