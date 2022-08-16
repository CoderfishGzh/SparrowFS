package utils

import (
	"SparrowFS/storage"
	"fmt"
	"log"
	"testing"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func TestCacheGet(t *testing.T) {

	loadCounts := make(map[string]int, len(db))

	gee := NewGroup("scores", 10<<20, GetterFunc(
		func(key []byte) (storage.Entry, error) {
			log.Println("[slowDB] search key", key)
			if v, ok := db[string(key)]; ok {
				if _, ok := loadCounts[string(key)]; !ok {
					loadCounts[string(key)] = 0
				}
				loadCounts[string(key)] += 1
				return storage.Entry{Key: []byte(key), Value: []byte(v)}, nil
			}
			return storage.Entry{}, fmt.Errorf("%s not exist", key)

		}))

	for k, v := range db {
		if view, err := gee.Get([]byte(k)); err != nil || string(view.Value) != v {
			t.Fatal("failed to get value from cache")
		}

		if _, err := gee.Get([]byte(k)); err != nil || loadCounts[k] > 1 {
			t.Fatalf("cache %s miss", k)
		}
	}

	if view, err := gee.Get([]byte("unknown")); err == nil {
		t.Fatalf("the value of unknow should be empty, but %s got", view.Value)
	}
}
