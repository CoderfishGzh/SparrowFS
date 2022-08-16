package utils

import (
	"SparrowFS/storage"
	"fmt"
	"log"
	"sync"
)

// A Getter loads data for a key
type Getter interface {
	Get(key []byte) (storage.Entry, error)
}

// A GetterFunc implements Getter with a function
type GetterFunc func(key []byte) (storage.Entry, error)

// Get implements Getter interface function
func (f GetterFunc) Get(key []byte) (storage.Entry, error) {
	return f(key)
}

type Group struct {
	name      string
	getter    Getter
	mainCache cache
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}

	mu.Lock()
	defer mu.Unlock()

	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
	}

	groups[name] = g

	return g
}

// GetGroup returns the named group previously created with NewGroup, or
// nil if there's no such group.
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// Get value for a key from cache
func (g *Group) Get(key []byte) (storage.Entry, error) {
	if string(key) == "" {
		return storage.Entry{}, fmt.Errorf("key is empty")
	}

	// if exit in the cache, return it
	if v, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] hit")
		// create entry
		entry := storage.NewEntry(key, v.b, storage.PUT)
		return *entry, nil
	}

	// if not exit, get from the file
	return g.load(key)
}

func (g *Group) load(key []byte) (storage.Entry, error) {
	return g.getLocally(key)
}

func (g *Group) getLocally(key []byte) (storage.Entry, error) {
	entry, err := g.getter.Get(key)
	if err != nil {
		return storage.Entry{}, err
	}

	// put into cache
	g.populateCache(entry)
	return entry, nil
}

func (g *Group) populateCache(entry storage.Entry) {
	g.mainCache.add(entry.Key, ByteView{
		b: entry.Value,
	})
}
