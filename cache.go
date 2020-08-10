package ytrebuilder

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

//Cache instance
type Cache struct {
	Items     map[int64]*CacheItem
	CacheSize uint64
	lock      sync.RWMutex
}

//CacheItem item in cache
type CacheItem struct {
	Hashs   [][]byte
	NodeIDs []int32
}

//NewCache create a new cache instance
func NewCache(cacheSize uint64) *Cache {
	return &Cache{Items: make(map[int64]*CacheItem), CacheSize: cacheSize}
}

//IsFull whether cache is full
func (cache *Cache) IsFull() bool {
	entry := log.WithFields(log.Fields{Function: "IsFull"})
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	entry.Tracef("size of cache: %d", len(cache.Items))
	if uint64(len(cache.Items)) >= cache.CacheSize {
		return true
	}
	return false
}

//Get find cache item by key
func (cache *Cache) Get(key int64) *CacheItem {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	return cache.Items[key]
}

//Put put an item in cache
func (cache *Cache) Put(shardID int64, hashs [][]byte, nodeIDs []int32) bool {
	entry := log.WithFields(log.Fields{Function: "Put"})
	cache.lock.Lock()
	defer cache.lock.Unlock()
	if uint64(len(cache.Items)) >= cache.CacheSize {
		return false
	}
	cache.Items[shardID] = &CacheItem{Hashs: hashs, NodeIDs: nodeIDs}
	entry.Tracef("put item %d, size of cache: %d", shardID, len(cache.Items))
	return true
}

//Delete delete a cached item
func (cache *Cache) Delete(shardID int64) {
	entry := log.WithFields(log.Fields{Function: "Delete"})
	cache.lock.Lock()
	defer cache.lock.Unlock()
	delete(cache.Items, shardID)
	entry.Tracef("deleted item %d, size of cache: %d", shardID, len(cache.Items))
}
