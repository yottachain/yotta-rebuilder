package ytrebuilder

import (
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

//Cache instance
type Cache struct {
	Items     []map[int64]*CacheItem
	CacheSize int64
	locks     []sync.RWMutex
	lockCount int64
	Size      int64
}

//CacheItem item in cache
type CacheItem struct {
	Hashs   [][]byte
	NodeIDs []int32
	// count   int32
}

//NewCache create a new cache instance
func NewCache(cacheSize int64) *Cache {
	lockCount := cacheSize / 100
	if lockCount == 0 {
		lockCount = 1
	}
	locks := make([]sync.RWMutex, 0)
	items := make([]map[int64]*CacheItem, 0)
	for i := int64(0); i < lockCount; i++ {
		locks = append(locks, sync.RWMutex{})
		items = append(items, make(map[int64]*CacheItem))
	}
	return &Cache{Items: items, CacheSize: cacheSize, locks: locks, lockCount: int64(lockCount), Size: 0}
}

//IsFull whether cache is full
func (cache *Cache) IsFull() bool {
	entry := log.WithFields(log.Fields{Function: "IsFull"})
	// cache.lock.RLock()
	// defer cache.lock.RUnlock()
	entry.Tracef("size of cache: %d", atomic.LoadInt64(&cache.Size))
	if int64(atomic.LoadInt64(&cache.Size)) >= cache.CacheSize {
		return true
	}
	return false
}

//Get find cache item by key
func (cache *Cache) Get(key int64) *CacheItem {
	cache.locks[key%cache.lockCount].RLock()
	defer cache.locks[key%cache.lockCount].RUnlock()
	return cache.Items[key%cache.lockCount][key]
}

//Put put an item in cache
func (cache *Cache) Put(shardID int64, hashs [][]byte, nodeIDs []int32) bool {
	entry := log.WithFields(log.Fields{Function: "Put"})
	cache.locks[shardID%cache.lockCount].Lock()
	defer cache.locks[shardID%cache.lockCount].Unlock()
	if int64(atomic.LoadInt64(&cache.Size)) >= cache.CacheSize {
		return false
	}
	// if _, ok := cache.Items[shardID%cache.lockCount][shardID]; ok {
	// 	cache.Items[shardID%cache.lockCount][shardID].count++
	// 	entry.Tracef("increase counter of item %d, size of cache: %d", shardID, atomic.LoadInt64(&cache.Size))
	// 	return true
	// }
	_, added := cache.Items[shardID%cache.lockCount][shardID]
	cache.Items[shardID%cache.lockCount][shardID] = &CacheItem{Hashs: hashs, NodeIDs: nodeIDs} //, count: 1}
	if !added {
		atomic.AddInt64(&cache.Size, 1)
	}
	entry.Tracef("put item %d, size of cache: %d", shardID, cache.Size)
	return true
}

//Delete delete a cached item
func (cache *Cache) Delete(shardID int64) {
	entry := log.WithFields(log.Fields{Function: "Delete"})
	cache.locks[shardID%cache.lockCount].Lock()
	defer cache.locks[shardID%cache.lockCount].Unlock()
	_, deleted := cache.Items[shardID%cache.lockCount][shardID]
	delete(cache.Items[shardID%cache.lockCount], shardID)
	if deleted {
		atomic.AddInt64(&cache.Size, -1)
	}
	entry.Tracef("deleted item %d, size of cache: %d", shardID, cache.Size)
	// if _, ok := cache.Items[shardID%cache.lockCount][shardID]; ok {
	// 	if cache.Items[shardID%cache.lockCount][shardID].count > 1 {
	// 		cache.Items[shardID%cache.lockCount][shardID].count--
	// 		entry.Tracef("decrease counter of item %d, size of cache: %d", shardID, len(cache.Items))
	// 	} else {
	// 		delete(cache.Items[shardID%cache.lockCount], shardID)
	// 		atomic.AddInt64(&cache.Size, -1)
	// 		entry.Tracef("deleted item %d, size of cache: %d", shardID, atomic.LoadInt64(&cache.Size))
	// 	}
	// }
}
