package ytrebuilder

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

//RingCache cache of rebuild tasks
type RingCache struct {
	cache       []*RebuildShard
	index       int
	size        int
	batchSize   int
	expiredTime int
	// retryCount  int
	lock sync.RWMutex
}

//NewRingCache create a new cache instance
func NewRingCache(tasks []*RebuildShard, batchSize, expiredTime int) *RingCache {
	size := len(tasks)
	if size == 0 {
		return nil
	}
	index := 0
	for i := 0; i < size-1; i++ {
		if tasks[i].Timestamp > tasks[i+1].Timestamp {
			index = i + 1
			break
		}
	}
	return &RingCache{cache: tasks, index: index, size: size, batchSize: batchSize, expiredTime: expiredTime}
}

//Empty whether cache is empty
func (cache *RingCache) Empty() bool {
	return cache.size == 0
}

//Allocate allocate rebuild tasks
func (cache *RingCache) Allocate(count int) []*RebuildShard {
	entry := log.WithFields(log.Fields{Function: "Allocate"})
	cache.lock.Lock()
	defer cache.lock.Unlock()
	if cache.size == 0 {
		entry.Debug("size of ringcache is 0")
		return nil
	}
	if cache.index == cache.size {
		entry.Debug("no shards can be allocated")
		return nil
	}
	// task := cache.cache[cache.index]
	now := time.Now().UnixNano()
	// if now-int64(cache.expiredTime*1000000000) < task.Timestamp {
	// 	entry.Debugf("first item is expired: %d", task.Timestamp)
	// 	return nil
	// }
	tasks := make([]*RebuildShard, 0)
	from := cache.index
	if count == 0 {
		count = cache.batchSize
	}
	for i := 0; i < count; i++ {
		task := cache.cache[cache.index]
		// if now-int64(cache.expiredTime*1000000000) < task.Timestamp {
		// 	entry.Debugf("item %d is expired: %d", task.ID, cache.expiredTime)
		// 	break
		// }
		task.Timestamp = now
		//task.ErrCount++
		//shard := *task
		tasks = append(tasks, task)
		cache.index += 1
		if cache.index == cache.size {
			break
		}
	}
	to := cache.index
	entry.Debugf("fetch items from %d to %d, total: %d", from, to, cache.size)
	return tasks
}

// //TagOne tag one task
// func (cache *RingCache) TagOne(id int64, ret int32) *RebuildShard {
// 	cache.lock.Lock()
// 	defer cache.lock.Unlock()
// 	return cache.tag(id, ret)
// }

// //TagMulti tag multi tasks
// func (cache *RingCache) TagMulti(id []int64, ret []int32) ([]*RebuildShard, error) {
// 	cache.lock.Lock()
// 	defer cache.lock.Unlock()
// 	if len(id) != len(ret) {
// 		return nil, errors.New("length of id and ret are not equal")
// 	}
// 	results := make([]*RebuildShard, 0)
// 	for i := 0; i < len(id); i++ {
// 		s := cache.tag(id[i], ret[i])
// 		results = append(results, s)
// 	}
// 	return results, nil
// }

// func (cache *RingCache) tag(id int64, ret int32) *RebuildShard {
// 	entry := log.WithFields(log.Fields{Function: "tag", ShardID: id})
// 	if cache.size == 0 {
// 		entry.Debug("size of ringcache is 0")
// 		return nil
// 	}
// 	index := sort.Search(cache.size, func(i int) bool { return cache.cache[i].ID >= id })
// 	if index == cache.size || cache.cache[index].ID != id {
// 		entry.Debugf("item with ID %d not found", id)
// 		return nil
// 	}
// 	now := time.Now().UnixNano()
// 	if now-int64(cache.expiredTime*1000000000) > cache.cache[index].Timestamp {
// 		entry.Debugf("item with ID %d is expired: %d", id, cache.cache[index].Timestamp)
// 		return nil
// 	}
// 	tmp := cache.cache[index]
// 	s := *tmp
// 	if ret == 0 {
// 		copy(cache.cache[index:], cache.cache[index+1:])
// 		cache.cache = cache.cache[0 : cache.size-1]
// 		if index < cache.index {
// 			cache.index = cache.index - 1
// 		}
// 		cache.size = cache.size - 1
// 		if cache.size > 0 {
// 			cache.index = cache.index % cache.size
// 		}
// 		entry.Debugf("delete item with ID %d", id)
// 		s.Timestamp = Int64Max
// 		return &s
// 	}
// 	if ret == 1 {
// 		return &s
// 	}
// 	// if ret == 1 {
// 	// 	tmp.ErrCount++
// 	// 	s.ErrCount++
// 	// 	if cache.cache[index].ErrCount >= int32(cache.retryCount) {
// 	// 		copy(cache.cache[index:], cache.cache[index+1:])
// 	// 		cache.cache = cache.cache[0 : cache.size-1]
// 	// 		if index < cache.index {
// 	// 			cache.index = cache.index - 1
// 	// 		}
// 	// 		cache.size = cache.size - 1
// 	// 		if cache.size > 0 {
// 	// 			cache.index = cache.index % cache.size
// 	// 		}
// 	// 		entry.Debugf("increase error count of item with ID %d to %d, delete item", id, tmp.ErrCount)
// 	// 		s.Timestamp = Int64Max
// 	// 		return &s
// 	// 	}
// 	// }
// 	// entry.Debugf("increase error count of item with ID %d to %d", id, tmp.ErrCount)
// 	return nil
// }
