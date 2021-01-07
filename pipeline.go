package ytrebuilder

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//HashAndID relate hash and node ID of rebuild shard
type HashAndID struct {
	ShardID int64    `json:"s"`
	Hashs   [][]byte `json:"h"`
	NodeIDs []int32  `json:"n"`
}

//Pipeline create task cache
func (rebuilder *Rebuilder) Pipeline(ctx context.Context, minerID int32) {
	entry := log.WithFields(log.Fields{Function: "Pipeline", MinerID: minerID})
	entry.Info("create task cache")
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	for {
		miner := new(RebuildMiner)
		err := collectionRM.FindOne(ctx, bson.M{"_id": minerID}).Decode(miner)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				entry.WithError(err).Warnf("miner not found")
				return
			}
			entry.WithError(err).Error("decoding rebuild miner failed")
			time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableMinerInterval) * time.Second)
			continue
		}
		if miner.FinishBuild {
			entry.Warn("all caches created")
			return
		}
		err = rebuilder.createCache(ctx, miner)
		if err != nil {
			entry.WithError(err).Error("create cache")
			time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableMinerInterval) * time.Second)
		} else {
			entry.Infof("create cache file: %s", fmt.Sprintf("%d_%d.srd", miner.ID, miner.Next))
		}
	}
}

//createCache create cache
func (rebuilder *Rebuilder) createCache(ctx context.Context, miner *RebuildMiner) error {
	entry := log.WithFields(log.Fields{Function: "createCache", MinerID: miner.ID})
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	collectionRU := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(UnrebuildShardTab)
	collectionAS := rebuilder.analysisdbClient.Database(MetaDB).Collection(Shards)
	collectionAB := rebuilder.analysisdbClient.Database(MetaDB).Collection(Blocks)
	cachepath := filepath.Join(rebuilder.Params.TaskCacheLocation, fmt.Sprintf("%d_%d.srd", miner.ID, miner.Next))
	err := os.Remove(cachepath)
	if err != nil {
		if os.IsExist(err) {
			entry.WithError(err).Errorf("create shards cache file failed: %s", cachepath)
			return err
		}
	}
	cache2path := filepath.Join(rebuilder.Params.TaskCacheLocation, fmt.Sprintf("%d_%d.ext", miner.ID, miner.Next))
	err = os.Remove(cache2path)
	if err != nil {
		if os.IsExist(err) {
			entry.WithError(err).Errorf("create extend cache file failed: %s", cache2path)
			return err
		}
	}
	cacheFile, err := os.Create(cachepath)
	if err != nil {
		entry.WithError(err).Errorf("create shards cache file failed: %s", cachepath)
		return err
	}
	cacheWriter := gzip.NewWriter(cacheFile)
	defer cacheWriter.Close()
	extFile, err := os.Create(cache2path)
	if err != nil {
		entry.WithError(err).Errorf("create extend cache file failed: %s", cache2path)
		return err
	}
	extWriter := gzip.NewWriter(extFile)
	defer extWriter.Close()
	opts := options.FindOptions{}
	opts.Sort = bson.M{"_id": 1}
	limit := int64(miner.BatchSize)
	opts.Limit = &limit
	curShard, err := collectionAS.Find(ctx, bson.M{"nodeId": miner.ID, "_id": bson.M{"$gte": miner.Next}}, &opts)
	if err != nil {
		entry.WithError(err).Error("fetching shard-rebuilding tasks for caching")
		return err
	}
	defer curShard.Close(ctx)
	shards := make([]interface{}, 0)
	var lastID int64
	var total int32
	var lock sync.Mutex
	var flock sync.Mutex
	var flock2 sync.Mutex
	idx := 0
	wg := sync.WaitGroup{}
	wg.Add(rebuilder.Params.MaxConcurrentTaskBuilderSize)
	for curShard.Next(ctx) {
		shard := new(Shard)
		err := curShard.Decode(shard)
		if err != nil {
			entry.WithError(err).Error("decoding shard")
			return err
		}
		block := new(Block)
		err = collectionAB.FindOne(ctx, bson.M{"_id": shard.BlockID}).Decode(block)
		if err != nil {
			entry.WithField(ShardID, shard.ID).WithError(err).Error("decoding block")
			return err
		}
		rshard := new(RebuildShard)
		rshard.ID = shard.ID
		rshard.BlockID = shard.BlockID
		rshard.MinerID = shard.NodeID
		rshard.VHF = shard.VHF.Data
		rshard.VNF = block.VNF
		rshard.SNID = block.SNID
		rshard.ErrCount = 0
		if block.AR == -2 {
			rshard.Type = 0xc258
			rshard.ParityShardCount = block.VNF
		} else if block.AR > 0 {
			rshard.Type = 0x68b3
			rshard.ParityShardCount = block.VNF - block.AR
		}
		idx++
		go func() {
			defer wg.Done()
			drop := false
			opts := options.FindOptions{}
			opts.Sort = bson.M{"_id": 1}
			scur, err := collectionAS.Find(ctx, bson.M{"_id": bson.M{"$gte": rshard.BlockID, "$lt": rshard.BlockID + int64(rshard.VNF)}}, &opts)
			if err != nil {
				entry.WithField(ShardID, rshard.ID).WithError(err).Error("fetching sibling shards failed")
			} else {
				hashs := make([][]byte, 0)
				nodeIDs := make([]int32, 0)
				i := rshard.BlockID
				for scur.Next(ctx) {
					s := new(Shard)
					err := scur.Decode(s)
					if err != nil {
						entry.WithField(ShardID, shard.ID).WithError(err).Errorf("decoding sibling shard %d", i)
						drop = true
						break
					}
					entry.WithField(ShardID, shard.ID).Tracef("decode sibling shard info %d", i)
					if s.ID != i {
						entry.WithField(ShardID, shard.ID).WithError(err).Errorf("sibling shard %d not found: %d", i, s.ID)
						drop = true
						break
					}
					hashs = append(hashs, s.VHF.Data)
					nodeIDs = append(nodeIDs, s.NodeID)
					i++
				}
				if len(hashs) == int(rshard.VNF) {
					haid := &HashAndID{ShardID: rshard.ID, Hashs: hashs, NodeIDs: nodeIDs}
					jstr, err := json.Marshal(haid)
					if err != nil {
						entry.WithError(err).Error("marshalling json of hashs and node IDs")
						drop = true
					} else {
						flock.Lock()
						_, err := fmt.Fprintln(extWriter, string(jstr))
						flock.Unlock()
						if err != nil {
							entry.WithError(err).Error("writing json of hashs and node IDs to file")
							drop = true
						}
					}
				} else {
					drop = true
				}
			}

			if drop {
				entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Warn("rebuilding task create failed: sibling shards lost")
				collectionRU.InsertOne(ctx, rshard)
			} else {
				lock.Lock()
				shards = append(shards, rshard)
				atomic.AddInt32(&total, 1)
				lock.Unlock()
			}
		}()
		if idx%rebuilder.Params.MaxConcurrentTaskBuilderSize == 0 {
			wg.Wait()
			wg = sync.WaitGroup{}
			wg.Add(rebuilder.Params.MaxConcurrentTaskBuilderSize)
			idx = 0
			if len(shards) > 0 {
				sort.Slice(shards, func(i, j int) bool {
					return shards[i].(*RebuildShard).ID < shards[j].(*RebuildShard).ID
				})
				lastID = shards[len(shards)-1].(*RebuildShard).ID
				flock2.Lock()
				for _, item := range shards {
					srd, _ := item.(*RebuildShard)
					jstr, err := json.Marshal(srd)
					if err != nil {
						entry.WithError(err).Error("marshalling json of rebuild shard")
						flock2.Unlock()
						return err
					}
					_, err = fmt.Fprintln(cacheWriter, string(jstr))
					if err != nil {
						entry.WithError(err).Error("writing json of rebuild shard to file")
						flock2.Unlock()
						return err
					}
				}
				flock2.Unlock()
			}
			shards = make([]interface{}, 0)
		}
	}
	for j := 0; j < rebuilder.Params.MaxConcurrentTaskBuilderSize-idx; j++ {
		wg.Done()
	}
	if idx != 0 {
		wg.Wait()
		if len(shards) > 0 {
			sort.Slice(shards, func(i, j int) bool {
				return shards[i].(*RebuildShard).ID < shards[j].(*RebuildShard).ID
			})
			lastID = shards[len(shards)-1].(*RebuildShard).ID
			flock2.Lock()
			for _, item := range shards {
				srd, _ := item.(*RebuildShard)
				jstr, err := json.Marshal(srd)
				if err != nil {
					entry.WithError(err).Error("marshalling json of rebuild shard")
					flock2.Unlock()
					return err
				}
				_, err = fmt.Fprintln(cacheWriter, string(jstr))
				if err != nil {
					entry.WithError(err).Error("writing json of rebuild shard to file")
					flock2.Unlock()
					return err
				}
			}
			flock2.Unlock()
		}
	}
	if total == 0 {
		_, err := collectionRM.UpdateOne(ctx, bson.M{"_id": miner.ID}, bson.M{"$set": bson.M{"finishBuild": true}})
		if err != nil {
			entry.WithError(err).Error("change status of finishBuild")
			return err
		}
		err = os.Remove(cachepath)
		if err != nil {
			entry.WithError(err).Errorf("remove shards cache file failed: %s", cachepath)
			return err
		}
		err = os.Remove(cache2path)
		if err != nil {
			entry.WithError(err).Errorf("remove extend cache file failed: %s", cache2path)
			return err
		}
		entry.Info("cache creation finished")
	} else {
		_, err := collectionRM.UpdateOne(ctx, bson.M{"_id": miner.ID}, bson.M{"$set": bson.M{"next": lastID + 1}})
		if err != nil {
			entry.WithError(err).Error("change value of next")
			return err
		}
		entry.Infof("change value of next to %d", lastID+1)
	}
	return nil
}
