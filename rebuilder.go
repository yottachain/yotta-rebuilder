package ytrebuilder

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aurawing/auramq"
	"github.com/aurawing/auramq/msg"
	proto "github.com/golang/protobuf/proto"
	"github.com/ivpusic/grpool"
	rl "github.com/juju/ratelimit"
	log "github.com/sirupsen/logrus"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	pb "github.com/yottachain/yotta-rebuilder/pbrebuilder"
	ytsync "github.com/yottachain/yotta-rebuilder/sync"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Int64Max max value of int64
//const Int64Max int64 = 9223372036854775807

//Rebuilder rebuilder
type Rebuilder struct {
	tikvCli           *rawkv.Client
	rebuilderdbClient *mongo.Client
	NodeManager       *NodeManager
	Cache             map[int32]*SiblingCache
	Compensation      *CompensationConfig
	Params            *MiscConfig
	mqClis            map[int]*ytsync.Service
	ratelimiter       *rl.Bucket
	httpCli           *http.Client
	taskAllocator     map[int32]*RingCache
	lock              sync.RWMutex
	lock2             sync.RWMutex
}

//New create a new rebuilder instance
func New(ctx context.Context, pdURLs []string, rebuilderDBURL string, mqconf *AuraMQConfig, cpsConf *CompensationConfig, conf *MiscConfig) (*Rebuilder, error) {
	entry := log.WithFields(log.Fields{Function: "New"})
	tikvCli, err := rawkv.NewClient(ctx, pdURLs, config.Default())
	if err != nil {
		entry.WithError(err).Errorf("creating tikv client failed: %v", pdURLs)
		return nil, err
	}
	rebuilderdbClient, err := mongo.Connect(ctx, options.Client().ApplyURI(rebuilderDBURL))
	if err != nil {
		entry.WithError(err).Errorf("creating rebuilderDB client failed: %s", rebuilderDBURL)
		return nil, err
	}
	entry.Infof("rebuilderDB client created: %s", rebuilderDBURL)
	nodeMgr, err := NewNodeManager(ctx, rebuilderdbClient)
	if err != nil {
		entry.WithError(err).Error("creating node manager failed")
		return nil, err
	}
	entry.Info("node manager created")
	cache := make(map[int32]*SiblingCache) //NewCache(conf.MaxCacheSize)
	entry.Info("cache created")
	pool := grpool.NewPool(conf.SyncPoolLength, conf.SyncQueueLength)
	taskAllocator := make(map[int32]*RingCache)
	ratelimiter := rl.NewBucketWithRate(float64(conf.FetchTaskRate), int64(conf.FetchTaskRate))
	rebuilder := &Rebuilder{tikvCli: tikvCli, rebuilderdbClient: rebuilderdbClient, NodeManager: nodeMgr, Cache: cache, Compensation: cpsConf, Params: conf, ratelimiter: ratelimiter, httpCli: &http.Client{}, taskAllocator: taskAllocator}
	callback := func(msg *msg.Message) {
		if msg.GetType() == auramq.BROADCAST {
			if msg.GetDestination() == mqconf.MinerSyncTopic {
				pool.JobQueue <- func() {
					nodemsg := new(pb.NodeMsg)
					err := proto.Unmarshal(msg.Content, nodemsg)
					if err != nil {
						entry.WithError(err).Error("decoding nodeMsg failed")
						return
					}
					node := new(Node)
					node.Fillby(nodemsg)
					rebuilder.syncNode(ctx, node)
				}
			}
		}
	}
	m, err := ytsync.StartSync(mqconf.SubscriberBufferSize, mqconf.PingWait, mqconf.ReadWait, mqconf.WriteWait, mqconf.MinerSyncTopic, mqconf.AllSNURLs, callback, mqconf.Account, mqconf.PrivateKey, mqconf.ClientID)
	if err != nil {
		entry.WithError(err).Error("creating mq clients map failed")
		return nil, err
	}
	rebuilder.mqClis = m
	return rebuilder, nil
}

func (rebuilder *Rebuilder) syncNode(ctx context.Context, node *Node) error {
	entry := log.WithFields(log.Fields{Function: "syncNode", MinerID: node.ID})
	if node.ID == 0 {
		return errors.New("node ID cannot be 0")
	}
	collection := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(NodeTab)
	otherDoc := bson.A{}
	if node.Ext != "" && node.Ext[0] == '[' && node.Ext[len(node.Ext)-1] == ']' {
		var bdoc interface{}
		err := bson.UnmarshalExtJSON([]byte(node.Ext), true, &bdoc)
		if err != nil {
			entry.WithError(err).Warn("parse ext document")
		} else {
			otherDoc, _ = bdoc.(bson.A)
		}
	}
	if node.Uspaces == nil {
		node.Uspaces = make(map[string]int64)
	}
	_, err := collection.InsertOne(ctx, bson.M{"_id": node.ID, "nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "poolOwner": node.PoolOwner, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "uspaces": node.Uspaces, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx, "other": otherDoc})
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			entry.WithError(err).Error("inserting node to database")
			return err
		}
		oldNode := new(Node)
		err := collection.FindOne(ctx, bson.M{"_id": node.ID}).Decode(oldNode)
		if err != nil {
			entry.WithError(err).Error("fetching node failed")
			return err
		}
		cond := bson.M{"nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "poolOwner": node.PoolOwner, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx, "other": otherDoc}
		for k, v := range node.Uspaces {
			cond[fmt.Sprintf("uspaces.%s", k)] = v
		}
		if node.Status == 2 && oldNode.Status == 1 {
			cond["tasktimestamp"] = time.Now().Unix()
		}
		opts := new(options.FindOneAndUpdateOptions)
		opts = opts.SetReturnDocument(options.After)
		result := collection.FindOneAndUpdate(ctx, bson.M{"_id": node.ID}, bson.M{"$set": cond}, opts)
		updatedNode := new(Node)
		err = result.Decode(updatedNode)
		if err != nil {
			entry.WithError(err).Error("updating record of node")
			return err
		}
		rebuilder.NodeManager.UpdateNode(updatedNode)
		if updatedNode.Status == 3 && oldNode.Status == 2 {
			//重建完毕，状态改为3，删除旧任务
			collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
			collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
			_, err := collectionRS.DeleteMany(ctx, bson.M{"minerID": updatedNode.ID})
			if err != nil {
				entry.WithError(err).Error("delete old shard-rebuildng tasks")
				return nil
			}
			_, err = collectionRM.UpdateOne(ctx, bson.M{"_id": updatedNode.ID}, bson.M{"$set": bson.M{"from": 0, "to": 0, "status": 3}})
			if err != nil {
				entry.WithError(err).Error("update rebuild miner status to 3")
			} else {
				entry.Info("all rebuild tasks finished")
			}
			rebuilder.lock.Lock()
			delete(rebuilder.taskAllocator, updatedNode.ID)
			rebuilder.lock.Unlock()
			rebuilder.lock2.Lock()
			delete(rebuilder.Cache, updatedNode.ID)
			rebuilder.lock2.Unlock()
			return nil
		}
	} else {
		rebuilder.NodeManager.UpdateNode(node)
	}
	return nil
}

//BinTuple binary tuple
// type BinTuple struct {
// 	A int32
// 	B int64
// }

//Start starting rebuilding process
func (rebuilder *Rebuilder) Start(ctx context.Context) {
	entry := log.WithFields(log.Fields{Function: "Start"})
	rebuilder.Preprocess(ctx)
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	//collectionRU := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(UnrebuildShardTab)
	curMiner, err := collectionRM.Find(ctx, bson.M{"status": 2})
	if err != nil {
		entry.WithError(err).Error("fetching unfinished building miner failed")
	} else {
		wg := sync.WaitGroup{}
		for curMiner.Next(ctx) {
			miner := new(RebuildMiner)
			err := curMiner.Decode(miner)
			if err != nil {
				entry.WithError(err).Error("decoding unfinished building miner failed")
				continue
			}
			if !miner.FinishBuild {
				go rebuilder.Pipeline(ctx, miner.ID)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				shards := make([]*RebuildShard, 0)
				opts := options.FindOptions{}
				opts.Sort = bson.M{"_id": 1}
				curShard, err := collectionRS.Find(ctx, bson.M{"minerID": miner.ID, "timestamp": 0}, &opts)
				if err != nil {
					entry.WithError(err).Error("pre-fetching rebuildable shards failed")
					return
				}
				for curShard.Next(ctx) {
					rshard := new(RebuildShard)
					err := curShard.Decode(rshard)
					if err != nil {
						entry.WithError(err).Error("decoding rebuildable shard failed")
						continue
					}
					shards = append(shards, rshard)
				}
				if len(shards) > 0 {
					entry.Debug("create sibling cache")
					cachepathExt := filepath.Join(rebuilder.Params.TaskCacheLocation, fmt.Sprintf("%d_%d.ext", miner.ID, miner.FileIndex))
					extFile, err := os.Open(cachepathExt)
					if err != nil {
						entry.WithError(err).Errorf("open extend cache file failed: %s", cachepathExt)
						return
					}
					defer extFile.Close()
					extReader, err := gzip.NewReader(extFile)
					if err != nil {
						entry.WithError(err).Errorf("create reader of extend cache file failed: %s", cachepathExt)
						return
					}
					defer extReader.Close()
					extBuf := bufio.NewReader(extReader)
					sibCache := NewSiblingCache()
					for {
						line, err := extBuf.ReadBytes('\n')
						if err != nil {
							if io.EOF == err {
								break
							} else {
								entry.WithError(err).Errorf("read extend cache file error: %s", cachepathExt)
								return
							}
						} else {
							b := strings.TrimSpace(string(line))
							line = []byte(b)
							if string(line) == "" {
								break
							}
							item := new(HashAndID)
							err := json.Unmarshal(line, item)
							if err != nil {
								entry.WithError(err).Errorf("unmarshalling extend item error: %s", string(line))
								return
							}
							sibCache.Put(item.ShardID, item.Hashs, item.NodeIDs)
						}
					}
					rebuilder.lock2.Lock()
					rebuilder.Cache[miner.ID] = sibCache
					rebuilder.lock2.Unlock()
					rcache := NewRingCache(shards, rebuilder.Params.RebuildShardMinerTaskBatchSize, rebuilder.Params.RebuildShardExpiredTime)
					entry.Debug("create ring cache")
					rebuilder.lock.Lock()
					rebuilder.taskAllocator[miner.ID] = rcache
					rebuilder.lock.Unlock()
					entry.Debugf("cache filling finished for miner %d", miner.ID)
				}
			}()
		}
		wg.Wait()
	}
	// opts := options.FindOptions{}
	// opts.Sort = bson.M{"_id": 1}
	// cachetmp := make(map[int32][]*RebuildShard)
	// failedtmp := make(map[int32][]int64)
	// curShard, err := collectionRS.Find(ctx, bson.M{"timestamp": 0}, &opts)
	// if err != nil {
	// 	entry.WithError(err).Error("pre-fetching rebuildable shards failed")
	// } else {
	// 	ch := make(chan *BinTuple, 1000)
	// 	go func() {
	// 		for {
	// 			select {
	// 			case val, ok := <-ch:
	// 				if !ok {
	// 					entry.Debug("finished collecting failed shards")
	// 					return
	// 				}
	// 				if failedtmp[val.A] == nil {
	// 					failedtmp[val.A] = make([]int64, 0)
	// 				}
	// 				failedtmp[val.A] = append(failedtmp[val.A], val.B)
	// 				entry.Debugf("collecting failed shards: %d", val.B)
	// 			}
	// 		}
	// 	}()
	// 	entry.Debugf("ready for filling cache")
	// 	idx := 0
	// 	wg := sync.WaitGroup{}
	// 	wg.Add(rebuilder.Params.MaxConcurrentTaskBuilderSize)
	// 	for curShard.Next(ctx) {
	// 		rshard := new(RebuildShard)
	// 		err := curShard.Decode(rshard)
	// 		if err != nil {
	// 			entry.WithError(err).Error("decoding rebuildable shard failed")
	// 			continue
	// 		}
	// 		idx++
	// 		go func() {
	// 			defer wg.Done()
	// 			drop := false
	// 			if !rebuilder.Cache.IsFull() {
	// 				opts := options.FindOptions{}
	// 				opts.Sort = bson.M{"_id": 1}
	// 				//scur, err := collectionAS.Find(ctx, bson.M{"_id": bson.M{"$gte": rshard.BlockID, "$lt": rshard.BlockID + int64(rshard.VNF)}}, &opts)
	// 				//rows, err := rebuilder.analysisdbClient.QueryxContext(ctx, "select * from shards where bid>=? and bid<? order by id asc", rshard.BlockID, rshard.BlockID+int64(rshard.VNF))
	// 				siblingShards, err := FetchShards(ctx, rebuilder.tikvCli, rshard.BlockID, rshard.BlockID+int64(rshard.VNF))
	// 				if err != nil {
	// 					entry.WithField(MinerID, rshard.MinerID).WithField(ShardID, rshard.ID).WithError(err).Error("fetching sibling shards failed")
	// 				} else {
	// 					//遍历分块内全部分片
	// 					hashs := make([][]byte, 0)
	// 					nodeIDs := make([]int32, 0)
	// 					i := rshard.BlockID
	// 					for _, s := range siblingShards {
	// 						// s := new(Shard)
	// 						// err := rows.StructScan(s)
	// 						// if err != nil {
	// 						// 	entry.WithField(MinerID, rshard.MinerID).WithField(ShardID, s.ID).WithError(err).Errorf("decoding sibling shard %d failed", i)
	// 						// 	drop = true
	// 						// 	break
	// 						// }
	// 						entry.WithField(MinerID, rshard.MinerID).WithField(ShardID, rshard.ID).Tracef("decode sibling shard info %d", i)
	// 						if s.ID != i {
	// 							entry.WithField(MinerID, rshard.MinerID).WithField(ShardID, s.ID).WithError(err).Errorf("sibling shard %d not found: %d", i, s.ID)
	// 							drop = true
	// 							break
	// 						}
	// 						hashs = append(hashs, s.VHF)
	// 						nodeIDs = append(nodeIDs, s.NodeID)
	// 						i++
	// 					}
	// 					//scur.Close(ctx)
	// 					// if err = rows.Close(); err != nil {
	// 					// 	entry.WithField(MinerID, rshard.MinerID).WithField(ShardID, rshard.ID).WithError(err).Error("close result set")
	// 					// }
	// 					if len(hashs) == int(rshard.VNF) {
	// 						rebuilder.Cache.Put(rshard.ID, hashs, nodeIDs)
	// 					}
	// 				}
	// 			}
	// 			if drop {
	// 				entry.WithField(MinerID, rshard.MinerID).WithField(ShardID, rshard.ID).Warn("rebuilding task create failed: sibling shards lost")
	// 				collectionRU.InsertOne(ctx, rshard)
	// 				r, err := collectionRS.UpdateOne(ctx, bson.M{"_id": rshard.ID}, bson.M{"$set": bson.M{"timestamp": Int64Max}})
	// 				if err != nil {
	// 					entry.WithError(err).WithField(ShardID, rshard.ID).Errorf("update timestamp to %d", Int64Max)
	// 				} else {
	// 					if r.ModifiedCount == 1 {
	// 						entry.WithField(ShardID, rshard.ID).Debugf("update timestamp to %d", Int64Max)
	// 					} else {
	// 						entry.WithField(ShardID, rshard.ID).Debug("no matched record for updating")
	// 					}
	// 				}
	// 				ch <- &BinTuple{A: rshard.MinerID, B: rshard.ID}
	// 			}
	// 		}()
	// 		if idx%rebuilder.Params.MaxConcurrentTaskBuilderSize == 0 {
	// 			wg.Wait()
	// 			wg = sync.WaitGroup{}
	// 			wg.Add(rebuilder.Params.MaxConcurrentTaskBuilderSize)
	// 			idx = 0
	// 		}
	// 		if cachetmp[rshard.MinerID] == nil {
	// 			cachetmp[rshard.MinerID] = make([]*RebuildShard, 0)
	// 		}
	// 		cachetmp[rshard.MinerID] = append(cachetmp[rshard.MinerID], rshard)
	// 		entry.Debugf("add shard to ring cache: %d", rshard.ID)
	// 	}
	// 	for j := 0; j < rebuilder.Params.MaxConcurrentTaskBuilderSize-idx; j++ {
	// 		wg.Done()
	// 	}
	// 	if idx != 0 {
	// 		wg.Wait()
	// 	}
	// 	curShard.Close(ctx)
	// 	close(ch)
	// 	rebuilder.lock.Lock()
	// 	defer rebuilder.lock.Unlock()
	// 	for k, v := range cachetmp {
	// 		rcache := NewRingCache(v, rebuilder.Params.RebuildShardMinerTaskBatchSize, rebuilder.Params.RebuildShardExpiredTime)
	// 		entry.Debugf("create ring cache of miner %d", k)
	// 		if len(failedtmp[k]) > 0 {
	// 			fids := failedtmp[k]
	// 			tags := make([]int32, len(fids))
	// 			for i := 0; i < len(fids); i++ {
	// 				tags[i] = 0
	// 			}
	// 			rcache.TagMulti(fids, tags)
	// 			entry.Debugf("delete % failed shards of miner %d", len(fids), k)
	// 		}
	// 		rebuilder.taskAllocator[k] = rcache
	// 	}
	// 	entry.Debugf("cache filling finished")
	// }

	go rebuilder.Compensate(ctx)
	go rebuilder.processRebuildableMiner(ctx)
	go rebuilder.processRebuildableShard(ctx)
	go rebuilder.reaper(ctx)
}

func (rebuilder *Rebuilder) processRebuildableMiner(ctx context.Context) {
	entry := log.WithFields(log.Fields{Function: "processRebuildableMiner"})
	collection := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(NodeTab)
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	entry.Info("starting rebuildable node processor")
	for {
		cur, err := collection.Find(ctx, bson.M{"status": 2, "tasktimestamp": bson.M{"$exists": true, "$lt": time.Now().Unix() - int64(rebuilder.Params.RebuildableMinerTimeGap)}})
		if err != nil {
			entry.WithError(err).Error("cannot fetch rebuildable nodes")
			time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableMinerInterval) * time.Second)
			continue
		}
		for cur.Next(ctx) {
			cnt, err := collectionRM.CountDocuments(ctx, bson.M{"status": bson.M{"$lt": 3}})
			if err != nil {
				entry.WithError(err).Error("cannot calculate count of rebuilding nodes")
				break
			}
			if cnt >= int64(rebuilder.Params.RebuildingMinerCountPerBatch) {
				entry.Debug("reaching max count of rebuilding miners")
				break
			}
			node := new(Node)
			err = cur.Decode(node)
			if err != nil {
				entry.WithError(err).Error("decoding node")
				continue
			}

			rangeFrom := int64(0)
			//rangeTo := int64(0)
			shardFrom, err := FetchFirstNodeShard(ctx, rebuilder.tikvCli, node.ID)
			if err != nil {
				if err != NoValError {
					entry.WithField(MinerID, node.ID).WithError(err).Error("finding starting shard")
					continue
				}
			} else {
				rangeFrom = shardFrom.ID
			}
			// shardTo, err := FetchLastNodeShard(ctx, rebuilder.tikvCli, node.ID)
			// if err != nil {
			// 	if err != NoValError {
			// 		entry.WithField(MinerID, node.ID).WithError(err).Error("finding ending shard")
			// 		continue
			// 	}
			// } else {
			// 	rangeTo = shardTo.ID
			// }

			miner := new(RebuildMiner)
			miner.ID = node.ID
			miner.RangeFrom = rangeFrom
			//miner.RangeTo = rangeTo
			miner.Status = node.Status
			miner.Timestamp = time.Now().Unix()
			miner.BatchSize = int64(rebuilder.Params.RebuildShardTaskBatchSize)
			miner.Next = rangeFrom
			miner.FinishBuild = false
			_, err = collectionRM.InsertOne(ctx, miner)
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Error("insert rebuild miner to database")
			}
			_, err = collection.UpdateOne(ctx, bson.M{"_id": node.ID}, bson.M{"$unset": bson.M{"tasktimestamp": ""}})
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Error("unset tasktimestamp of rebuild miner")
			}
			go rebuilder.Pipeline(ctx, miner.ID)
			entry.WithField(MinerID, miner.ID).Info("ready for rebuilding")
		}
		cur.Close(ctx)
		time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableMinerInterval) * time.Second)
	}
}

func (rebuilder *Rebuilder) readCacheFile(ctx context.Context, miner *RebuildMiner, fileIndex int64) (int64, error) {
	entry := log.WithFields(log.Fields{Function: "readCacheFile", MinerID: miner.ID})
	cachepathSrd := filepath.Join(rebuilder.Params.TaskCacheLocation, fmt.Sprintf("%d_%d.srd", miner.ID, fileIndex))
	cachepathExt := filepath.Join(rebuilder.Params.TaskCacheLocation, fmt.Sprintf("%d_%d.ext", miner.ID, fileIndex))
	cacheFile, err := os.Open(cachepathSrd)
	if err != nil {
		entry.WithError(err).Errorf("open shards cache file failed: %s", cachepathSrd)
		return 0, err
	}
	defer cacheFile.Close()
	cacheReader, err := gzip.NewReader(cacheFile)
	if err != nil {
		entry.WithError(err).Errorf("create reader of shards cache file failed: %s", cachepathSrd)
		return 0, err
	}
	defer cacheReader.Close()
	extFile, err := os.Open(cachepathExt)
	if err != nil {
		entry.WithError(err).Errorf("open extend cache file failed: %s", cachepathExt)
		return 0, err
	}
	defer extFile.Close()
	extReader, err := gzip.NewReader(extFile)
	if err != nil {
		entry.WithError(err).Errorf("create reader of extend cache file failed: %s", cachepathExt)
		return 0, err
	}
	defer extReader.Close()
	cacheBuf := bufio.NewReader(cacheReader)
	extBuf := bufio.NewReader(extReader)
	sibCache := NewSiblingCache()
	for {
		line, err := extBuf.ReadBytes('\n')
		if err != nil {
			if io.EOF == err {
				break
			} else {
				entry.WithError(err).Errorf("read extend cache file error: %s", cachepathExt)
				return 0, err
			}
		} else {
			b := strings.TrimSpace(string(line))
			line = []byte(b)
			if string(line) == "" {
				break
			}
			item := new(HashAndID)
			err := json.Unmarshal(line, item)
			if err != nil {
				entry.WithError(err).Errorf("unmarshalling extend item error: %s", string(line))
				return 0, err
			}
			sibCache.Put(item.ShardID, item.Hashs, item.NodeIDs)
			// if !rebuilder.Cache.IsFull() {
			// 	rebuilder.Cache.Put(item.ShardID, item.Hashs, item.NodeIDs)
			// }
		}
	}
	shards := make([]interface{}, 0)
	shards2 := make([]*RebuildShard, 0)
	for {
		line, err := cacheBuf.ReadBytes('\n')
		if err != nil {
			if io.EOF == err {
				break
			} else {
				entry.WithError(err).Errorf("read shards cache file error: %s", cachepathExt)
				return 0, err
			}
		} else {
			b := strings.TrimSpace(string(line))
			line = []byte(b)
			if string(line) == "" {
				break
			}
			item := new(RebuildShard)
			err := json.Unmarshal(line, item)
			if err != nil {
				entry.WithError(err).Errorf("unmarshalling shard item error: %s", string(line))
				return 0, err
			}
			shards = append(shards, item)
			shards2 = append(shards2, item)
		}
	}
	rcache := NewRingCache(shards2, rebuilder.Params.RebuildShardMinerTaskBatchSize, rebuilder.Params.RebuildShardExpiredTime)
	entry.Debug("create ring cache")
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	_, err = collectionRS.InsertMany(ctx, shards, new(options.InsertManyOptions).SetOrdered(false))
	if err != nil {
		entry.WithError(err).Error("insert shard-rebuilding tasks")
		return 0, err
	}
	From := shards[0].(*RebuildShard).ID
	To := shards[len(shards)-1].(*RebuildShard).ID
	fi := miner.To + 1
	if miner.To == 0 {
		fi = miner.RangeFrom
	}
	_, err = collectionRM.UpdateOne(ctx, bson.M{"_id": miner.ID}, bson.M{"$set": bson.M{"from": From, "to": To, "fileIndex": fi}})
	if err != nil {
		entry.WithError(err).Error("update shard-rebuilding tasks range")
		return 0, err
	}
	lastIdx := miner.FileIndex
	miner.FileIndex = fi
	miner.From = From
	miner.To = To
	rebuilder.lock2.Lock()
	rebuilder.Cache[miner.ID] = sibCache
	rebuilder.lock2.Unlock()
	rebuilder.lock.Lock()
	rebuilder.taskAllocator[miner.ID] = rcache
	rebuilder.lock.Unlock()
	return lastIdx, nil
}

func (rebuilder *Rebuilder) processRebuildableShard(ctx context.Context) {
	entry := log.WithFields(log.Fields{Function: "processRebuildableShard"})
	collection := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(NodeTab)
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	for {
		cur, err := collectionRM.Find(ctx, bson.M{"status": 2})
		if err != nil {
			entry.WithError(err).Error("fetching rebuildable miners")
			time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableShardInterval) * time.Second)
			continue
		}
		for cur.Next(ctx) {
			miner := new(RebuildMiner)
			err := cur.Decode(miner)
			if err != nil {
				entry.WithError(err).Warn("decoding miner")
				continue
			}
			entry.WithField(MinerID, miner.ID).Info("ready for fetching shards")
			result := collectionRS.FindOne(ctx, bson.M{"minerID": miner.ID, "timestamp": 0})
			if result.Err() == nil {
				entry.WithField(MinerID, miner.ID).Debug("unfinished shard-rebuilding tasks exist")
				continue
			}
			if result.Err() != mongo.ErrNoDocuments {
				entry.WithField(MinerID, miner.ID).WithError(err).Debug("finding unfinished shard-rebuilding tasks")
				continue
			}
			if miner.FinishBuild && miner.To+1 == miner.Next {
				//TODO: 增加判断是否全部重建完毕（超时判断）
				if miner.ExpiredTime == 0 {
					rebuilder.lock.Lock()
					delete(rebuilder.taskAllocator, miner.ID)
					rebuilder.lock.Unlock()
					rebuilder.lock2.Lock()
					delete(rebuilder.Cache, miner.ID)
					rebuilder.lock2.Unlock()
					miner.ExpiredTime = time.Now().UnixNano() + int64(rebuilder.Params.RebuildShardExpiredTime)*1000000000
					_, err = collectionRM.UpdateOne(ctx, bson.M{"_id": miner.ID}, bson.M{"$set": bson.M{"expiredTime": miner.ExpiredTime}})
					if err != nil {
						entry.WithField(MinerID, miner.ID).WithError(err).Error("update expired time")
						continue
					}
				}
				if time.Now().UnixNano() > miner.ExpiredTime {
					_, err := FetchFirstNodeShard(ctx, rebuilder.tikvCli, miner.ID)
					if err != nil {
						if err != NoValError {
							entry.WithField(MinerID, miner.ID).WithError(err).Error("finding starting shard")
						} else {
							msg := &pb.RebuiltMessage{NodeID: miner.ID}
							b, err := proto.Marshal(msg)
							if err != nil {
								entry.WithField(MinerID, miner.ID).WithError(err).Error("marshaling RebuiltMessage failed")
							} else {
								snID := int(miner.ID) % len(rebuilder.mqClis)
								ret := rebuilder.mqClis[snID].Send(ctx, fmt.Sprintf("sn%d", snID), append([]byte{byte(RebuiltMessage)}, b...))
								if !ret {
									entry.WithField(MinerID, miner.ID).Warn("sending RebuiltMessage failed")
								}
							}
						}
					} else {
						_, err = collectionRM.DeleteOne(ctx, bson.M{"_id": miner.ID})
						if err != nil {
							entry.WithField(MinerID, miner.ID).WithError(err).Error("delete rebuild miner")
						}
						_, err = collectionRS.DeleteMany(ctx, bson.M{"minerID": miner.ID})
						if err != nil {
							entry.WithField(MinerID, miner.ID).WithError(err).Error("delete finished shard-rebuilding tasks")
							continue
						}
						_, err = collection.UpdateOne(ctx, bson.M{"_id": miner.ID}, bson.M{"$set": bson.M{"tasktimestamp": time.Now().Unix() - int64(rebuilder.Params.RebuildableMinerTimeGap)}})
						if err != nil {
							entry.WithField(MinerID, miner.ID).WithError(err).Error("restart rebuild miner")
						}
						//TODO: 清空缓存
						cachepathExt := filepath.Join(rebuilder.Params.TaskCacheLocation, fmt.Sprintf("%d_%d.ext", miner.ID, miner.FileIndex))
						err = os.Remove(cachepathExt)
						if err != nil {
							entry.WithField(MinerID, miner.ID).WithError(err).Errorf("delete extend cache file failed: %s", cachepathExt)
						}
					}
				}
				continue
				// msg := &pb.RebuiltMessage{NodeID: miner.ID}
				// b, err := proto.Marshal(msg)
				// if err != nil {
				// 	entry.WithError(err).Error("marshaling RebuiltMessage failed")
				// 	continue
				// }
				// snID := int(miner.ID) % len(rebuilder.mqClis)
				// ret := rebuilder.mqClis[snID].Send(ctx, fmt.Sprintf("sn%d", snID), append([]byte{byte(RebuiltMessage)}, b...))
				// if !ret {
				// 	entry.Warnf("sending RebuiltMessage of miner %d failed", miner.ID)
				// }
				// continue
			}
			nf := miner.To + 1
			if miner.To == 0 {
				nf = miner.RangeFrom
			}
			if nf >= miner.Next {
				continue
			}
			lastnf, err := rebuilder.readCacheFile(ctx, miner, nf)
			if err != nil {
				entry.WithError(err).Error("error when reading cache file")
			} else {
				cachepathSrd := filepath.Join(rebuilder.Params.TaskCacheLocation, fmt.Sprintf("%d_%d.srd", miner.ID, nf))
				err = os.Remove(cachepathSrd)
				if err != nil {
					entry.WithError(err).Errorf("delete shards cache file failed: %s", cachepathSrd)
				}
				if lastnf != 0 {
					cachepathExt := filepath.Join(rebuilder.Params.TaskCacheLocation, fmt.Sprintf("%d_%d.ext", miner.ID, lastnf))
					err = os.Remove(cachepathExt)
					if err != nil {
						entry.WithError(err).Errorf("delete extend cache file failed: %s", cachepathExt)
					}
				}
			}
		}
		cur.Close(ctx)
		time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableShardInterval) * time.Second)
	}
}

func (rebuilder *Rebuilder) reaper(ctx context.Context) {
	entry := log.WithFields(log.Fields{Function: "reaper"})
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	for {
		cur, err := collectionRM.Find(ctx, bson.M{})
		if err != nil {
			entry.WithError(err).Error("fetching miners")
			time.Sleep(time.Duration(rebuilder.Params.ProcessReaperInterval) * time.Second)
			continue
		}
		for cur.Next(ctx) {
			miner := new(RebuildMiner)
			err := cur.Decode(miner)
			if err != nil {
				entry.WithError(err).Warn("decoding miner")
				continue
			}
			_, err = collectionRS.DeleteMany(ctx, bson.M{"minerID": miner.ID, "_id": bson.M{"$lt": miner.From}})
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Error("delete finished shard-rebuilding tasks")
			}
		}
		time.Sleep(time.Duration(rebuilder.Params.ProcessReaperInterval) * time.Second)
	}
}

//GetRebuildTasks get rebuild tasks
func (rebuilder *Rebuilder) GetRebuildTasks(ctx context.Context, id int32) (*pb.MultiTaskDescription, error) {
	entry := log.WithFields(log.Fields{Function: "GetRebuildTasks", RebuilderID: id})
	entry.Debug("ready for fetching rebuildable tasks")
	//collectionAS := rebuilder.analysisdbClient.Database(MetaDB).Collection(Shards)
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	//collectionRU := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(UnrebuildShardTab)
	rbNode := rebuilder.NodeManager.GetNode(id)
	if rbNode == nil {
		err := fmt.Errorf("node %d not found", id)
		entry.WithError(err).Error("fetch rebuilder miner")
		return nil, err
	}
	startTime := time.Now().UnixNano()
	//如果被分配重建任务的矿机状态大于1或者权重为零则不分配重建任务
	if rbNode.Rebuilding > 100000 || rbNode.Status > 1 || rbNode.Valid == 0 || rbNode.Weight < float64(rebuilder.Params.WeightThreshold) || rbNode.AssignedSpace <= 0 || rbNode.Quota <= 0 || rbNode.Version < int32(rebuilder.Params.MinerVersionThreshold) {
		err := fmt.Errorf("no tasks can be allocated to miner %d", id)
		entry.WithError(err).Debugf("status of rebuilder miner is %d, weight is %f, rebuilding is %d, version is %d", rbNode.Status, rbNode.Weight, rbNode.Rebuilding, rbNode.Version)
		return nil, err
	}
	tokenCount := rebuilder.ratelimiter.TakeAvailable(1)
	if tokenCount == 0 {
		err := errors.New("token bucket is empty, no tasks can be allocated")
		entry.Debug(err)
		return nil, err
	}

	rebuilder.lock.RLock()
	defer rebuilder.lock.RUnlock()
	for minerID, rcache := range rebuilder.taskAllocator {
		startTime = time.Now().UnixNano()
		rbshards := rcache.Allocate(0)
		if rbshards == nil {
			continue
		}
		//rshards := make([]*RebuildShard, 0)
		for _, item := range rbshards {
			// if item.ErrCount >= int32(rebuilder.Params.RetryCount+2) {
			// 	item.Timestamp = Int64Max
			// 	rcache.TagOne(item.ID, 0)
			// 	rebuilder.Cache.Delete(item.ID)
			// 	entry.WithField(MinerID, minerID).WithField(ShardID, item.ID).Warnf("reaching max count of retries: %d", rebuilder.Params.RetryCount)
			// 	_, err := collectionRU.InsertOne(ctx, item)
			// 	if err != nil {
			// 		entry.WithField(MinerID, minerID).WithField(ShardID, item.ID).WithError(err).Error("insert into unrebuild shard collection")
			// 	}
			// } else {
			// 	rshards = append(rshards, item)
			// }
			//rshards = append(rshards, item)
			r, err := collectionRS.UpdateOne(ctx, bson.M{"_id": item.ID}, bson.M{"$set": bson.M{"timestamp": item.Timestamp}})
			if err != nil {
				entry.WithField(MinerID, minerID).WithField(ShardID, id).WithError(err).Error("update timestamp")
			} else {
				if r.ModifiedCount == 1 {
					entry.WithField(ShardID, item.ID).Tracef("update timestamp to %d", item.Timestamp)
				} else {
					entry.WithField(ShardID, item.ID).Trace("no matched record for updating")
				}
			}
		}

		if len(rbshards) == 0 {
			continue
		}
		rebuilder.lock2.RLock()
		sibCache := rebuilder.Cache[minerID]
		rebuilder.lock2.RUnlock()
		expiredTime := time.Now().Unix() + int64(rebuilder.Params.RebuildShardExpiredTime)
		tasks := new(pb.MultiTaskDescription)
		for _, shard := range rbshards {
			hashs := make([][]byte, 0)
			locations := make([]*pb.P2PLocation, 0)
			item := sibCache.Get(shard.ID)
			if item != nil {
				hashs = item.Hashs
				for _, id := range item.NodeIDs {
					n := rebuilder.NodeManager.GetNode(id)
					if n == nil {
						locations = append(locations, &pb.P2PLocation{NodeId: "16Uiu2HAmKg7EXBqx3SXbE2XkqbPLft8NGkzQcsbJymVB9uw7fW1r", Addrs: []string{"/ip4/127.0.0.1/tcp/59999"}})
					} else {
						locations = append(locations, &pb.P2PLocation{NodeId: n.NodeID, Addrs: n.Addrs})
					}
				}
			} else {
				//遍历分块内全部分片
				for i := shard.BlockID; i < shard.BlockID+int64(shard.VNF); i++ {
					//s := new(Shard)
					//err := collectionAS.FindOne(ctx, bson.M{"_id": i}).Decode(s)
					//err := rebuilder.analysisdbClient.QueryRowxContext(ctx, "select * from shards where id=?", i).StructScan(s)
					s, err := FetchShard(ctx, rebuilder.tikvCli, i)
					if err != nil {
						entry.WithField(MinerID, minerID).WithField(ShardID, shard.ID).WithError(err).Errorf("decoding sibling shard %d", i)
						break
					}
					entry.WithField(MinerID, minerID).WithField(ShardID, shard.ID).Tracef("decode sibling shard info %d: %d", i, s.ID)
					// entry.WithField(MinerID, minerID).WithField(ShardID, shard.ID).Tracef("<time trace %d>4. Decode sibling shard %d: %d", randtag, i, (time.Now().UnixNano()-startTime)/1000000)
					// startTime = time.Now().UnixNano()
					hashs = append(hashs, s.VHF)
					n := rebuilder.NodeManager.GetNode(s.NodeID)
					if n == nil {
						entry.WithField(MinerID, minerID).WithField(ShardID, shard.ID).WithError(errors.New("miner not found")).Errorf("get miner info of sibling shard %d: %d", i, s.ID)
						locations = append(locations, &pb.P2PLocation{NodeId: "16Uiu2HAmKg7EXBqx3SXbE2XkqbPLft8NGkzQcsbJymVB9uw7fW1r", Addrs: []string{"/ip4/127.0.0.1/tcp/59999"}})
					} else {
						entry.WithField(MinerID, minerID).WithField(ShardID, shard.ID).Tracef("decode miner info of sibling shard %d: %d", s.ID, n.ID)
						locations = append(locations, &pb.P2PLocation{NodeId: n.NodeID, Addrs: n.Addrs})
					}
				}
			}
			if (shard.Type == 0x68b3 && len(locations) < int(shard.VNF)) || (shard.Type == 0xc258 && len(locations) == 0) {
				entry.WithField(MinerID, minerID).WithField(ShardID, shard.ID).Warnf("sibling shards are not enough, only %d shards", len(hashs))
				// s := rcache.TagOne(shard.ID, 0)
				// if s != nil {
				// 	s.ErrCount = 100
				// 	collectionRU.InsertOne(ctx, s)
				// 	r, err := collectionRS.UpdateOne(ctx, bson.M{"_id": s.ID}, bson.M{"$set": bson.M{"timestamp": s.Timestamp, "errCount": s.ErrCount}})
				// 	if err != nil {
				// 		entry.WithField(MinerID, minerID).WithField(ShardID, s.ID).WithError(err).Errorf("update timestamp to %d", s.Timestamp)
				// 	} else {
				// 		if r.ModifiedCount == 1 {
				// 			entry.WithField(ShardID, s.ID).Tracef("update timestamp to %d", s.Timestamp)
				// 		} else {
				// 			entry.WithField(ShardID, s.ID).Trace("no matched record for updating")
				// 		}
				// 	}
				// }
				// shard.Timestamp = Int64Max
				continue
			}
			var b []byte
			var err error
			if shard.Type == 0x68b3 {
				//LRC
				task := new(pb.TaskDescription)
				task.Id = append(Int64ToBytes(shard.ID), Uint16ToBytes(uint16(shard.SNID))...)
				task.Hashs = hashs
				task.ParityShardCount = shard.ParityShardCount
				task.RecoverId = int32(shard.ID - shard.BlockID)
				task.Locations = locations
				b, err = proto.Marshal(task)
				if err != nil {
					entry.WithField(MinerID, minerID).WithField(ShardID, shard.ID).WithError(err).Errorf("marshaling LRC task: %d", shard.ID)
					continue
				}
			} else if shard.Type == 0xc258 {
				//replication
				task := new(pb.TaskDescriptionCP)
				task.Id = append(Int64ToBytes(shard.ID), Uint16ToBytes(uint16(shard.SNID))...)
				task.DataHash = shard.VHF
				task.Locations = locations
				b, err = proto.Marshal(task)
				if err != nil {
					entry.WithField(MinerID, minerID).WithField(ShardID, shard.ID).WithError(err).Errorf("marshaling replication task: %d", shard.ID)
					continue
				}
			}
			btask := append(Uint16ToBytes(uint16(shard.Type)), b...)
			tasks.Tasklist = append(tasks.Tasklist, btask)
		}
		tasks.ExpiredTime = expiredTime
		tasks.SrcNodeID = minerID
		tasks.ExpiredTimeGap = int32(rebuilder.Params.RebuildShardExpiredTime)
		// for _, shard := range rshards {
		// 	if shard.Timestamp != Int64Max {
		// 		continue
		// 	}
		// 	r, err := collectionRS.UpdateOne(context.Background(), bson.M{"_id": shard.ID}, bson.M{"$set": bson.M{"timestamp": shard.Timestamp}})
		// 	if err != nil {
		// 		entry.WithField(MinerID, minerID).WithField(ShardID, shard.ID).WithError(err).Error("update timestamp of rebuildable shard failed")
		// 	} else {
		// 		if r.ModifiedCount == 1 {
		// 			entry.WithField(ShardID, shard.ID).Debugf("update timestamp to %d", shard.Timestamp)
		// 		} else {
		// 			entry.WithField(ShardID, shard.ID).Debug("no matched record for updating")
		// 		}
		// 	}
		// }
		entry.WithField(MinerID, minerID).Debugf("length of task list is %d, expired time is %d, total time: %dms", len(tasks.Tasklist), tasks.ExpiredTime, (time.Now().UnixNano()-startTime)/1000000)
		return tasks, nil
	}
	err := errors.New("no tasks can be allocated")
	entry.Warn(err)
	return nil, err
}

//UpdateTaskStatus update task status
func (rebuilder *Rebuilder) UpdateTaskStatus(ctx context.Context, result *pb.MultiTaskOpResult) error {
	nodeID := result.NodeID
	srcNodeID := result.SrcNodeID
	//startTime := time.Now().UnixNano()
	entry := log.WithFields(log.Fields{Function: "UpdateTaskStatus", MinerID: srcNodeID, RebuilderID: nodeID})
	if time.Now().Unix() > result.ExpiredTime {
		entry.Warn("tasks expired")
		return errors.New("tasks expired")
	}
	entry.Debugf("received rebuilding status: %d results", len(result.Id))

	ids := make([]int64, 0)
	rets := make([]int32, 0)
	dedup := make(map[int64]bool)
	for i, b := range result.Id {
		id := BytesToInt64(b[0:8])
		if _, ok := dedup[id]; !ok {
			dedup[id] = true
			ids = append(ids, id)
			rets = append(rets, result.RES[i])
		}
	}
	// var results []*RebuildShard
	// var err error
	// rebuilder.lock.RLock()
	// defer rebuilder.lock.RUnlock()
	// if rebuilder.taskAllocator[srcNodeID] != nil {
	// 	results, err = rebuilder.taskAllocator[srcNodeID].TagMulti(ids, rets)
	// 	if err != nil {
	// 		return err
	// 	}
	// } else {
	// 	entry.Errorf("no match task of miner %d", srcNodeID)
	// 	return fmt.Errorf("no match task of miner %d", srcNodeID)
	// }
	// collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	for i, id := range ids {
		if rets[i] == 0 {
			entry.WithField(ShardID, id).Debug("task rebuilt success")
		} else {
			entry.WithField(ShardID, id).Debug("task rebuilt failed")
		}
		// shard := results[i]
		// if shard != nil {
		// 	if shard.Timestamp == Int64Max {
		// 		rebuilder.Cache.Delete(id)
		// 		r, err := collectionRS.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"timestamp": Int64Max}})
		// 		if err != nil {
		// 			entry.WithField(ShardID, id).WithError(err).Errorf("update timestamp to %d", Int64Max)
		// 			return err
		// 		}
		// 		if r.ModifiedCount == 1 {
		// 			entry.WithField(ShardID, id).Tracef("update timestamp to %d", Int64Max)
		// 		} else {
		// 			entry.WithField(ShardID, id).Trace("no matched record for updating")
		// 		}
		// 		entry.WithField(ShardID, id).Debugf("task rebuilt success: matched %d, modified %d", r.MatchedCount, r.ModifiedCount)
		// 	} else {
		// 		entry.WithField(ShardID, id).Debugf("task rebuilt failed, current error count: %d", shard.ErrCount)
		// 	}
		// } else {
		// 	entry.WithField(ShardID, id).Debugf("task expired or not found: %d", result.GetExpiredTime())
		// }
	}
	//entry.Debugf("Update task status (%d tasks): %dms", len(result.Id), (time.Now().UnixNano()-startTime)/1000000)
	return nil
}

func FetchFirstNodeShard(ctx context.Context, tikvCli *rawkv.Client, nodeId int32) (*Shard, error) {
	shards, err := FetchNodeShards(ctx, tikvCli, nodeId, 0, 1)
	if err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		return nil, NoValError
	}
	return shards[0], nil
}

func FetchLastNodeShard(ctx context.Context, tikvCli *rawkv.Client, nodeId int32) (*Shard, error) {
	from := fmt.Sprintf("%019d", 0)
	to := "9999999999999999999"
	_, values, err := tikvCli.ReverseScan(ctx, append([]byte(fmt.Sprintf("%s_%d_%s", PFX_SHARDNODES, nodeId, to)), '\x00'), append([]byte(fmt.Sprintf("%s_%d_%s", PFX_SHARDNODES, nodeId, from)), '\x00'), 1)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, NoValError
	}
	shards := make([]*Shard, 0)
	for _, buf := range values {
		s := new(Shard)
		err := s.FillBytes(buf)
		if err != nil {
			return nil, err
		}
		shards = append(shards, s)
	}
	return shards[0], nil
}
