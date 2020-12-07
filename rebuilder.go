package ytrebuilder

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aurawing/auramq"
	"github.com/aurawing/auramq/msg"
	proto "github.com/golang/protobuf/proto"
	"github.com/ivpusic/grpool"
	rl "github.com/juju/ratelimit"
	log "github.com/sirupsen/logrus"
	pb "github.com/yottachain/yotta-rebuilder/pbrebuilder"
	ytsync "github.com/yottachain/yotta-rebuilder/sync"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Int64Max max value of int64
const Int64Max int64 = 9223372036854775807

//Rebuilder rebuilder
type Rebuilder struct {
	analysisdbClient  *mongo.Client
	rebuilderdbClient *mongo.Client
	NodeManager       *NodeManager
	Cache             *Cache
	Compensation      *CompensationConfig
	Params            *MiscConfig
	mqClis            map[int]*ytsync.Service
	ratelimiter       *rl.Bucket
	httpCli           *http.Client
	taskAllocator     map[int32]*RingCache
	lock              sync.RWMutex
}

//New create a new rebuilder instance
func New(analysisDBURL, rebuilderDBURL string, mqconf *AuraMQConfig, cpsConf *CompensationConfig, conf *MiscConfig) (*Rebuilder, error) {
	entry := log.WithFields(log.Fields{Function: "New"})
	analysisdbClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(analysisDBURL))
	if err != nil {
		entry.WithError(err).Errorf("creating analysisDB client failed: %s", analysisDBURL)
		return nil, err
	}
	entry.Infof("analysisDB client created: %s", analysisDBURL)
	rebuilderdbClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(rebuilderDBURL))
	if err != nil {
		entry.WithError(err).Errorf("creating rebuilderDB client failed: %s", rebuilderDBURL)
		return nil, err
	}
	entry.Infof("rebuilderDB client created: %s", rebuilderDBURL)
	nodeMgr, err := NewNodeManager(rebuilderdbClient)
	if err != nil {
		entry.WithError(err).Error("creating node manager failed")
		return nil, err
	}
	entry.Info("node manager created")
	cache := NewCache(conf.MaxCacheSize)
	entry.Info("cache created")
	pool := grpool.NewPool(conf.SyncPoolLength, conf.SyncQueueLength)
	taskAllocator := make(map[int32]*RingCache)
	ratelimiter := rl.NewBucketWithRate(float64(conf.FetchTaskRate), int64(conf.FetchTaskRate))
	rebuilder := &Rebuilder{analysisdbClient: analysisdbClient, rebuilderdbClient: rebuilderdbClient, NodeManager: nodeMgr, Cache: cache, Compensation: cpsConf, Params: conf, ratelimiter: ratelimiter, httpCli: &http.Client{}, taskAllocator: taskAllocator}
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
					rebuilder.syncNode(node)
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

func (rebuilder *Rebuilder) syncNode(node *Node) error {
	entry := log.WithFields(log.Fields{Function: "syncNode"})
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
	_, err := collection.InsertOne(context.Background(), bson.M{"_id": node.ID, "nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "poolOwner": node.PoolOwner, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "uspaces": node.Uspaces, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx, "other": otherDoc})
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			entry.WithError(err).Warnf("inserting node %d to database", node.ID)
			return err
		}
		oldNode := new(Node)
		err := collection.FindOne(context.Background(), bson.M{"_id": node.ID}).Decode(oldNode)
		if err != nil {
			entry.WithError(err).Warnf("fetching node %d failed", node.ID)
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
		result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": cond}, opts)
		updatedNode := new(Node)
		err = result.Decode(updatedNode)
		if err != nil {
			entry.WithError(err).Warnf("updating record of node %d", node.ID)
			return err
		}
		rebuilder.NodeManager.UpdateNode(updatedNode)
		if updatedNode.Status == 3 && oldNode.Status == 2 {
			//重建完毕，状态改为3，删除旧任务
			collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
			collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
			_, err := collectionRS.DeleteMany(context.Background(), bson.M{"minerID": updatedNode.ID})
			if err != nil {
				entry.WithField(MinerID, updatedNode.ID).WithError(err).Error("delete old shard-rebuildng tasks")
				return nil
			}
			_, err = collectionRM.UpdateOne(context.Background(), bson.M{"_id": updatedNode.ID}, bson.M{"$set": bson.M{"from": 0, "to": 0, "status": 3}})
			if err != nil {
				entry.WithField(MinerID, updatedNode.ID).WithError(err).Error("update rebuild miner status to 3")
			} else {
				entry.WithField(MinerID, updatedNode.ID).Info("all rebuild tasks finished")
			}
			rebuilder.lock.Lock()
			delete(rebuilder.taskAllocator, updatedNode.ID)
			rebuilder.lock.Unlock()
			return nil
		}
	} else {
		rebuilder.NodeManager.UpdateNode(node)
	}
	return nil
}

//BinTuple binary tuple
type BinTuple struct {
	A int32
	B int64
}

//Start starting rebuilding process
func (rebuilder *Rebuilder) Start() {
	entry := log.WithFields(log.Fields{Function: "Start"})
	rebuilder.Preprocess()
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	collectionRU := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(UnrebuildShardTab)
	collectionAS := rebuilder.analysisdbClient.Database(MetaDB).Collection(Shards)
	opts := options.FindOptions{}
	opts.Sort = bson.M{"_id": 1}
	cachetmp := make(map[int32][]*RebuildShard)
	failedtmp := make(map[int32][]int64)
	curShard, err := collectionRS.Find(context.Background(), bson.M{"timestamp": bson.M{"$lt": Int64Max}}, &opts)
	if err != nil {
		entry.WithError(err).Error("pre-fetching rebuildable shards failed")
	} else {
		ch := make(chan *BinTuple, 1000)
		go func() {
			for {
				select {
				case val, ok := <-ch:
					if !ok {
						entry.Debug("finished collecting failed shards")
						return
					}
					if failedtmp[val.A] == nil {
						failedtmp[val.A] = make([]int64, 0)
					}
					failedtmp[val.A] = append(failedtmp[val.A], val.B)
					entry.Debugf("collecting failed shards: %d", val.B)
				}
			}
		}()
		entry.Debugf("ready for filling cache")
		idx := 0
		wg := sync.WaitGroup{}
		wg.Add(rebuilder.Params.MaxConcurrentTaskBuilderSize)
		for curShard.Next(context.Background()) {
			rshard := new(RebuildShard)
			err := curShard.Decode(rshard)
			if err != nil {
				entry.WithError(err).Error("decoding rebuildable shard failed")
				continue
			}
			idx++
			go func() {
				defer wg.Done()
				drop := false
				if !rebuilder.Cache.IsFull() {
					opts := options.FindOptions{}
					opts.Sort = bson.M{"_id": 1}
					scur, err := collectionAS.Find(context.Background(), bson.M{"_id": bson.M{"$gte": rshard.BlockID, "$lt": rshard.BlockID + int64(rshard.VNF)}}, &opts)
					if err != nil {
						entry.WithField(MinerID, rshard.MinerID).WithField(ShardID, rshard.ID).WithError(err).Error("fetching sibling shards failed")
					} else {
						//遍历分块内全部分片
						hashs := make([][]byte, 0)
						nodeIDs := make([]int32, 0)
						i := rshard.BlockID
						for scur.Next(context.Background()) {
							s := new(Shard)
							err := scur.Decode(s)
							if err != nil {
								entry.WithField(MinerID, rshard.MinerID).WithField(ShardID, s.ID).WithError(err).Errorf("decoding sibling shard %d failed", i)
								drop = true
								break
							}
							entry.WithField(MinerID, rshard.MinerID).WithField(ShardID, rshard.ID).Tracef("decode sibling shard info %d", i)
							if s.ID != i {
								entry.WithField(MinerID, rshard.MinerID).WithField(ShardID, s.ID).WithError(err).Errorf("sibling shard %d not found: %d", i, s.ID)
								drop = true
								break
							}
							hashs = append(hashs, s.VHF.Data)
							nodeIDs = append(nodeIDs, s.NodeID)
							i++
						}
						scur.Close(context.Background())
						if len(hashs) == int(rshard.VNF) {
							rebuilder.Cache.Put(rshard.ID, hashs, nodeIDs)
						}
					}
				}
				if drop {
					entry.WithField(MinerID, rshard.MinerID).WithField(ShardID, rshard.ID).Warn("rebuilding task create failed: sibling shards lost")
					collectionRU.InsertOne(context.Background(), rshard)
					collectionRS.UpdateOne(context.Background(), bson.M{"_id": rshard.ID}, bson.M{"$set": bson.M{"timestamp": Int64Max}})
					ch <- &BinTuple{A: rshard.MinerID, B: rshard.ID}
				}
			}()
			if idx%rebuilder.Params.MaxConcurrentTaskBuilderSize == 0 {
				wg.Wait()
				wg = sync.WaitGroup{}
				wg.Add(rebuilder.Params.MaxConcurrentTaskBuilderSize)
				idx = 0
			}
			if cachetmp[rshard.MinerID] == nil {
				cachetmp[rshard.MinerID] = make([]*RebuildShard, 0)
			}
			cachetmp[rshard.MinerID] = append(cachetmp[rshard.MinerID], rshard)
			entry.Debugf("add shard to ring cache: %d", rshard.ID)
		}
		for j := 0; j < rebuilder.Params.MaxConcurrentTaskBuilderSize-idx; j++ {
			wg.Done()
		}
		if idx != 0 {
			wg.Wait()
		}
		curShard.Close(context.Background())
		close(ch)
		rebuilder.lock.Lock()
		defer rebuilder.lock.Unlock()
		for k, v := range cachetmp {
			rcache := NewRingCache(v, rebuilder.Params.RebuildShardMinerTaskBatchSize, rebuilder.Params.RebuildShardExpiredTime, rebuilder.Params.RetryCount)
			entry.Debugf("create ring cache of miner %d", k)
			if len(failedtmp[k]) > 0 {
				fids := failedtmp[k]
				tags := make([]int32, len(fids))
				for i := 0; i < len(fids); i++ {
					tags[i] = 0
				}
				rcache.TagMulti(fids, tags)
				entry.Debugf("delete % failed shards of miner %d", len(fids), k)
			}
			rebuilder.taskAllocator[k] = rcache
		}
		entry.Debugf("cache filling finished")
	}

	go rebuilder.Compensate()
	go rebuilder.processRebuildableMiner()
	go rebuilder.processRebuildableShard()
	go rebuilder.reaper()
}

func (rebuilder *Rebuilder) processRebuildableMiner() {
	entry := log.WithFields(log.Fields{Function: "processRebuildableMiner"})
	collection := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(NodeTab)
	collectionAS := rebuilder.analysisdbClient.Database(MetaDB).Collection(Shards)
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	entry.Info("starting rebuildable node processor")
	for {
		cur, err := collection.Find(context.Background(), bson.M{"status": 2, "tasktimestamp": bson.M{"$exists": true, "$lt": time.Now().Unix() - int64(rebuilder.Params.RebuildableMinerTimeGap)}})
		if err != nil {
			entry.WithError(err).Error("cannot fetch rebuildable nodes")
			time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableMinerInterval) * time.Second)
			continue
		}
		for cur.Next(context.Background()) {
			cnt, err := collectionRM.CountDocuments(context.Background(), bson.M{"status": bson.M{"$lt": 3}})
			if err != nil {
				entry.WithError(err).Error("cannot calculate count of rebuilding nodes")
				break
			}
			if cnt >= int64(rebuilder.Params.RebuildingMinerCountPerBatch) {
				entry.Debugf("reaching max count of rebuilding miners")
				break
			}
			node := new(Node)
			err = cur.Decode(node)
			if err != nil {
				entry.WithError(err).Warn("decoding node")
				continue
			}

			rangeFrom := int64(0)
			rangeTo := int64(0)
			shardFrom := new(Shard)
			opts := options.FindOneOptions{}
			opts.Sort = bson.M{"_id": 1}
			err = collectionAS.FindOne(context.Background(), bson.M{"nodeId": node.ID}, &opts).Decode(shardFrom)
			if err != nil {
				if err != mongo.ErrNoDocuments {
					entry.WithError(err).Warnf("finding starting shard of miner %d", node.ID)
					continue
				}
			} else {
				rangeFrom = shardFrom.ID
			}
			shardTo := new(Shard)
			opts.Sort = bson.M{"_id": -1}
			err = collectionAS.FindOne(context.Background(), bson.M{"nodeId": node.ID}, &opts).Decode(shardTo)
			if err != nil {
				if err != mongo.ErrNoDocuments {
					entry.WithError(err).Warnf("finding ending shard of miner %d", node.ID)
					continue
				}
			} else {
				rangeTo = shardTo.ID
			}

			miner := new(RebuildMiner)
			miner.ID = node.ID
			miner.RangeFrom = rangeFrom
			miner.RangeTo = rangeTo
			miner.Status = node.Status
			miner.Timestamp = time.Now().Unix()
			_, err = collectionRM.InsertOne(context.Background(), miner)
			if err != nil {
				entry.WithError(err).Warnf("insert rebuild miner %d to database", node.ID)
			}
			collection.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$unset": bson.M{"tasktimestamp": ""}})
			entry.WithField(MinerID, miner.ID).Info("ready for rebuilding")
		}
		cur.Close(context.Background())
		time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableMinerInterval) * time.Second)
	}
}

func (rebuilder *Rebuilder) processRebuildableShard() {
	entry := log.WithFields(log.Fields{Function: "processRebuildableShard"})
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	collectionRU := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(UnrebuildShardTab)
	collectionAS := rebuilder.analysisdbClient.Database(MetaDB).Collection(Shards)
	collectionAB := rebuilder.analysisdbClient.Database(MetaDB).Collection(Blocks)
	for {
		cur, err := collectionRM.Find(context.Background(), bson.M{"status": 2})
		if err != nil {
			entry.WithError(err).Error("fetching rebuildable miners")
			time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableShardInterval) * time.Second)
			continue
		}
	INNER:
		for cur.Next(context.Background()) {
			miner := new(RebuildMiner)
			err := cur.Decode(miner)
			if err != nil {
				entry.WithError(err).Warn("decoding miner")
				continue
			}
			entry.WithField(MinerID, miner.ID).Info("ready for fetching shards")
			result := collectionRS.FindOne(context.Background(), bson.M{"minerID": miner.ID, "timestamp": bson.M{"$lt": Int64Max}})
			if result.Err() == nil {
				entry.WithField(MinerID, miner.ID).Debug("unfinished shard-rebuilding tasks exist")
				continue
			}
			if result.Err() != mongo.ErrNoDocuments {
				entry.WithField(MinerID, miner.ID).WithError(err).Debug("finding unfinished shard-rebuilding tasks")
				continue
			}
			cachetmp := make([]*RebuildShard, 0)
			failedtmp := make([]int64, 0)
			opts := options.FindOptions{}
			opts.Sort = bson.M{"_id": 1}
			limit := int64(rebuilder.Params.RebuildShardTaskBatchSize)
			opts.Limit = &limit
			curShard, err := collectionAS.Find(context.Background(), bson.M{"nodeId": miner.ID, "_id": bson.M{"$gt": miner.To}}, &opts)
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Error("fetching shard-rebuilding tasks")
				continue
			}
			ch := make(chan int64, 1000)
			go func() {
				for {
					select {
					case val, ok := <-ch:
						if !ok {
							entry.Debugf("finished collecting failed shards of miner %d", miner.ID)
							return
						}
						failedtmp = append(failedtmp, val)
						entry.Debugf("collecting failed shards: %d", val)
					}
				}
			}()
			shards := make([]interface{}, 0)
			var lock sync.Mutex
			idx := 0
			wg := sync.WaitGroup{}
			wg.Add(rebuilder.Params.MaxConcurrentTaskBuilderSize)
			for curShard.Next(context.Background()) {
				shard := new(Shard)
				err := curShard.Decode(shard)
				if err != nil {
					entry.WithField(MinerID, miner.ID).WithError(err).Error("decoding shard")
					curShard.Close(context.Background())
					close(ch)
					continue INNER
				}
				block := new(Block)
				err = collectionAB.FindOne(context.Background(), bson.M{"_id": shard.BlockID}).Decode(block)
				if err != nil {
					entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).WithError(err).Errorf("decoding block of shard %d", shard.ID)
					curShard.Close(context.Background())
					close(ch)
					continue INNER
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
					if !rebuilder.Cache.IsFull() {
						opts := options.FindOptions{}
						opts.Sort = bson.M{"_id": 1}
						scur, err := collectionAS.Find(context.Background(), bson.M{"_id": bson.M{"$gte": rshard.BlockID, "$lt": rshard.BlockID + int64(rshard.VNF)}}, &opts)
						if err != nil {
							entry.WithField(MinerID, miner.ID).WithField(ShardID, rshard.ID).WithError(err).Error("fetching sibling shards failed")
						} else {
							//遍历分块内全部分片
							hashs := make([][]byte, 0)
							nodeIDs := make([]int32, 0)
							i := rshard.BlockID
							for scur.Next(context.Background()) {
								s := new(Shard)
								err := scur.Decode(s)
								if err != nil {
									entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).WithError(err).Errorf("decoding sibling shard %d", i)
									drop = true
									break
								}
								entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Tracef("decode sibling shard info %d", i)
								if s.ID != i {
									entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).WithError(err).Errorf("sibling shard %d not found: %d", i, s.ID)
									drop = true
									break
								}
								hashs = append(hashs, s.VHF.Data)
								nodeIDs = append(nodeIDs, s.NodeID)
								i++
							}
							if len(hashs) == int(rshard.VNF) {
								rebuilder.Cache.Put(rshard.ID, hashs, nodeIDs)
							}
						}
					}

					if drop {
						entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Warn("rebuilding task create failed: sibling shards lost")
						collectionRU.InsertOne(context.Background(), rshard)
						ch <- shard.ID
					} else {
						lock.Lock()
						shards = append(shards, *rshard)
						lock.Unlock()
					}
				}()
				if idx%rebuilder.Params.MaxConcurrentTaskBuilderSize == 0 {
					wg.Wait()
					wg = sync.WaitGroup{}
					wg.Add(rebuilder.Params.MaxConcurrentTaskBuilderSize)
					idx = 0
				}
				cachetmp = append(cachetmp, rshard)
				entry.Debugf("add shard to ring cache: %d", rshard.ID)
			}
			for j := 0; j < rebuilder.Params.MaxConcurrentTaskBuilderSize-idx; j++ {
				wg.Done()
			}
			if idx != 0 {
				wg.Wait()
			}
			curShard.Close(context.Background())
			close(ch)
			if len(shards) == 0 {
				msg := &pb.RebuiltMessage{NodeID: miner.ID}
				b, err := proto.Marshal(msg)
				if err != nil {
					entry.WithError(err).Error("marshaling RebuiltMessage failed")
					continue
				}
				snID := int(miner.ID) % len(rebuilder.mqClis)
				ret := rebuilder.mqClis[snID].Send(fmt.Sprintf("sn%d", snID), append([]byte{byte(RebuiltMessage)}, b...))
				if !ret {
					entry.Warnf("sending RebuiltMessage of miner %d failed", miner.ID)
				}
				continue
			}
			sort.Slice(shards, func(i, j int) bool {
				return shards[i].(RebuildShard).ID < shards[j].(RebuildShard).ID
			})
			_, err = collectionRS.InsertMany(context.Background(), shards)
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Warn("insert shard-rebuilding tasks")
			}
			From := shards[0].(RebuildShard).ID
			To := shards[len(shards)-1].(RebuildShard).ID
			_, err = collectionRM.UpdateOne(context.Background(), bson.M{"_id": miner.ID}, bson.M{"$set": bson.M{"from": From, "to": To}})
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Warn("update shard-rebuilding tasks range")
			}
			rcache := NewRingCache(cachetmp, rebuilder.Params.RebuildShardMinerTaskBatchSize, rebuilder.Params.RebuildShardExpiredTime, rebuilder.Params.RetryCount)
			entry.Debugf("create ring cache of miner %d", miner.ID)
			if len(failedtmp) > 0 {
				tags := make([]int32, len(failedtmp))
				for i := 0; i < len(failedtmp); i++ {
					tags[i] = 0
				}
				rcache.TagMulti(failedtmp, tags)
				entry.Debugf("delete % failed shards of miner %d", len(failedtmp), miner.ID)
			}
			rebuilder.lock.Lock()
			rebuilder.taskAllocator[miner.ID] = rcache
			rebuilder.lock.Unlock()
		}
		cur.Close(context.Background())
		time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableShardInterval) * time.Second)
	}
}

func (rebuilder *Rebuilder) reaper() {
	entry := log.WithFields(log.Fields{Function: "reaper"})
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	for {
		cur, err := collectionRM.Find(context.Background(), bson.M{})
		if err != nil {
			entry.WithError(err).Error("fetching miners")
			time.Sleep(time.Duration(rebuilder.Params.ProcessReaperInterval) * time.Second)
			continue
		}
		for cur.Next(context.Background()) {
			miner := new(RebuildMiner)
			err := cur.Decode(miner)
			if err != nil {
				entry.WithError(err).Warn("decoding miner")
				continue
			}
			_, err = collectionRS.DeleteMany(context.Background(), bson.M{"minerID": miner.ID, "_id": bson.M{"$lt": miner.From}})
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Error("delete finished shard-rebuilding tasks")
			}
		}
		time.Sleep(time.Duration(rebuilder.Params.ProcessReaperInterval) * time.Second)
	}
}

//GetRebuildTasks get rebuild tasks
func (rebuilder *Rebuilder) GetRebuildTasks(id int32) (*pb.MultiTaskDescription, error) {
	entry := log.WithFields(log.Fields{Function: "GetRebuildTasks", RebuilderID: id})
	entry.Debug("ready for fetching rebuildable tasks")
	// r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// randtag := r.Int31()
	collectionAS := rebuilder.analysisdbClient.Database(MetaDB).Collection(Shards)
	//collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	collectionRU := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(UnrebuildShardTab)
	rbNode := rebuilder.NodeManager.GetNode(id)
	if rbNode == nil {
		err := fmt.Errorf("node %d not found", id)
		entry.WithError(err).Error("fetch rebuilder miner")
		return nil, err
	}
	startTime := time.Now().UnixNano()
	//如果被分配重建任务的矿机状态大于1或者权重为零则不分配重建任务
	if rbNode.Rebuilding > 0 || rbNode.Status > 1 || rbNode.Weight < float64(rebuilder.Params.WeightThreshold) || rbNode.AssignedSpace <= 0 || rbNode.Quota <= 0 || rbNode.Version < int32(rebuilder.Params.MinerVersionThreshold) {
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
		rshards := rcache.Allocate()
		if rshards == nil {
			continue
		}
		expiredTime := time.Now().Unix() + int64(rebuilder.Params.RebuildShardExpiredTime)
		tasks := new(pb.MultiTaskDescription)
		for _, shard := range rshards {
			hashs := make([][]byte, 0)
			locations := make([]*pb.P2PLocation, 0)
			item := rebuilder.Cache.Get(shard.ID)
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
					s := new(Shard)
					err := collectionAS.FindOne(context.Background(), bson.M{"_id": i}).Decode(s)
					if err != nil {
						entry.WithField(MinerID, minerID).WithField(ShardID, shard.ID).WithError(err).Errorf("decoding sibling shard %d", i)
						break
					}
					entry.WithField(MinerID, minerID).WithField(ShardID, shard.ID).Tracef("decode sibling shard info %d: %d", i, s.ID)
					// entry.WithField(MinerID, minerID).WithField(ShardID, shard.ID).Tracef("<time trace %d>4. Decode sibling shard %d: %d", randtag, i, (time.Now().UnixNano()-startTime)/1000000)
					// startTime = time.Now().UnixNano()
					hashs = append(hashs, s.VHF.Data)
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
				s := rcache.TagOne(shard.ID, 0)
				if s != nil {
					s.ErrCount = 100
					collectionRU.InsertOne(context.Background(), s)
					collectionRS.UpdateOne(context.Background(), bson.M{"_id": s.ID}, bson.M{"$set": bson.M{"timestamp": s.Timestamp}})
				}
				shard.Timestamp = Int64Max
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
		for _, shard := range rshards {
			if shard.Timestamp == Int64Max {
				continue
			}
			_, err := collectionRS.UpdateOne(context.Background(), bson.M{"_id": shard.ID}, bson.M{"$set": bson.M{"timestamp": shard.Timestamp}})
			if err != nil {
				entry.WithField(MinerID, minerID).WithField(ShardID, shard.ID).WithError(err).Error("update timestamp of rebuildable shard failed")
			}
		}
		entry.WithField(MinerID, minerID).Debugf("length of task list is %d, expired time is %d, total time: %dms", len(tasks.Tasklist), tasks.ExpiredTime, (time.Now().UnixNano()-startTime)/1000000)
		return tasks, nil
	}
	err := errors.New("no tasks can be allocated")
	entry.Warn(err)
	return nil, err
}

//UpdateTaskStatus update task status
func (rebuilder *Rebuilder) UpdateTaskStatus(result *pb.MultiTaskOpResult) error {
	nodeID := result.NodeID
	srcNodeID := result.SrcNodeID
	if time.Now().Unix() > result.ExpiredTime {
		return errors.New("tasks expired")
	}
	startTime := time.Now().UnixNano()
	entry := log.WithFields(log.Fields{Function: "UpdateTaskStatus", MinerID: srcNodeID, RebuilderID: nodeID})
	entry.Infof("received rebuilding status: %d results", len(result.Id))

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
	var results []*RebuildShard
	var err error
	rebuilder.lock.RLock()
	defer rebuilder.lock.RUnlock()
	if rebuilder.taskAllocator[srcNodeID] != nil {
		results, err = rebuilder.taskAllocator[srcNodeID].TagMulti(ids, rets)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("no match task of miner %d", srcNodeID)
	}

	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	collectionRU := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(UnrebuildShardTab)
	for i, id := range ids {
		shard := results[i]
		if shard != nil {
			if shard.Timestamp == Int64Max {
				rebuilder.Cache.Delete(id)
				if shard.ErrCount == int32(rebuilder.Params.RetryCount) {
					entry.WithField(ShardID, id).Warnf("reaching max count of retries: %d", rebuilder.Params.RetryCount)
					collectionRU.InsertOne(context.Background(), shard)
				} else {
					entry.WithField(ShardID, id).Debug("task rebuilt success")
				}
			} else {
				entry.WithField(ShardID, id).Debugf("task rebuilt failed, current error count: %d", shard.ErrCount)
			}
			_, err := collectionRS.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": bson.M{"timestamp": shard.Timestamp, "errCount": shard.ErrCount}})
			if err != nil {
				entry.WithField(ShardID, id).WithError(err).Error("update timestamp and errCount")
				return err
			}
		} else {
			entry.WithField(ShardID, id).Debugf("task expired or not found: %d", result.GetExpiredTime())
		}
	}
	entry.Debugf("Update task status (%d tasks): %dms", len(result.Id), (time.Now().UnixNano()-startTime)/1000000)
	return nil
}
