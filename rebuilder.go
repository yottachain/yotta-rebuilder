package ytrebuilder

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aurawing/auramq"
	"github.com/aurawing/auramq/msg"
	proto "github.com/golang/protobuf/proto"
	"github.com/ivpusic/grpool"
	log "github.com/sirupsen/logrus"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	ytab "github.com/yottachain/yotta-arraybase"
	pb "github.com/yottachain/yotta-rebuilder/pbrebuilder"
	ytsync "github.com/yottachain/yotta-rebuilder/sync"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Int64Max max value of int64
//const Int64Max int64 = 9223372036854775807
var ErrNoTaskAlloc error = errors.New("no tasks can be allocated")
var ErrInvalidNode error = errors.New("invalid rebuilding node")

//Rebuilder rebuilder
type Rebuilder struct {
	tikvCli           *rawkv.Client
	rebuilderdbClient *mongo.Client
	NodeManager       *NodeManager
	Compensation      *CompensationConfig
	Params            *MiscConfig
	mqClis            map[int]*ytsync.Service
	httpCli           *http.Client
	lock              sync.RWMutex
	taskAllocator     map[int32]*TaskChan
	checkPoints       map[int32]int64
	lock2             sync.RWMutex
	taskSender        *TaskSender
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
	pool := grpool.NewPool(conf.SyncPoolLength, conf.SyncQueueLength)
	rebuilder := &Rebuilder{tikvCli: tikvCli, rebuilderdbClient: rebuilderdbClient, NodeManager: nodeMgr, Compensation: cpsConf, Params: conf, httpCli: &http.Client{}, taskAllocator: make(map[int32]*TaskChan), checkPoints: make(map[int32]int64), taskSender: NewTaskSender()}
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
		tcond := bson.M{"$set": cond}
		for k, v := range node.Uspaces {
			cond[fmt.Sprintf("uspaces.%s", k)] = v
		}
		if (node.Status == 2 || node.Status == 3) && oldNode.Status == 1 {
			cond["tasktimestamp"] = time.Now().Unix()
		} else if node.Status == 1 {
			tcond["$unset"] = bson.M{"tasktimestamp": true}
		}
		opts := new(options.FindOneAndUpdateOptions)
		opts = opts.SetReturnDocument(options.After)
		result := collection.FindOneAndUpdate(ctx, bson.M{"_id": node.ID}, tcond, opts)
		updatedNode := new(Node)
		err = result.Decode(updatedNode)
		if err != nil {
			entry.WithError(err).Error("updating record of node")
			return err
		}
		rebuilder.NodeManager.UpdateNode(updatedNode)
		if updatedNode.Status == 99 && oldNode.Status > 1 && oldNode.Status < 99 {
			//重建完毕，状态改为3，删除旧任务
			collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
			_, err = collectionRM.DeleteOne(ctx, bson.M{"_id": updatedNode.ID})
			if err != nil {
				entry.WithError(err).Error("delete rebuild miner with status 99")
			} else {
				entry.Info("all rebuild tasks finished")
			}
			rebuilder.lock.Lock()
			delete(rebuilder.taskAllocator, updatedNode.ID)
			rebuilder.lock.Unlock()
			return nil
		}
	} else {
		if node.Status == 2 || node.Status == 3 {
			opts := new(options.FindOneAndUpdateOptions)
			opts = opts.SetReturnDocument(options.After)
			result := collection.FindOneAndUpdate(ctx, bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"tasktimestamp": time.Now().Unix()}}, opts)
			updatedNode := new(Node)
			err = result.Decode(updatedNode)
			if err != nil {
				entry.WithError(err).Error("updating record of node")
				return err
			}
			node = updatedNode
		}
		rebuilder.NodeManager.UpdateNode(node)
	}
	// if node.Rebuilding > 8000 || node.Status > 1 || node.Valid == 0 || node.Weight < float64(rebuilder.Params.WeightThreshold) || node.AssignedSpace <= 0 || node.Quota <= 0 || node.Version < int32(rebuilder.Params.MinerVersionThreshold) {
	// 	entry.Debug("can not allocate rebuild task to current miner")
	// 	return nil
	// }
	// rebuilder.SendTask(ctx, node)
	if node.Rebuilding > 1 || node.Status != 1 || node.Valid == 0 || node.Weight < float64(rebuilder.Params.WeightThreshold) || node.AssignedSpace <= 0 || node.Quota <= 0 || node.Version < int32(rebuilder.Params.MinerVersionThreshold) {
		entry.Debug("can not allocate rebuild task to current miner")
		return nil
	}
	rebuilder.taskSender.AddToQueue(node)
	return nil
}

//Start starting rebuilding process
func (rebuilder *Rebuilder) Start(ctx context.Context) {
	//go rebuilder.Compensate(ctx)
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
		cur, err := collection.Find(ctx, bson.M{"status": bson.M{"$gt": 1, "$lt": 99}, "tasktimestamp": bson.M{"$exists": true, "$lt": time.Now().Unix() - int64(rebuilder.Params.RebuildableMinerTimeGap)}})
		if err != nil {
			entry.WithError(err).Error("cannot fetch rebuildable nodes")
			time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableMinerInterval) * time.Second)
			continue
		}
		for cur.Next(ctx) {
			cnt, err := collectionRM.CountDocuments(ctx, bson.M{"status": bson.M{"$lt": 99}})
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
			rangeTo := int64(0)
			shardFrom, err := FetchFirstNodeShard(ctx, rebuilder.tikvCli, node.ID)
			if err != nil {
				if err != NoValError {
					entry.WithField(MinerID, node.ID).WithError(err).Error("finding starting shard")
				} else {
					entry.WithField(MinerID, node.ID).WithError(err).Error("no shards for rebuilding")
					//没有分片可重建，直接将矿机状态改为99
					msg := &pb.RebuiltMessage{NodeID: node.ID}
					b, err := proto.Marshal(msg)
					if err != nil {
						entry.WithField(MinerID, node.ID).WithError(err).Error("marshaling RebuiltMessage failed")
					} else {
						snID := int(node.ID) % len(rebuilder.mqClis)
						ret := rebuilder.mqClis[snID].Send(ctx, fmt.Sprintf("sn%d", snID), append([]byte{byte(RebuiltMessage)}, b...))
						if !ret {
							entry.WithField(MinerID, node.ID).Warn("sending RebuiltMessage failed")
						}
						_, err = collection.UpdateOne(ctx, bson.M{"_id": node.ID}, bson.M{"$unset": bson.M{"tasktimestamp": ""}})
						if err != nil {
							entry.WithField(MinerID, node.ID).WithError(err).Error("unset tasktimestamp of rebuild miner")
						}
					}
				}
				continue
			} else {
				rangeFrom = shardFrom.ID
				entry.WithField(MinerID, node.ID).Debugf("rebuilding range starts at %d", rangeFrom)
			}
			shardTo, err := FetchLastNodeShard(ctx, rebuilder.tikvCli, node.ID)
			if err != nil {
				if err != NoValError {
					entry.WithField(MinerID, node.ID).WithError(err).Error("finding ending shard")
					//continue
				} else {
					entry.WithField(MinerID, node.ID).WithError(err).Error("no shards found")
					//continue
				}
				rangeTo = time.Now().Unix() << 32
			} else {
				rangeTo = shardTo.ID
				entry.WithField(MinerID, node.ID).Debugf("rebuilding range ends at %d", rangeTo)
			}
			segs := make([]int64, 0)
			//grids := make(map[int64]int64)
			grids := make([]int64, 0)
			gap := (rangeTo - rangeFrom) / int64(rebuilder.Params.MaxConcurrentTaskBuilderSize)
			if gap < 1000000000000 {
				segs = append(segs, rangeFrom, rangeTo+1)
				//grids[rangeFrom] = rangeFrom
				grids = append(grids, rangeFrom)
			} else {
				for i := rangeFrom; i < rangeTo; i += gap {
					segs = append(segs, i)
					//grids[i] = i
					grids = append(grids, i)
					entry.WithField(MinerID, node.ID).Debugf("spliting rebuilding range at %d", i)
				}
				segs = append(segs, rangeTo+1)
				entry.WithField(MinerID, node.ID).Debugf("spliting rebuilding range at %d", rangeTo+1)
			}

			miner := new(RebuildMiner)
			miner.ID = node.ID
			miner.Segs = segs
			miner.Grids = grids
			if node.Round == 0 { //|| node.Status == 3 {
				miner.Status = 3
			} else {
				miner.Status = 2
			}
			miner.Timestamp = time.Now().Unix()
			miner.FinishBuild = false
			_, err = collectionRM.InsertOne(ctx, miner)
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Error("insert rebuild miner to database")
			}
			_, err = collection.UpdateOne(ctx, bson.M{"_id": node.ID}, bson.M{"$unset": bson.M{"tasktimestamp": ""}, "$inc": bson.M{"round": 1}})
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Error("unset tasktimestamp of rebuild miner")
			}
			entry.WithField(MinerID, miner.ID).Info("ready for rebuilding")
		}
		cur.Close(ctx)
		time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableMinerInterval) * time.Second)
	}
}

func (rebuilder *Rebuilder) processRebuildableShard(ctx context.Context) {
	entry := log.WithFields(log.Fields{Function: "processRebuildableShard"})
	collection := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(NodeTab)
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	for {
		cur, err := collectionRM.Find(ctx, bson.M{"status": bson.M{"$gt": 1, "$lt": 99}})
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
			if !miner.FinishBuild {
				rebuilder.lock.RLock()
				_, ok := rebuilder.taskAllocator[miner.ID]
				rebuilder.lock.RUnlock()
				if !ok {
					rebuilder.lock.Lock()
					if _, ok := rebuilder.taskAllocator[miner.ID]; !ok {
						rebuilder.taskAllocator[miner.ID] = &TaskChan{ch: make(chan *RebuildShard, rebuilder.Params.RebuildShardTaskBatchSize), close: false}
						entry.WithField(MinerID, miner.ID).Debugf("create task allocator, length is %d", rebuilder.Params.RebuildShardTaskBatchSize)
						go rebuilder.Processing(ctx, miner)
					}
					rebuilder.lock.Unlock()
				}
			} else {
				rebuilder.lock2.RLock()
				checkpoints := make([]int64, 0, len(rebuilder.checkPoints))
				for _, value := range rebuilder.checkPoints {
					checkpoints = append(checkpoints, value)
				}
				rebuilder.lock2.RUnlock()
				checkpoint := Min(checkpoints...)
				if time.Now().UnixNano() > miner.ExpiredTime && checkpoint > miner.ExpiredTime {
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
						_, err = collection.UpdateOne(ctx, bson.M{"_id": miner.ID, "status": bson.M{"$ne": 1}}, bson.M{"$set": bson.M{"tasktimestamp": time.Now().Unix() - int64(rebuilder.Params.RebuildableMinerTimeGap)}})
						if err != nil {
							entry.WithField(MinerID, miner.ID).WithError(err).Error("restart rebuild miner")
						}
					}
				}
			}
		}
		cur.Close(ctx)
		time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableShardInterval) * time.Second)
	}
}

//Processing filling buffered channel with rebuild tasks
func (rebuilder *Rebuilder) Processing(ctx context.Context, miner *RebuildMiner) {
	entry := log.WithFields(log.Fields{Function: "Processing", MinerID: miner.ID})
	entry.Infof("creating task buffer for miner %+v", miner)
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	segs := miner.Segs
	grids := miner.Grids
	wg := sync.WaitGroup{}
	wg.Add(len(grids))
	for i := 0; i < len(segs)-1; i++ {
		index := i
		begin := segs[i]
		from := grids[i]
		to := segs[i+1]
		if from == -1 {
			wg.Done()
			continue
		}
		go func() {
			entry.WithField("Grid", index).Infof("starting goroutine from %d to %d, checkpoint is %d", begin, to, from)
			defer wg.Done()
			for {
				rebuildShards, err := FetchNodeShards(ctx, rebuilder.tikvCli, miner.ID, from, to, int64(rebuilder.Params.RebuildShardMinerTaskBatchSize)*10)
				if err != nil {
					entry.WithField("Grid", index).WithError(err).Error("fetching shard-rebuilding tasks for caching")
					time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableShardInterval) * time.Second)
					continue
				}
				if len(rebuildShards) == 0 {
					entry.WithField("Grid", index).Infof("finished shards from %d to %d", begin, to)
					_, err = collectionRM.UpdateOne(ctx, bson.M{"_id": miner.ID}, bson.M{"$set": bson.M{fmt.Sprintf("grids.%d", index): -1}})
					if err != nil {
						entry.WithField("Grid", index).WithError(err).Errorf("change grids %d failed: %d", begin, from)
					}
					break
				}
				entry.WithField("Grid", index).Debugf("fetching %d shards from %d to %d", len(rebuildShards), from, to)
				tasks, err := rebuilder.buildTasks(ctx, rebuildShards, miner)
				if err != nil {
					entry.WithField("Grid", index).WithError(err).Error("building tasks")
					time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableShardInterval) * time.Second)
					continue
				}
				entry.WithField("Grid", index).Debugf("build %d tasks from %d to %d", len(tasks), from, to)
				oldfrom := from
				from = rebuildShards[len(rebuildShards)-1].ID + 1
				_, err = collectionRM.UpdateOne(ctx, bson.M{"_id": miner.ID}, bson.M{"$set": bson.M{fmt.Sprintf("grids.%d", index): from}})
				if err != nil {
					entry.WithField("Grid", index).WithError(err).Errorf("change value of grid.%d failed: %d -> %d", index, oldfrom, from)
				}
				entry.WithField("Grid", index).Debugf("change value of grid.%d: %d -> %d", index, oldfrom, from)
				rebuilder.lock.RLock()
			OUTER:
				for _, task := range tasks {
					select {
					case rebuilder.taskAllocator[miner.ID].ch <- task:
						entry.WithField("Grid", index).Debugf("insert shard %d to chan: nodeid -> %d/%d", task.ID, task.MinerID, task.MinerID2)
						continue
					default:
						oldfrom = from
						from = task.ID
						_, err = collectionRM.UpdateOne(ctx, bson.M{"_id": miner.ID}, bson.M{"$set": bson.M{fmt.Sprintf("grids.%d", index): from}})
						if err != nil {
							entry.WithField("Grid", index).WithError(err).Errorf("task allocator is fullfilled, interrupt goroutine%d: %d -> %d", index, oldfrom, to)
						}
						entry.WithField("Grid", index).Debugf("task allocator is fullfilled, interrupt goroutine%d: %d -> %d", index, oldfrom, from)
						time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableShardInterval) * time.Second)
						break OUTER
					}
				}
				rebuilder.lock.RUnlock()
			}
		}()
	}
	wg.Wait()
	//TODO: 全部分片发送完成
	rebuilder.lock.RLock()
	if _, ok := rebuilder.taskAllocator[miner.ID]; ok {
		entry.Info("close task chan")
		close(rebuilder.taskAllocator[miner.ID].ch)
	}
	rebuilder.lock.RUnlock()
}

func (rebuilder *Rebuilder) buildTasks(ctx context.Context, shards []*Shard, miner *RebuildMiner) ([]*RebuildShard, error) {
	entry := log.WithFields(log.Fields{Function: "buildTasks"})
	rebuildShards := make([]*RebuildShard, 0)
	var blocksMap map[uint64]*ytab.Block
	if miner.Status == 2 {
		bindexes := make([]uint64, 0)
		for _, s := range shards {
			bindexes = append(bindexes, s.BIndex)
		}
		var err error
		blocksMap, err = GetBlocks(rebuilder.httpCli, rebuilder.Compensation.SyncClientURL, bindexes)
		if err != nil {
			entry.WithError(err).Error("fetching blocks")
			return nil, err
		}
	}
OUTER:
	for _, shard := range shards {
		rshard := new(RebuildShard)
		rshard.ID = shard.ID
		rshard.BlockID = shard.ID - int64(shard.Offset)
		rshard.MinerID = shard.NodeID
		rshard.MinerID2 = shard.NodeID2
		rshard.VHF = shard.VHF
		//rshard.VNF = block.VNF
		rshard.SNID = snIDFromID(uint64(shard.ID))
		//rshard.SNID = block.SNID
		var block *ytab.Block
		var err error
		if miner.Status == 2 {
			block = blocksMap[shard.BIndex]
			if block == nil {
				entry.WithField(ShardID, shard.ID).Warn("block of shard have deleted")
				continue
			}
			// block, err = FetchBlock(ctx, rebuilder.tikvCli, shard.BlockID)
			// if err != nil {
			// 	entry.WithField(ShardID, shard.ID).WithError(err).Error("fetching block")
			// 	return nil, err
			// }
			entry.Debugf("fetch block %d for shard %d", block.ID, shard.ID)
			rshard.BlockID = int64(block.ID)
			if block.AR == -2 {
				rshard.Type = 0xc258
				rshard.ParityShardCount = int32(block.VNF)
			} else if block.AR > 0 {
				rshard.Type = 0x68b3
				rshard.ParityShardCount = int32(block.VNF) - int32(block.AR)
			}
			rshard.VNF = int32(block.VNF)
		} else if miner.Status == 3 {
			// if block.AR == -2 {
			// 	rshard.Type = 0xc258
			// 	rshard.ParityShardCount = block.VNF
			// } else if block.AR > 0 {
			rshard.Type = 0xc258
			if shard.NodeID2 == 0 {
				rshard.ParityShardCount = 1
				rshard.VNF = 1
			} else {
				rshard.ParityShardCount = 2
				rshard.VNF = 2
			}
			//}
		}
		var siblingShards []*Shard
		hashs := make([][]byte, 0)
		nodeIDs := make([]*NodePair, 0)
		if miner.Status == 2 { //|| (miner.Status == 3 && rshard.Type == 0xc258 && rshard.ParityShardCount > 2) {
			for i, s := range block.Shards {
				siblingShards = append(siblingShards, &Shard{ID: int64(block.ID) + int64(i), VHF: s.VHF, BIndex: shard.BIndex, Offset: uint8(i), NodeID: int32(s.NodeID), NodeID2: int32(s.NodeID2)})
			}
			//if block.Shards != nil {
			//siblingShards = block.Shards
			// } else {
			// 	siblingShards, err = FetchShards(ctx, rebuilder.tikvCli, rshard.BlockID, rshard.BlockID+int64(rshard.VNF))
			// 	if err != nil {
			// 		entry.WithField(ShardID, rshard.ID).WithError(err).Error("fetching sibling shards failed")
			// 		return nil, err
			// 	}
			// }
			entry.Debugf("fetch %d sibling shards for shard %d", len(siblingShards), rshard.ID)

			i := rshard.BlockID
			needHash := false
			if rshard.Type == 0x68b3 {
				needHash = true
			}
			for _, s := range siblingShards {
				entry.WithField(ShardID, shard.ID).Debugf("decode sibling shard info %d: %d", i, s.ID)
				if s.ID != i {
					entry.WithField(ShardID, shard.ID).Errorf("sibling shard %d not found: %d", i, s.ID)
					continue OUTER
				}
				if needHash {
					hashs = append(hashs, s.VHF)
				}
				nodeIDs = append(nodeIDs, &NodePair{s.NodeID, s.NodeID2})
				// node1 := rebuilder.NodeManager.GetNode(s.NodeID)
				// node2 := rebuilder.NodeManager.GetNode(s.NodeID2)
				// if node1 != nil && node1.Timestamp > time.Now().Unix()-600 && node1.Status == 1 && node1.Valid == 1 {
				// 	nodeIDs = append(nodeIDs, &NodePair{s.NodeID, s.NodeID2})
				// } else if node2 != nil && node2.Timestamp > time.Now().Unix()-600 && node2.Status == 1 && node2.Valid == 1 {
				// 	nodeIDs = append(nodeIDs, s.NodeID2)
				// } else {
				// 	nodeIDs = append(nodeIDs, s.NodeID)
				// }
				i++
			}
			if len(nodeIDs) != int(rshard.VNF) {
				entry.WithField(ShardID, shard.ID).WithError(err).Errorf("count of sibling shard is %d, not equal to VNF %d", len(hashs), rshard.VNF)
				continue
			}
			rshard.Hashs = hashs
			rshard.NodeIDs = nodeIDs
		} else if miner.Status == 3 {
			//hashs = append(hashs, shard.VHF)
			if miner.ID == shard.NodeID {
				if shard.NodeID2 != 0 {
					//hashs = append(hashs, shard.VHF)
					nodeIDs = append(nodeIDs, &NodePair{shard.NodeID2, 0})
				}
				nodeIDs = append(nodeIDs, &NodePair{shard.NodeID, 0})
			} else if miner.ID == shard.NodeID2 {
				nodeIDs = append(nodeIDs, &NodePair{shard.NodeID, 0})
				if shard.NodeID2 != 0 {
					//hashs = append(hashs, shard.VHF)
					nodeIDs = append(nodeIDs, &NodePair{shard.NodeID2, 0})
				}
			}
			//nodeIDs = append(nodeIDs, &NodePair{shard.NodeID, shard.NodeID2})
			rshard.Hashs = hashs
			rshard.NodeIDs = nodeIDs
		}

		rebuildShards = append(rebuildShards, rshard)
	}
	return rebuildShards, nil
}

func (rebuilder *Rebuilder) reaper(ctx context.Context) {
	entry := log.WithFields(log.Fields{Function: "reaper"})
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	for {
		cur, err := collectionRM.Find(ctx, bson.M{"status": bson.M{"$gt": 1, "$lt": 99}, "finishBuild": true})
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
			rebuilder.lock.Lock()
			delete(rebuilder.taskAllocator, miner.ID)
			rebuilder.lock.Unlock()

		}
		time.Sleep(time.Duration(rebuilder.Params.ProcessReaperInterval) * time.Second)
	}
}

//GetRebuildTasks get rebuild tasks
func (rebuilder *Rebuilder) GetRebuildTasks(ctx context.Context, id int32) (*pb.MultiTaskDescription, error) {
	entry := log.WithFields(log.Fields{Function: "GetRebuildTasks", RebuilderID: id})
	entry.Debug("ready for fetching rebuildable tasks")
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	rbNode := rebuilder.NodeManager.GetNode(id)
	if rbNode == nil {
		err := fmt.Errorf("node %d not found", id)
		entry.WithError(err).Error("fetch rebuilder miner")
		return nil, err
	}
	entry.Debugf("fetch rebuilding node %d", rbNode.ID)
	startTime := time.Now().UnixNano()
	//如果被分配重建任务的矿机状态大于1或者权重为零则不分配重建任务
	if rbNode.Rebuilding > 1 || rbNode.Status != 1 || rbNode.Valid == 0 || rbNode.Weight < float64(rebuilder.Params.WeightThreshold) || rbNode.AssignedSpace <= 0 || rbNode.Quota <= 0 || rbNode.Version < int32(rebuilder.Params.MinerVersionThreshold) {
		//err := fmt.Errorf("no tasks can be allocated to miner %d", id)
		entry.WithError(ErrInvalidNode).Debugf("status of rebuilder miner is %d, weight is %f, rebuilding is %d, version is %d", rbNode.Status, rbNode.Weight, rbNode.Rebuilding, rbNode.Version)
		return nil, ErrInvalidNode
	}
	rbshards := make([]*RebuildShard, 0)
	i := 0
	rebuilder.lock.RLock()
	var minerID int32 = 0
OUTER:
	for mid, taskChan := range rebuilder.taskAllocator {
		minerID = mid
		if taskChan.close {
			continue
		}
		entry.Debugf("fetch tasks from task allocator %d", minerID)
		for {
			select {
			case t, ok := <-taskChan.ch:
				if !ok {
					entry.Debugf("task allocator %d is closed", minerID)
					taskChan.close = true
					_, err := collectionRM.UpdateOne(ctx, bson.M{"_id": minerID}, bson.M{"$set": bson.M{"finishBuild": true, "expiredTime": time.Now().UnixNano() + int64(rebuilder.Params.RebuildShardExpiredTime)*1000000000}})
					if err != nil {
						entry.WithError(err).WithField(MinerID, minerID).Error("change finishBuild failed")
					}
					if i > 0 {
						break OUTER
					} else {
						continue OUTER
					}
				}
				// if t.Type == 0x68b3 {
				// 	entry.Debugf("append rebuild task for miner %d: ID %d, type LRC2", minerID, t.ID)
				// } else if t.Type == 0xc258 {
				// 	entry.Debugf("append rebuild task for miner %d: ID %d, type Replication", minerID, t.ID)
				// }
				rbshards = append(rbshards, t)
				i++
				if i == rebuilder.Params.RebuildShardMinerTaskBatchSize {
					break OUTER
				}
			default:
				entry.WithField(MinerID, minerID).Debugf("length of chan: %d", len(taskChan.ch))
				if i > 0 {
					break OUTER
				} else {
					continue OUTER
				}
			}
		}
	}
	rebuilder.lock.RUnlock()
	if len(rbshards) == 0 {
		//err := errors.New("no tasks can be allocated")
		entry.Warn(ErrNoTaskAlloc)
		return nil, ErrNoTaskAlloc
	}
	//任务超时时间（绝对时间），为当前时间加上预设的任务超时时间，单位是秒
	expiredTime := time.Now().Unix() + int64(rebuilder.Params.RebuildShardExpiredTime)
	//一个任务包，多个分片的重建任务均在该任务包内
	tasks := new(pb.MultiTaskDescription)
	//rbshards为RebuildShard结构体数组
	for _, shard := range rbshards {
		entry.Debugf("fetch shard %d: nodeId %d/%d", shard.ID, shard.MinerID, shard.MinerID2)
		//被重建分片的全部关联分片（包括自身）的MD5哈希（仅当status=2且分片类型为0x68b3，即LRC分片时才需要该值，否则为空）
		hashs := shard.Hashs
		//所有关联分片所在矿机的P2P地址（只使用NodeID，没有NodeID2的，为和旧代码兼容）
		locations := make([]*pb.P2PLocation, 0)
		for idx, id := range shard.NodeIDs {
			n1 := rebuilder.NodeManager.GetNode(id.NodeID1)
			n2 := rebuilder.NodeManager.GetNode(id.NodeID2)
			loc := new(pb.P2PLocation)
			if n1 == nil {
				//如果矿机不存在则制造一个假地址
				entry.Debugf("invalid node1 for shard %d/%d: nodeId %d", shard.ID, idx, id.NodeID1)
				loc.NodeId = "16Uiu2HAmKg7EXBqx3SXbE2XkqbPLft8NGkzQcsbJymVB9uw7fW1r"
				loc.Addrs = []string{"/ip4/127.0.0.1/tcp/59999"}
			} else {
				loc.NodeId = n1.NodeID
				loc.Addrs = n1.Addrs
			}
			if n2 == nil {
				//如果矿机不存在则制造一个假地址
				entry.Debugf("invalid node2 for shard %d/%d: nodeId %d", shard.ID, idx, id.NodeID2)
				loc.NodeId2 = "16Uiu2HAmKg7EXBqx3SXbE2XkqbPLft8NGkzQcsbJymVB9uw7fW1r"
				loc.Addrs2 = []string{"/ip4/127.0.0.1/tcp/59999"}
			} else {
				loc.NodeId2 = n2.NodeID
				loc.Addrs2 = n2.Addrs
			}
			locations = append(locations, loc)
		}
		//关联分片数不正确则跳过该分片的重建
		if (shard.Type == 0x68b3 && len(locations) < int(shard.VNF)) || (shard.Type == 0xc258 && len(locations) == 0) {
			entry.WithField(MinerID, shard.MinerID).WithField(ShardID, shard.ID).Warnf("sibling shards are not enough, only %d shards", len(hashs))
			continue
		}
		var b []byte
		var err error
		if shard.Type == 0x68b3 { //LRC2重建部分
			task := new(pb.TaskDescription)
			//拼接任务ID
			task.Id = append(Int64ToBytes(shard.ID), Uint16ToBytes(uint16(shard.SNID))...)
			//关联分片哈希
			task.Hashs = hashs
			//status=3且被重建分片没有nodeID2时该值为1，有nodeID2时为2（即副本数）；status=2时若分片类型为0x68b3则该值为分片所属block的VNF减去AR，分片类型为0xc258时该值为分片所属block的VNF
			task.ParityShardCount = shard.ParityShardCount
			//分片在block中的索引值
			task.RecoverId = int32(shard.ID - shard.BlockID)
			//所有关联分片所在矿机的P2P地址
			task.Locations = locations
			//如果存在nodeID2，即LRC2重建类型
			if shard.MinerID2 != 0 {
				if minerID == shard.MinerID { //如果被重建分片的NodeID为被重建矿机，则使用NodeID2作为重建备份节点
					backNode := rebuilder.NodeManager.GetNode(shard.MinerID2)
					if backNode != nil {
						task.BackupLocation = &pb.P2PLocation{NodeId: backNode.NodeID, Addrs: backNode.Addrs}
					}
				} else if minerID == shard.MinerID2 { //如果被重建分片的NodeID2为被重建矿机，则使用NodeID作为重建备份节点
					backNode := rebuilder.NodeManager.GetNode(shard.MinerID)
					if backNode != nil {
						task.BackupLocation = &pb.P2PLocation{NodeId: backNode.NodeID, Addrs: backNode.Addrs}
					}
				} else {
					entry.Errorf("neither MinerID nor MinerID2 match rebuilder ID for shard %d", shard.ID)
				}
			}
			//序列化任务为字节数组
			b, err = proto.Marshal(task)
			if err != nil {
				entry.WithField(MinerID, shard.MinerID).WithField(ShardID, shard.ID).WithError(err).Errorf("marshaling LRC task: %d", shard.ID)
				continue
			}
		} else if shard.Type == 0xc258 { //多副本重建部分
			task := new(pb.TaskDescriptionCP)
			//拼接任务ID
			task.Id = append(Int64ToBytes(shard.ID), Uint16ToBytes(uint16(shard.SNID))...)
			//被重建分片的MD5哈希
			task.DataHash = shard.VHF
			//被重建分片的全部副本所在Node的地址列表
			task.Locations = locations
			//序列化任务为字节数组
			b, err = proto.Marshal(task)
			if err != nil {
				entry.WithField(MinerID, shard.MinerID).WithField(ShardID, shard.ID).WithError(err).Errorf("marshaling replication task: %d", shard.ID)
				continue
			}
		}
		//将分片类型加到重建数据前面
		btask := append(Uint16ToBytes(uint16(shard.Type)), b...)
		//将组装好的一个重建任务加入任务包
		tasks.Tasklist = append(tasks.Tasklist, btask)
	}
	//设置超时时间
	tasks.ExpiredTime = expiredTime
	//设置分片所属原矿机ID
	tasks.SrcNodeID = minerID
	//设置超时时间（相对时间，单位为秒）
	tasks.ExpiredTimeGap = int32(rebuilder.Params.RebuildShardExpiredTime)
	entry.WithField(MinerID, minerID).Debugf("length of task list is %d, expired time is %d, total time: %dms", len(tasks.Tasklist), tasks.ExpiredTime, (time.Now().UnixNano()-startTime)/1000000)
	return tasks, nil
}

//UpdateTaskStatus update task status
func (rebuilder *Rebuilder) UpdateTaskStatus(ctx context.Context, result *pb.MultiTaskOpResult) error {
	nodeID := result.NodeID
	srcNodeID := result.SrcNodeID
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
	for i, id := range ids {
		if rets[i] == 0 {
			entry.WithField(ShardID, id).Debug("task rebuilt success")
		} else {
			entry.WithField(ShardID, id).Debug("task rebuilt failed")
		}

	}
	return nil
}

func GetBlocksRetries(httpCli *http.Client, syncClientURL string, bindexes []uint64, retries int) (map[uint64]*ytab.Block, error) {
	//entry := log.WithFields(log.Fields{Function: "GetBlocksRetries"})
	blocks, err := GetBlocks(httpCli, syncClientURL, bindexes)
	if err != nil {
		if retries == 0 {
			return blocks, err
		} else {
			return GetBlocksRetries(httpCli, syncClientURL, bindexes, retries-1)
		}
	} else {
		return blocks, err
	}
}

//GetBlocks get blocks from sync client service
func GetBlocks(httpCli *http.Client, syncClientURL string, bindexes []uint64) (map[uint64]*ytab.Block, error) {
	entry := log.WithFields(log.Fields{Function: "GetBlocks"})
	strs := make([]string, 0)
	for _, v := range bindexes {
		strs = append(strs, fmt.Sprintf("%d", v))
	}
	fullURL := fmt.Sprintf("%s/getBlocks?bindexes=%s", syncClientURL, strings.Join(strs, ","))
	entry.Debugf("fetching blocks by URL: %s", fullURL)
	request, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		entry.WithError(err).Errorf("create request failed: %s", fullURL)
		return nil, err
	}
	request.Header.Add("Accept-Encoding", "gzip")
	resp, err := httpCli.Do(request)
	if err != nil {
		entry.WithError(err).Errorf("get checkpoints failed: %s", fullURL)
		return nil, err
	}
	defer resp.Body.Close()
	reader := io.Reader(resp.Body)
	if strings.Contains(resp.Header.Get("Content-Encoding"), "gzip") {
		gbuf, err := gzip.NewReader(reader)
		if err != nil {
			entry.WithError(err).Errorf("decompress response body: %s", fullURL)
			return nil, err
		}
		reader = io.Reader(gbuf)
		defer gbuf.Close()
	}
	response := make(map[uint64]*ytab.Block, 0)
	err = json.NewDecoder(reader).Decode(&response)
	if err != nil {
		entry.WithError(err).Errorf("decode blocks failed: %s", fullURL)
		return nil, err
	}
	return response, nil
}

// func FetchBlock(ctx context.Context, tikvCli *rawkv.Client, blockID int64) (*Block, error) {
// 	buf, err := tikvCli.Get(ctx, []byte(fmt.Sprintf("%s_%019d", PFX_BLOCKS, blockID)))
// 	if err != nil {
// 		return nil, err
// 	}
// 	block := new(Block)
// 	err = block.FillBytes(buf)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return block, nil
// }

// func FetchShard(ctx context.Context, tikvCli *rawkv.Client, shardID int64) (*Shard, error) {
// 	buf, err := tikvCli.Get(ctx, []byte(fmt.Sprintf("%s_%019d", PFX_SHARDS, shardID)))
// 	if err != nil {
// 		return nil, err
// 	}
// 	shard := new(Shard)
// 	err = shard.FillBytes(buf)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return shard, nil
// }

// func FetchBlocks(ctx context.Context, tikvCli *rawkv.Client, blockFrom, blockTo int64, limit int64) ([]*Block, error) {
// 	entry := log.WithFields(log.Fields{Function: "FetchBlocks", BlockID: blockFrom, "Limit": limit})
// 	batchSize := int64(10000)
// 	from := fmt.Sprintf("%019d", blockFrom)
// 	to := fmt.Sprintf("%019d", blockTo)
// 	blocks := make([]*Block, 0)
// 	cnt := int64(0)
// 	for {
// 		lmt := batchSize
// 		if cnt+batchSize-limit > 0 {
// 			lmt = limit - cnt
// 		}
// 		_, values, err := tikvCli.Scan(ctx, []byte(fmt.Sprintf("%s_%s", PFX_BLOCKS, from)), []byte(fmt.Sprintf("%s_%s", PFX_BLOCKS, to)), int(lmt))
// 		if err != nil {
// 			return nil, err
// 		}
// 		if len(values) == 0 {
// 			break
// 		}
// 		for _, buf := range values {
// 			b := new(Block)
// 			err := b.FillBytes(buf)
// 			if err != nil {
// 				return nil, err
// 			}
// 			blocks = append(blocks, b)
// 		}
// 		from = fmt.Sprintf("%019d", blocks[len(blocks)-1].ID+1)
// 		cnt += int64(len(values))
// 	}
// 	entry.Debugf("fetch %d shards", len(blocks))
// 	return blocks, nil
// }

func FetchNodeShards(ctx context.Context, tikvCli *rawkv.Client, nodeId int32, shardFrom, shardTo int64, limit int64) ([]*Shard, error) {
	entry := log.WithFields(log.Fields{Function: "FetchNodeShards", MinerID: nodeId, ShardID: shardFrom, "Limit": limit})
	batchSize := int64(10000)
	from := fmt.Sprintf("%019d", shardFrom)
	to := fmt.Sprintf("%019d", shardTo)
	shards := make([]*Shard, 0)
	cnt := int64(0)
	for {
		lmt := batchSize
		if cnt+batchSize-limit > 0 {
			lmt = limit - cnt
		}
		_, values, err := tikvCli.Scan(ctx, []byte(fmt.Sprintf("%s_%d_%s", PFX_SHARDNODES, nodeId, from)), []byte(fmt.Sprintf("%s_%d_%s", PFX_SHARDNODES, nodeId, to)), int(lmt))
		if err != nil {
			return nil, err
		}
		if len(values) == 0 {
			break
		}
		for _, buf := range values {
			s := new(Shard)
			err := s.FillBytes(buf)
			if err != nil {
				return nil, err
			}
			if s.NodeID != nodeId && s.NodeID2 != nodeId {
				continue
			}
			shards = append(shards, s)
		}
		from = fmt.Sprintf("%019d", shards[len(shards)-1].ID+1)
		cnt += int64(len(values))
	}
	entry.Debugf("fetch %d shards", len(shards))
	return shards, nil
}

// func FetchShards(ctx context.Context, tikvCli *rawkv.Client, shardFrom int64, shardTo int64) ([]*Shard, error) {
// 	_, values, err := tikvCli.Scan(ctx, []byte(fmt.Sprintf("%s_%s", PFX_SHARDS, fmt.Sprintf("%019d", shardFrom))), []byte(fmt.Sprintf("%s_%s", PFX_SHARDS, fmt.Sprintf("%019d", shardTo))), 164)
// 	if err != nil {
// 		return nil, err
// 	}
// 	shards := make([]*Shard, 0)
// 	for _, buf := range values {
// 		s := new(Shard)
// 		err := s.FillBytes(buf)
// 		if err != nil {
// 			return nil, err
// 		}
// 		shards = append(shards, s)
// 	}
// 	return shards, nil
// }

func FetchFirstNodeShard(ctx context.Context, tikvCli *rawkv.Client, nodeId int32) (*Shard, error) {
	shards, err := FetchNodeShards(ctx, tikvCli, nodeId, 0, 9223372036854775807, 1)
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
	to := "9223372036854775807"
	_, values, err := tikvCli.ReverseScan(ctx, append([]byte(fmt.Sprintf("%s_%d_%s", PFX_SHARDNODES, nodeId, to)), '\x00'), append([]byte(fmt.Sprintf("%s_%d_%s", PFX_SHARDNODES, nodeId, from)), '\x00'), 1)
	if err != nil {
		return nil, err
	}
	shards := make([]*Shard, 0)
	for _, buf := range values {
		s := new(Shard)
		err := s.FillBytes(buf)
		if err != nil {
			return nil, err
		}
		if s.NodeID != nodeId && s.NodeID2 != nodeId {
			continue
		}
		shards = append(shards, s)
	}
	if len(shards) == 0 {
		return nil, NoValError
	}
	return shards[0], nil
}

// func BuildTasks2(ctx context.Context, tikvCli *rawkv.Client, shards []*Shard) ([]*RebuildShard, error) {
// 	entry := log.WithFields(log.Fields{Function: "buildTasks"})
// 	rebuildShards := make([]*RebuildShard, 0)
// OUTER:
// 	for _, shard := range shards {
// 		block, err := FetchBlock(ctx, tikvCli, shard.BlockID)
// 		if err != nil {
// 			entry.WithField(ShardID, shard.ID).WithError(err).Error("fetching block")
// 			return nil, err
// 		}
// 		rshard := new(RebuildShard)
// 		rshard.ID = shard.ID
// 		rshard.BlockID = shard.BlockID
// 		rshard.MinerID = shard.NodeID
// 		rshard.VHF = shard.VHF
// 		rshard.VNF = block.VNF
// 		rshard.SNID = block.SNID
// 		if block.AR == -2 {
// 			rshard.Type = 0xc258
// 			rshard.ParityShardCount = block.VNF
// 		} else if block.AR > 0 {
// 			rshard.Type = 0x68b3
// 			rshard.ParityShardCount = block.VNF - block.AR
// 		}
// 		siblingShards := block.Shards // FetchShards(ctx, tikvCli, rshard.BlockID, rshard.BlockID+int64(rshard.VNF))
// 		if err != nil {
// 			entry.WithField(ShardID, rshard.ID).WithError(err).Error("fetching sibling shards failed")
// 			return nil, err
// 		} else {
// 			hashs := make([][]byte, 0)
// 			nodeIDs := make([]int32, 0)
// 			i := rshard.BlockID
// 			for _, s := range siblingShards {
// 				entry.WithField(ShardID, shard.ID).Tracef("decode sibling shard info %d", i)
// 				if s.ID != i {
// 					entry.WithField(ShardID, shard.ID).Errorf("sibling shard %d not found: %d", i, s.ID)
// 					continue OUTER
// 				}
// 				hashs = append(hashs, s.VHF)
// 				nodeIDs = append(nodeIDs, s.NodeID)
// 				i++
// 			}
// 			if len(hashs) != int(rshard.VNF) {
// 				entry.WithField(ShardID, shard.ID).WithError(err).Errorf("count of sibling shard is %d, not equal to VNF %d", len(hashs), rshard.VNF)
// 				continue
// 			}
// 			rshard.Hashs = hashs
// 			rshard.NodeIDs = nodeIDs
// 		}
// 		rebuildShards = append(rebuildShards, rshard)
// 	}
// 	return rebuildShards, nil
// }

//FindShardMetas get shard metas by ShardRebuildMeta
func FindShardMeta(ctx context.Context, tikvCli *rawkv.Client, blockID uint64) (*pb.ShardMetaMsg, error) {
	buf, err := tikvCli.Get(ctx, []byte(fmt.Sprintf("shardmeta_%019d", blockID)))
	if err != nil {
		return nil, err
	}
	msg := new(pb.ShardMetaMsg)
	err = proto.Unmarshal(buf, msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func FindNodeShard(ctx context.Context, tikvCli *rawkv.Client, shardID uint64, nodeID uint32) (*pb.ShardMsg, error) {
	buf, err := tikvCli.Get(ctx, []byte(fmt.Sprintf("%s_%d_%019d", PFX_SHARDNODES, nodeID, shardID)))
	if err != nil {
		return nil, err
	}
	msg := new(pb.ShardMsg)
	err = proto.Unmarshal(buf, msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}
