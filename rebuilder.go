package ytrebuilder

import (
	"context"
	"errors"
	"fmt"
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
	Compensation      *CompensationConfig
	Params            *MiscConfig
	mqClis            map[int]*ytsync.Service
	httpCli           *http.Client
	lock              sync.RWMutex
	taskAllocator     map[int32]*TaskChan
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
	rebuilder := &Rebuilder{tikvCli: tikvCli, rebuilderdbClient: rebuilderdbClient, NodeManager: nodeMgr, Compensation: cpsConf, Params: conf, httpCli: &http.Client{}, taskAllocator: make(map[int32]*TaskChan)}
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
			_, err = collectionRM.UpdateOne(ctx, bson.M{"_id": updatedNode.ID}, bson.M{"$set": bson.M{"from": 0, "to": 0, "status": 3}})
			if err != nil {
				entry.WithError(err).Error("update rebuild miner status to 3")
			} else {
				entry.Info("all rebuild tasks finished")
			}
			rebuilder.lock.Lock()
			delete(rebuilder.taskAllocator, updatedNode.ID)
			rebuilder.lock.Unlock()
			return nil
		}
	} else {
		rebuilder.NodeManager.UpdateNode(node)
	}
	rebuilder.SendTask(ctx, node)
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
			rangeTo := int64(0)
			shardFrom, err := FetchFirstNodeShard(ctx, rebuilder.tikvCli, node.ID)
			if err != nil {
				if err != NoValError {
					entry.WithField(MinerID, node.ID).WithError(err).Error("finding starting shard")
				} else {
					entry.WithField(MinerID, node.ID).WithError(err).Error("no shards for rebuilding")
					//没有分片可重建，直接将矿机状态改为3
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
			}
			shardTo, err := FetchLastNodeShard(ctx, rebuilder.tikvCli, node.ID)
			if err != nil {
				if err != NoValError {
					entry.WithField(MinerID, node.ID).WithError(err).Error("finding ending shard")
					continue
				}
			} else {
				rangeTo = shardTo.ID
			}
			segs := make([]int64, 0)
			grids := make(map[int64]int64)
			gap := (rangeTo - rangeFrom) / 200
			if gap < 1000000000000 {
				segs = append(segs, rangeFrom, rangeTo+1)
				grids[rangeFrom] = rangeFrom
			} else {
				for i := rangeFrom; i < rangeTo; i += gap {
					segs = append(segs, i)
					grids[i] = i
				}
				segs = append(segs, rangeTo+1)
			}

			miner := new(RebuildMiner)
			miner.ID = node.ID
			miner.Segs = segs
			miner.Grids = grids
			miner.Status = node.Status
			miner.Timestamp = time.Now().Unix()
			miner.FinishBuild = false
			_, err = collectionRM.InsertOne(ctx, miner)
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Error("insert rebuild miner to database")
			}
			_, err = collection.UpdateOne(ctx, bson.M{"_id": node.ID}, bson.M{"$unset": bson.M{"tasktimestamp": ""}})
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
			if !miner.FinishBuild {
				rebuilder.lock.Lock()
				if _, ok := rebuilder.taskAllocator[miner.ID]; !ok {
					rebuilder.taskAllocator[miner.ID] = &TaskChan{ch: make(chan *RebuildShard, rebuilder.Params.RebuildShardTaskBatchSize), close: false}
					go rebuilder.Processing(ctx, miner)
				}
				rebuilder.lock.Unlock()
			} else {
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
						_, err = collection.UpdateOne(ctx, bson.M{"_id": miner.ID}, bson.M{"$set": bson.M{"tasktimestamp": time.Now().Unix() - int64(rebuilder.Params.RebuildableMinerTimeGap)}})
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
	entry.Info("creating task buffer")
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	segs := miner.Segs
	grids := miner.Grids
	wg := sync.WaitGroup{}
	wg.Add(len(grids))
	for i := 0; i < len(segs)-1; i++ {
		index := i
		begin := segs[i]
		from := grids[begin]
		to := segs[i+1]
		if from == -1 {
			wg.Done()
			continue
		}
		go func() {
			entry.Infof("starting goroutine%d from %d to %d, checkpoint is %d", index, begin, to, from)
			defer wg.Done()
			for {
				rebuildShards, err := FetchNodeShards(ctx, rebuilder.tikvCli, miner.ID, from, to, int64(rebuilder.Params.RebuildShardMinerTaskBatchSize)*100)
				if err != nil {
					entry.WithError(err).Error("fetching shard-rebuilding tasks for caching")
					time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableShardInterval) * time.Second)
					continue
				}
				if len(rebuildShards) == 0 {
					entry.Infof("finished goroutine%d from %d to %d", index, begin, to)
					_, err = collectionRM.UpdateOne(ctx, bson.M{"_id": miner.ID}, bson.M{"$set": bson.M{fmt.Sprintf("grids.%d", begin): -1}})
					if err != nil {
						entry.WithError(err).Errorf("change grids %d failed: %d", begin, from)
					}
					break
				}
				tasks, err := rebuilder.buildTasks(ctx, rebuildShards)
				if err != nil {
					entry.WithError(err).Error("building tasks")
					time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableShardInterval) * time.Second)
					continue
				}
				from = rebuildShards[len(rebuildShards)-1].ID + 1
				_, err = collectionRM.UpdateOne(ctx, bson.M{"_id": miner.ID}, bson.M{"$set": bson.M{fmt.Sprintf("grids.%d", begin): from}})
				if err != nil {
					entry.WithError(err).Errorf("change grids %d failed: %d", begin, from)
				}
				rebuilder.lock.RLock()
				for _, task := range tasks {
					select {
					case rebuilder.taskAllocator[miner.ID].ch <- task:
						continue
					default:
						from = task.ID
						_, err = collectionRM.UpdateOne(ctx, bson.M{"_id": miner.ID}, bson.M{"$set": bson.M{fmt.Sprintf("grids.%d", begin): from}})
						if err != nil {
							entry.WithError(err).Errorf("change grids %d failed: %d", begin, from)
						}
						time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableShardInterval) * time.Second)
						break
					}
				}
				rebuilder.lock.RUnlock()
			}
		}()
	}
	wg.Wait()
	//TODO: 全部分片发送完成
	rebuilder.lock.RLock()
	if _, ok := rebuilder.taskAllocator[miner.ID]; !ok {
		close(rebuilder.taskAllocator[miner.ID].ch)
	}
	rebuilder.lock.RUnlock()
}

func (rebuilder *Rebuilder) buildTasks(ctx context.Context, shards []*Shard) ([]*RebuildShard, error) {
	entry := log.WithFields(log.Fields{Function: "buildTasks"})
	rebuildShards := make([]*RebuildShard, 0)
OUTER:
	for _, shard := range shards {
		block, err := FetchBlock(ctx, rebuilder.tikvCli, shard.BlockID)
		if err != nil {
			entry.WithField(ShardID, shard.ID).WithError(err).Error("fetching block")
			return nil, err
		}
		rshard := new(RebuildShard)
		rshard.ID = shard.ID
		rshard.BlockID = shard.BlockID
		rshard.MinerID = shard.NodeID
		rshard.VHF = shard.VHF
		rshard.VNF = block.VNF
		rshard.SNID = block.SNID
		if block.AR == -2 {
			rshard.Type = 0xc258
			rshard.ParityShardCount = block.VNF
		} else if block.AR > 0 {
			rshard.Type = 0x68b3
			rshard.ParityShardCount = block.VNF - block.AR
		}
		siblingShards, err := FetchShards(ctx, rebuilder.tikvCli, rshard.BlockID, rshard.BlockID+int64(rshard.VNF))
		if err != nil {
			entry.WithField(ShardID, rshard.ID).WithError(err).Error("fetching sibling shards failed")
			return nil, err
		} else {
			hashs := make([][]byte, 0)
			nodeIDs := make([]int32, 0)
			i := rshard.BlockID
			for _, s := range siblingShards {
				entry.WithField(ShardID, shard.ID).Tracef("decode sibling shard info %d", i)
				if s.ID != i {
					entry.WithField(ShardID, shard.ID).Errorf("sibling shard %d not found: %d", i, s.ID)
					continue OUTER
				}
				hashs = append(hashs, s.VHF)
				nodeIDs = append(nodeIDs, s.NodeID)
				i++
			}
			if len(hashs) != int(rshard.VNF) {
				entry.WithField(ShardID, shard.ID).WithError(err).Errorf("count of sibling shard is %d, not equal to VNF %d", len(hashs), rshard.VNF)
				continue
			}
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
		cur, err := collectionRM.Find(ctx, bson.M{"status": 2, "finishBuild": true})
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
	startTime := time.Now().UnixNano()
	//如果被分配重建任务的矿机状态大于1或者权重为零则不分配重建任务
	if rbNode.Rebuilding > 100000 || rbNode.Status > 1 || rbNode.Valid == 0 || rbNode.Weight < float64(rebuilder.Params.WeightThreshold) || rbNode.AssignedSpace <= 0 || rbNode.Quota <= 0 || rbNode.Version < int32(rebuilder.Params.MinerVersionThreshold) {
		err := fmt.Errorf("no tasks can be allocated to miner %d", id)
		entry.WithError(err).Debugf("status of rebuilder miner is %d, weight is %f, rebuilding is %d, version is %d", rbNode.Status, rbNode.Weight, rbNode.Rebuilding, rbNode.Version)
		return nil, err
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
		for {
			select {
			case t, ok := <-taskChan.ch:
				if !ok {
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
				rbshards = append(rbshards, t)
				i++
				if i == rebuilder.Params.RebuildShardMinerTaskBatchSize {
					break OUTER
				}
			default:
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
		err := errors.New("no tasks can be allocated")
		entry.Warn(err)
		return nil, err
	}
	expiredTime := time.Now().Unix() + int64(rebuilder.Params.RebuildShardExpiredTime)
	tasks := new(pb.MultiTaskDescription)
	for _, shard := range rbshards {
		hashs := shard.Hashs
		locations := make([]*pb.P2PLocation, 0)
		for _, id := range shard.NodeIDs {
			n := rebuilder.NodeManager.GetNode(id)
			if n == nil {
				locations = append(locations, &pb.P2PLocation{NodeId: "16Uiu2HAmKg7EXBqx3SXbE2XkqbPLft8NGkzQcsbJymVB9uw7fW1r", Addrs: []string{"/ip4/127.0.0.1/tcp/59999"}})
			} else {
				locations = append(locations, &pb.P2PLocation{NodeId: n.NodeID, Addrs: n.Addrs})
			}
		}
		if (shard.Type == 0x68b3 && len(locations) < int(shard.VNF)) || (shard.Type == 0xc258 && len(locations) == 0) {
			entry.WithField(MinerID, shard.MinerID).WithField(ShardID, shard.ID).Warnf("sibling shards are not enough, only %d shards", len(hashs))
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
				entry.WithField(MinerID, shard.MinerID).WithField(ShardID, shard.ID).WithError(err).Errorf("marshaling LRC task: %d", shard.ID)
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
				entry.WithField(MinerID, shard.MinerID).WithField(ShardID, shard.ID).WithError(err).Errorf("marshaling replication task: %d", shard.ID)
				continue
			}
		}
		btask := append(Uint16ToBytes(uint16(shard.Type)), b...)
		tasks.Tasklist = append(tasks.Tasklist, btask)
	}
	tasks.ExpiredTime = expiredTime
	tasks.SrcNodeID = minerID
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

func FetchBlock(ctx context.Context, tikvCli *rawkv.Client, blockID int64) (*Block, error) {
	buf, err := tikvCli.Get(ctx, []byte(fmt.Sprintf("%s_%019d", PFX_BLOCKS, blockID)))
	if err != nil {
		return nil, err
	}
	block := new(Block)
	err = block.FillBytes(buf)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func FetchShard(ctx context.Context, tikvCli *rawkv.Client, shardID int64) (*Shard, error) {
	buf, err := tikvCli.Get(ctx, []byte(fmt.Sprintf("%s_%019d", PFX_SHARDS, shardID)))
	if err != nil {
		return nil, err
	}
	shard := new(Shard)
	err = shard.FillBytes(buf)
	if err != nil {
		return nil, err
	}
	return shard, nil
}

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
			shards = append(shards, s)
		}
		from = fmt.Sprintf("%019d", shards[len(shards)-1].ID+1)
		cnt += int64(len(values))
	}
	entry.Debugf("fetch %d shards", len(shards))
	return shards, nil
}

func FetchShards(ctx context.Context, tikvCli *rawkv.Client, shardFrom int64, shardTo int64) ([]*Shard, error) {
	_, values, err := tikvCli.Scan(ctx, []byte(fmt.Sprintf("%s_%s", PFX_SHARDS, fmt.Sprintf("%019d", shardFrom))), []byte(fmt.Sprintf("%s_%s", PFX_SHARDS, fmt.Sprintf("%019d", shardTo))), 164)
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
		shards = append(shards, s)
	}
	return shards, nil
}

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

func BuildTasks2(ctx context.Context, tikvCli *rawkv.Client, shards []*Shard) ([]*RebuildShard, error) {
	entry := log.WithFields(log.Fields{Function: "buildTasks"})
	rebuildShards := make([]*RebuildShard, 0)
OUTER:
	for _, shard := range shards {
		block, err := FetchBlock(ctx, tikvCli, shard.BlockID)
		if err != nil {
			entry.WithField(ShardID, shard.ID).WithError(err).Error("fetching block")
			return nil, err
		}
		rshard := new(RebuildShard)
		rshard.ID = shard.ID
		rshard.BlockID = shard.BlockID
		rshard.MinerID = shard.NodeID
		rshard.VHF = shard.VHF
		rshard.VNF = block.VNF
		rshard.SNID = block.SNID
		if block.AR == -2 {
			rshard.Type = 0xc258
			rshard.ParityShardCount = block.VNF
		} else if block.AR > 0 {
			rshard.Type = 0x68b3
			rshard.ParityShardCount = block.VNF - block.AR
		}
		siblingShards, err := FetchShards(ctx, tikvCli, rshard.BlockID, rshard.BlockID+int64(rshard.VNF))
		if err != nil {
			entry.WithField(ShardID, rshard.ID).WithError(err).Error("fetching sibling shards failed")
			return nil, err
		} else {
			hashs := make([][]byte, 0)
			nodeIDs := make([]int32, 0)
			i := rshard.BlockID
			for _, s := range siblingShards {
				entry.WithField(ShardID, shard.ID).Tracef("decode sibling shard info %d", i)
				if s.ID != i {
					entry.WithField(ShardID, shard.ID).Errorf("sibling shard %d not found: %d", i, s.ID)
					continue OUTER
				}
				hashs = append(hashs, s.VHF)
				nodeIDs = append(nodeIDs, s.NodeID)
				i++
			}
			if len(hashs) != int(rshard.VNF) {
				entry.WithField(ShardID, shard.ID).WithError(err).Errorf("count of sibling shard is %d, not equal to VNF %d", len(hashs), rshard.VNF)
				continue
			}
			rshard.Hashs = hashs
			rshard.NodeIDs = nodeIDs
		}
		rebuildShards = append(rebuildShards, rshard)
	}
	return rebuildShards, nil
}
