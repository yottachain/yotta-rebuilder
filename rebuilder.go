package ytrebuilder

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
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
	cache := NewCache(uint64(conf.MaxCacheSize))
	entry.Info("cache created")
	pool := grpool.NewPool(conf.SyncPoolLength, conf.SyncQueueLength)
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
					syncNode(rebuilderdbClient, nodeMgr, node)
				}
			}
		}
	}
	m, err := ytsync.StartSync(mqconf.SubscriberBufferSize, mqconf.PingWait, mqconf.ReadWait, mqconf.WriteWait, mqconf.MinerSyncTopic, mqconf.AllSNURLs, callback, mqconf.Account, mqconf.PrivateKey, mqconf.ClientID)
	if err != nil {
		entry.WithError(err).Error("creating mq clients map failed")
		return nil, err
	}
	ratelimiter := rl.NewBucket(time.Millisecond*time.Duration(conf.FetchTaskTimeGap), 2)
	rebuilder := &Rebuilder{analysisdbClient: analysisdbClient, rebuilderdbClient: rebuilderdbClient, NodeManager: nodeMgr, Cache: cache, Compensation: cpsConf, Params: conf, mqClis: m, ratelimiter: ratelimiter}
	return rebuilder, nil
}

func syncNode(cli *mongo.Client, nodeMgr *NodeManager, node *Node) error {
	entry := log.WithFields(log.Fields{Function: "syncNode"})
	if node.ID == 0 {
		return errors.New("node ID cannot be 0")
	}
	collection := cli.Database(RebuilderDB).Collection(NodeTab)
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
		nodeMgr.UpdateNode(updatedNode)
		if updatedNode.Status == 3 {
			//重建完毕，状态改为3，删除旧任务
			collectionRM := cli.Database(RebuilderDB).Collection(RebuildMinerTab)
			collectionRS := cli.Database(RebuilderDB).Collection(RebuildShardTab)
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
			return nil
		}
	} else {
		nodeMgr.UpdateNode(node)
	}
	return nil
}

func dedup(urls []string) []string {
	if urls == nil || len(urls) == 0 {
		return nil
	}
	sort.Strings(urls)
	j := 0
	for i := 1; i < len(urls); i++ {
		if urls[j] == urls[i] {
			continue
		}
		j++
		urls[j] = urls[i]
	}
	return urls[:j+1]
}

//Start starting rebuilding process
func (rebuilder *Rebuilder) Start() {
	entry := log.WithFields(log.Fields{Function: "Start"})
	rebuilder.Preprocess()
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	collectionRU := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(UnrebuildShardTab)
	collectionAS := rebuilder.analysisdbClient.Database(MetaDB).Collection(Shards)
	curShard, err := collectionRS.Find(context.Background(), bson.M{"timestamp": bson.M{"$lt": Int64Max}})
	if err != nil {
		entry.WithError(err).Error("pre-fetching rebuildable shards failed")
	} else {
		for curShard.Next(context.Background()) {
			rshard := new(RebuildShard)
			err := curShard.Decode(rshard)
			if err != nil {
				entry.WithError(err).Error("decoding rebuildable shard failed")
				continue
			}

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
			}
		}
		curShard.Close(context.Background())
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
			opts := options.FindOptions{}
			opts.Sort = bson.M{"_id": 1}
			limit := int64(rebuilder.Params.RebuildShardTaskBatchSize)
			opts.Limit = &limit
			curShard, err := collectionAS.Find(context.Background(), bson.M{"nodeId": miner.ID, "_id": bson.M{"$gt": miner.To}}, &opts)
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Error("fetching shard-rebuilding tasks")
				continue
			}
			shards := make([]interface{}, 0)
			for curShard.Next(context.Background()) {
				shard := new(Shard)
				err := curShard.Decode(shard)
				if err != nil {
					entry.WithField(MinerID, miner.ID).WithError(err).Error("decoding shard")
					curShard.Close(context.Background())
					continue INNER
				}
				block := new(Block)
				err = collectionAB.FindOne(context.Background(), bson.M{"_id": shard.BlockID}).Decode(block)
				if err != nil {
					entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).WithError(err).Errorf("decoding block of shard %d", shard.ID)
					curShard.Close(context.Background())
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
				} else {
					shards = append(shards, *rshard)
				}
			}
			curShard.Close(context.Background())
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
	tokenCount := rebuilder.ratelimiter.TakeAvailable(1)
	if tokenCount == 0 {
		err := errors.New("token bucket is empty, no tasks can be allocated")
		entry.Debug(err)
		return nil, err
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randtag := r.Int31()
	startTime := time.Now().UnixNano()
	sTime := startTime
	collectionAS := rebuilder.analysisdbClient.Database(MetaDB).Collection(Shards)
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	collectionRU := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(UnrebuildShardTab)
	rbNode := rebuilder.NodeManager.GetNode(id)
	if rbNode == nil {
		err := fmt.Errorf("node %d not found", id)
		entry.WithError(err).Error("fetch rebuilder miner")
		return nil, err
	}
	entry.Debugf("<time trace %d>1. Get rebuilder node: %d", randtag, (time.Now().UnixNano()-startTime)/1000000)
	startTime = time.Now().UnixNano()
	//如果被分配重建任务的矿机状态大于1或者权重为零则不分配重建任务
	if rbNode.Rebuilding > 0 || rbNode.Status > 1 || rbNode.Weight < float64(rebuilder.Params.WeightThreshold) {
		err := fmt.Errorf("no tasks can be allocated to miner %d", id)
		entry.WithError(err).Errorf("status of rebuilder miner is %d, weight is %f, rebuilding is %d", rbNode.Status, rbNode.Weight, rbNode.Rebuilding)
		return nil, err
	}
	//在待重建矿机表中查找状态为2的矿机
	opt := new(options.FindOptions)
	opt.Sort = bson.M{"timestamp": 1}
	cur, err := collectionRM.Find(context.Background(), bson.M{"status": 2}, opt)
	if err != nil {
		entry.WithError(err).Error("fetch rebuildable miners")
		return nil, err
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		miner := new(RebuildMiner)
		err := cur.Decode(miner)
		if err != nil {
			entry.WithError(err).Warn("decode rebuildable miner")
			continue
		}
		entry.WithField(MinerID, miner.ID).Debug("decode miner info")
		entry.WithField(MinerID, miner.ID).Debugf("<time trace %d>2. Decode miner: %d", randtag, (time.Now().UnixNano()-startTime)/1000000)
		startTime = time.Now().UnixNano()
		expiredTime := time.Now().Unix() + int64(rebuilder.Params.RebuildShardExpiredTime)
		//查找属于当前矿机的未重建或重建超时分片
		cur2, err := collectionRS.Find(context.Background(), bson.M{"minerID": miner.ID, "timestamp": bson.M{"$lt": time.Now().UnixNano() - int64(rebuilder.Params.RebuildShardExpiredTime*1000000000)}})
		if err != nil {
			entry.WithField(MinerID, miner.ID).WithError(err).Warn("fetch rebuildable shards")
			continue
		}
		i := 0
		tasks := new(pb.MultiTaskDescription)
		for cur2.Next(context.Background()) {
			//达到单台矿机最大可分配分片数则停止分配
			if i == rebuilder.Params.RebuildShardMinerTaskBatchSize {
				entry.WithField(MinerID, miner.ID).Debug("miner task batch size exceed")
				break
			}
			i++
			shard := new(RebuildShard)
			err := cur2.Decode(shard)
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Error("decoding rebuildable shard")
				i--
				continue
			}
			entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Debug("decode shard info")
			entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Debugf("<time trace %d>3. Decode shard: %d", randtag, (time.Now().UnixNano()-startTime)/1000000)
			startTime = time.Now().UnixNano()
			updateTime := time.Now().UnixNano()
			//更新待重建分片的时间戳
			result, err := collectionRS.UpdateOne(context.Background(), bson.M{"_id": shard.ID, "timestamp": shard.Timestamp}, bson.M{"$set": bson.M{"timestamp": updateTime}})
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).WithError(err).Error("update timestamp of rebuildable shard failed")
				i--
				continue
			}
			entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Debugf("<time trace %d>4. Update shard timstamp: %d", randtag, (time.Now().UnixNano()-startTime)/1000000)
			startTime = time.Now().UnixNano()
			//未更新表示分片已被其他协程更新，跳过
			if result.ModifiedCount == 0 {
				entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Debugf("update timestamp of rebuildable shard failed")
				i--
				continue
			}
			entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Debugf("update timestamp of shard task to %d", updateTime)
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
						entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).WithError(err).Errorf("decoding sibling shard %d", i)
						break
					}
					entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Tracef("decode sibling shard info %d: %d", i, s.ID)
					entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Debugf("<time trace %d>4. Decode sibling shard %d: %d", randtag, i, (time.Now().UnixNano()-startTime)/1000000)
					startTime = time.Now().UnixNano()
					hashs = append(hashs, s.VHF.Data)
					n := rebuilder.NodeManager.GetNode(s.NodeID)
					if n == nil {
						entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).WithError(err).Errorf("get miner info of sibling shard %d: %d", i, s.ID)
						locations = append(locations, &pb.P2PLocation{NodeId: "16Uiu2HAmKg7EXBqx3SXbE2XkqbPLft8NGkzQcsbJymVB9uw7fW1r", Addrs: []string{"/ip4/127.0.0.1/tcp/59999"}})
					} else {
						entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Tracef("decode miner info of sibling shard %d: %d", s.ID, n.ID)
						locations = append(locations, &pb.P2PLocation{NodeId: n.NodeID, Addrs: n.Addrs})
					}
				}
			}
			if (shard.Type == 0x68b3 && len(locations) < int(shard.VNF)) || (shard.Type == 0xc258 && len(locations) == 0) {
				entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Warnf("sibling shards are not enough, only %d shards", len(hashs))
				collectionRU.InsertOne(context.Background(), shard)
				collectionRS.UpdateOne(context.Background(), bson.M{"_id": shard.ID}, bson.M{"$set": bson.M{"timestamp": Int64Max}})
				i--
				continue
			}
			var b []byte
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
					entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).WithError(err).Errorf("marshaling LRC task: %d", shard.ID)
					i--
					continue
				}
				entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Debugf("task ID: %s", hex.EncodeToString(task.Id))
				entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Tracef("create LRC rebuilding task: %+v", task)
			} else if shard.Type == 0xc258 {
				//replication
				task := new(pb.TaskDescriptionCP)
				task.Id = append(Int64ToBytes(shard.ID), Uint16ToBytes(uint16(shard.SNID))...)
				task.DataHash = shard.VHF
				task.Locations = locations
				b, err = proto.Marshal(task)
				if err != nil {
					entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).WithError(err).Errorf("marshaling replication task: %d", shard.ID)
					i--
					continue
				}
				entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Debugf("task ID: %s", hex.EncodeToString(task.Id))
				entry.WithField(MinerID, miner.ID).WithField(ShardID, shard.ID).Tracef("create replication rebuilding task: %+v", task)
			}
			btask := append(Uint16ToBytes(uint16(shard.Type)), b...)
			tasks.Tasklist = append(tasks.Tasklist, btask)
		}
		cur2.Close(context.Background())
		if i == 0 {
			continue
		}
		tasks.ExpiredTime = expiredTime
		entry.WithField(MinerID, miner.ID).Debugf("length of task list is %d， expired time is %d", len(tasks.Tasklist), tasks.ExpiredTime)
		entry.WithField(MinerID, miner.ID).Debugf("<time trace %d>5. Finish %d tasks built: %d, total time: %d", randtag, len(tasks.Tasklist), (time.Now().UnixNano()-startTime)/1000000, (time.Now().UnixNano()-sTime)/1000000)
		return tasks, nil
	}
	err = errors.New("no tasks can be allocated")
	entry.Warn(err)
	return nil, err
}

//UpdateTaskStatus update task status
func (rebuilder *Rebuilder) UpdateTaskStatus(result *pb.MultiTaskOpResult) error {
	nodeID := result.NodeID
	if time.Now().Unix() > result.ExpiredTime {
		return errors.New("tasks expired")
	}
	startTime := time.Now().UnixNano()
	entry := log.WithFields(log.Fields{Function: "UpdateTaskStatus"})
	entry.WithField(MinerID, nodeID).Infof("received rebuilding status: %d results", len(result.Id))
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	collectionRU := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(UnrebuildShardTab)
	for i, b := range result.Id {
		id := BytesToInt64(b[0:8])
		ret := result.RES[i]
		if ret == 0 {
			entry.WithField(MinerID, nodeID).WithField(ShardID, id).Debug("task rebuilt success")
			result, err := collectionRS.UpdateOne(context.Background(), bson.M{"_id": id, "timestamp": bson.M{"$lt": Int64Max}}, bson.M{"$set": bson.M{"timestamp": Int64Max}})
			if err != nil {
				entry.WithField(MinerID, nodeID).WithField(ShardID, id).WithError(err).Error("update timestamp to finish tag")
				return err
			}
			if result.ModifiedCount == 0 {
				entry.WithField(MinerID, nodeID).WithField(ShardID, id).Warnf("update timestamp failed: 0 record modified")
				return fmt.Errorf("modify timestamp of shard %d failed: 0 record modified", id)
			}
			rebuilder.Cache.Delete(id)
		} else if ret == 1 {
			entry.WithField(MinerID, nodeID).WithField(ShardID, id).Debug("task rebuilt failed")
			opts := new(options.FindOneAndUpdateOptions)
			opts = opts.SetReturnDocument(options.After)
			result := collectionRS.FindOneAndUpdate(context.Background(), bson.M{"_id": id}, bson.M{"$inc": bson.M{"errCount": 1}}, opts)
			shard := new(RebuildShard)
			err := result.Decode(shard)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					entry.WithField(MinerID, nodeID).WithField(ShardID, id).Warnf("update timestamp failed: 0 record modified")
					return fmt.Errorf("modify error count of shard %d failed: 0 record modified", id)
				} else {
					entry.WithField(MinerID, nodeID).WithField(ShardID, id).WithField(ShardID, id).WithError(err).Error("decoding shard")
					return err
				}
			} else if shard.ErrCount == int32(rebuilder.Params.RetryCount) {
				entry.WithField(ShardID, id).Warnf("reaching max count of retries: %d", rebuilder.Params.RetryCount)
				collectionRU.InsertOne(context.Background(), shard)
				collectionRS.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": bson.M{"timestamp": Int64Max}})
				rebuilder.Cache.Delete(id)
			}
		}
	}
	entry.Debugf("<time trace> Update task status (%d tasks): %d", len(result.Id), (time.Now().UnixNano()-startTime)/1000000)
	return nil
}
