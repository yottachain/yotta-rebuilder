package ytrebuilder

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aurawing/auramq"
	"github.com/aurawing/auramq/msg"
	proto "github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/yottachain/yotta-rebuilder/pb"
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
	Params            *MiscConfig
	mqClis            map[int]*ytsync.Service
}

//New create a new rebuilder instance
func New(analysisDBURL, rebuilderDBURL string, mqconf *AuraMQConfig, conf *MiscConfig) (*Rebuilder, error) {
	entry := log.WithFields(log.Fields{Function: "New"})
	analysisdbClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(analysisDBURL))
	if err != nil {
		entry.WithError(err).Errorf("creating analysisDB client failed: %s", analysisDBURL)
		return nil, err
	}
	entry.Infof("created analysisDB client: %s", analysisDBURL)
	rebuilderdbClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(rebuilderDBURL))
	if err != nil {
		entry.WithError(err).Errorf("creating rebuilderDB client failed: %s", rebuilderDBURL)
		return nil, err
	}
	entry.Infof("created rebuilderDB client: %s", rebuilderDBURL)
	callback := func(msg *msg.Message) {
		if msg.GetType() == auramq.BROADCAST {
			if msg.GetDestination() == mqconf.MinerSyncTopic {
				nodemsg := new(pb.NodeMsg)
				err := proto.Unmarshal(msg.Content, nodemsg)
				if err != nil {
					entry.WithError(err).Error("decoding nodeMsg failed")
					return
				}
				node := new(Node)
				node.Fillby(nodemsg)
				syncNode(rebuilderdbClient, node, conf.ExcludeAddrPrefix)
			}
		}
	}
	m, err := ytsync.StartSync(mqconf.SubscriberBufferSize, mqconf.PingWait, mqconf.ReadWait, mqconf.WriteWait, mqconf.MinerSyncTopic, mqconf.AllSNURLs, callback, mqconf.Account, mqconf.PrivateKey)
	if err != nil {
		entry.WithError(err).Error("creating mq clients map failed")
		return nil, err
	}
	rebuilder := &Rebuilder{analysisdbClient: analysisdbClient, rebuilderdbClient: rebuilderdbClient, Params: conf, mqClis: m}
	return rebuilder, nil
}

func syncNode(cli *mongo.Client, node *Node, excludeAddrPrefix string) error {
	entry := log.WithFields(log.Fields{Function: "syncNode"})
	if node.ID == 0 {
		return errors.New("node ID cannot be 0")
	}
	node.Addrs = checkPublicAddrs(node.Addrs, excludeAddrPrefix)
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
	}
	return nil
}

func checkPublicAddrs(addrs []string, excludeAddrPrefix string) []string {
	filteredAddrs := []string{}
	for _, addr := range addrs {
		if strings.HasPrefix(addr, "/ip4/127.") ||
			strings.HasPrefix(addr, "/ip4/192.168.") ||
			strings.HasPrefix(addr, "/ip4/169.254.") ||
			strings.HasPrefix(addr, "/ip4/10.") ||
			strings.HasPrefix(addr, "/ip4/172.16.") ||
			strings.HasPrefix(addr, "/ip4/172.17.") ||
			strings.HasPrefix(addr, "/ip4/172.18.") ||
			strings.HasPrefix(addr, "/ip4/172.19.") ||
			strings.HasPrefix(addr, "/ip4/172.20.") ||
			strings.HasPrefix(addr, "/ip4/172.21.") ||
			strings.HasPrefix(addr, "/ip4/172.22.") ||
			strings.HasPrefix(addr, "/ip4/172.23.") ||
			strings.HasPrefix(addr, "/ip4/172.24.") ||
			strings.HasPrefix(addr, "/ip4/172.25.") ||
			strings.HasPrefix(addr, "/ip4/172.26.") ||
			strings.HasPrefix(addr, "/ip4/172.27.") ||
			strings.HasPrefix(addr, "/ip4/172.28.") ||
			strings.HasPrefix(addr, "/ip4/172.29.") ||
			strings.HasPrefix(addr, "/ip4/172.30.") ||
			strings.HasPrefix(addr, "/ip4/172.31.") ||
			strings.HasPrefix(addr, "/ip6/") ||
			strings.HasPrefix(addr, "/p2p-circuit/") {
			if excludeAddrPrefix != "" && strings.HasPrefix(addr, excludeAddrPrefix) {
				filteredAddrs = append(filteredAddrs, addr)
			}
			continue
		} else {
			filteredAddrs = append(filteredAddrs, addr)
		}
	}
	return dedup(filteredAddrs)
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
	go rebuilder.processRebuildableMiner()
	go rebuilder.processRebuildableShard()
	go rebuilder.reaper()
}

func (rebuilder *Rebuilder) processRebuildableMiner() {
	entry := log.WithFields(log.Fields{Function: "processRebuildableMiner"})
	collection := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(NodeTab)
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
			node := new(Node)
			err := cur.Decode(node)
			if err != nil {
				entry.WithError(err).Warn("decoding node")
				continue
			}
			miner := new(RebuildMiner)
			miner.ID = node.ID
			miner.Status = node.Status
			miner.Timestamp = time.Now().Unix()
			_, err = collectionRM.InsertOne(context.Background(), miner)
			if err != nil {
				entry.WithError(err).Warnf("insert rebuild miner %d to database", node.ID)
			}
			collection.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$unset": bson.M{"tasktimestamp": ""}})
		}
		cur.Close(context.Background())
		time.Sleep(time.Duration(rebuilder.Params.ProcessRebuildableMinerInterval) * time.Second)
	}
}

func (rebuilder *Rebuilder) processRebuildableShard() {
	entry := log.WithFields(log.Fields{Function: "processRebuildableShard"})
	//collectionRN := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(NodeTab)
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
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
			//result := collectionRS.FindOne(context.Background(), bson.M{"minerID": miner.ID, "timestamp": bson.M{"$lt": time.Now().Unix() - int64(rebuilder.Params.RebuildShardExpiredTime)}})
			result := collectionRS.FindOne(context.Background(), bson.M{"minerID": miner.ID, "timestamp": bson.M{"$lt": Int64Max}})
			if result.Err() == nil {
				entry.WithField(MinerID, miner.ID).Debug("unfinished shard-rebuilding tasks exist")
				continue
			}
			if result.Err() != mongo.ErrNoDocuments {
				entry.WithField(MinerID, miner.ID).WithError(err).Warn("finding unfinished shard-rebuilding tasks")
				continue
			}
			options := options.FindOptions{}
			options.Sort = bson.M{"_id": 1}
			limit := int64(rebuilder.Params.RebuildShardTaskBatchSize)
			options.Limit = &limit
			curShard, err := collectionAS.Find(context.Background(), bson.M{"nodeId": miner.ID, "_id": bson.M{"$gt": miner.To}}, &options)
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
					entry.WithField(MinerID, miner.ID).WithError(err).Error("decoding block")
					curShard.Close(context.Background())
					continue INNER
				}
				rshard := new(RebuildShard)
				rshard.ID = shard.ID
				rshard.BlockID = shard.BlockID
				rshard.MinerID = shard.NodeID
				rshard.VHF = shard.VHF.Data
				if block.AR == -2 {
					rshard.Type = 0xc258
					rshard.ParityShardCount = block.VNF
				} else if block.AR > 0 {
					rshard.Type = 0x68b3
					rshard.ParityShardCount = block.VNF - block.AR
				}
				// hashs := make([][]byte, 0)
				// locations := make([]*Location, 0)
				// for i := rshard.BlockID; i < rshard.BlockID+int64(block.VNF); i++ {
				// 	s := new(Shard)
				// 	err := collectionAS.FindOne(context.Background(), bson.M{"_id": i}).Decode(s)
				// 	if err != nil {
				// 		entry.WithField(MinerID, miner.ID).WithError(err).Errorf("decoding sibling shard %d", i)
				// 		continue
				// 	}
				// 	n := new(Node)
				// 	err = collectionRN.FindOne(context.Background(), bson.M{"_id": s.NodeID}).Decode(n)
				// 	if err != nil {
				// 		entry.WithField(MinerID, miner.ID).WithError(err).Errorf("decoding miner info of sibling shard %d", i)
				// 		continue
				// 	}
				// 	hashs = append(hashs, s.VHF.Data)
				// 	locations = append(locations, &Location{NodeID: n.NodeID, Addrs: n.Addrs})
				// }
				// if block.AR > 0 {
				// 	if len(hashs) < int(block.AR) {
				// 		entry.WithField(MinerID, miner.ID).WithError(err).Errorf("sibling shards are not enough for shard %d, only %d shards", rshard.ID, len(hashs))
				// 		continue
				// 	}
				// 	rshard.Hashs = hashs
				// }
				// rshard.Locations = locations
				shards = append(shards, *rshard)
			}
			curShard.Close(context.Background())
			if len(shards) == 0 {
				//重建完毕，状态改为3，删除旧任务
				_, err := collectionRS.DeleteMany(context.Background(), bson.M{"_id": miner.ID})
				if err != nil {
					entry.WithField(MinerID, miner.ID).WithError(err).Error("delete old shard-rebuildng tasks")
					continue
				}
				_, err = collectionRM.UpdateOne(context.Background(), bson.M{"_id": miner.ID}, bson.M{"$set": bson.M{"from": 0, "to": 0, "status": 3}})
				if err != nil {
					entry.WithField(MinerID, miner.ID).WithError(err).Error("update rebuild miner status to 3")
				}
				continue
			}
			_, err = collectionRS.InsertMany(context.Background(), shards)
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Warn("insert shard-rebuilding tasks")
			}
			From := shards[0].(Shard).ID
			To := shards[len(shards)-1].(Shard).ID
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
					entry.WithField(MinerID, miner.ID).WithError(err).Error("delete old shard-rebuilding tasks")
				}
			}
		}
	}
}

//GetRebuildTasks get rebuild tasks
func (rebuilder *Rebuilder) GetRebuildTasks() (*pb.MultiTaskDescription, error) {
	entry := log.WithFields(log.Fields{Function: "GetRebuildTasks"})
	collectionRN := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(NodeTab)
	collectionAB := rebuilder.analysisdbClient.Database(MetaDB).Collection(Blocks)
	collectionAS := rebuilder.analysisdbClient.Database(MetaDB).Collection(Shards)
	collectionRM := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildMinerTab)
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	collectionRU := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(UnrebuildShardTab)
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
		cur2, err := collectionRS.Find(context.Background(), bson.M{"minerID": miner.ID, "timestamp": bson.M{"$lt": time.Now().Unix() - int64(rebuilder.Params.RebuildShardExpiredTime)}})
		if err != nil {
			entry.WithField(MinerID, miner.ID).WithError(err).Warn("fetch rebuildable shards")
			continue
		}
		i := 0
		tasks := new(pb.MultiTaskDescription)
		for cur2.Next(context.Background()) {
			if i == rebuilder.Params.RebuildShardMinerTaskBatchSize {
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
			result, err := collectionRS.UpdateOne(context.Background(), bson.M{"_id": shard.ID, "timestamp": shard.Timestamp}, bson.M{"$set": bson.M{"timestamp": time.Now().UnixNano()}})
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Errorf("update timestamp of rebuildable shard: %d", shard.ID)
				i--
				continue
			}
			if result.ModifiedCount == 0 {
				entry.WithField(MinerID, miner.ID).Warnf("update timestamp of rebuildable shard failed: %d", shard.ID)
				i--
				continue
			}
			block := new(Block)
			err = collectionAB.FindOne(context.Background(), bson.M{"_id": shard.BlockID}).Decode(block)
			if err != nil {
				entry.WithField(MinerID, miner.ID).WithError(err).Error("decoding block")
				collectionRU.InsertOne(context.Background(), shard)
				collectionRS.UpdateOne(context.Background(), bson.M{"_id": shard.ID}, bson.M{"$set": bson.M{"timestamp": Int64Max}})
				i--
				continue
			}
			hashs := make([][]byte, 0)
			locations := make([]*pb.P2PLocation, 0)
			for i := shard.BlockID; i < shard.BlockID+int64(block.VNF); i++ {
				s := new(Shard)
				err := collectionAS.FindOne(context.Background(), bson.M{"_id": i}).Decode(s)
				if err != nil {
					entry.WithField(MinerID, miner.ID).WithError(err).Errorf("decoding sibling shard %d of shard %d", i, shard.ID)
					continue
				}
				n := new(Node)
				err = collectionRN.FindOne(context.Background(), bson.M{"_id": s.NodeID}).Decode(n)
				if err != nil {
					entry.WithField(MinerID, miner.ID).WithError(err).Errorf("decoding miner info of sibling shard %d of shard %d", i, shard.ID)
					continue
				}
				hashs = append(hashs, s.VHF.Data)
				locations = append(locations, &pb.P2PLocation{NodeId: n.NodeID, Addrs: n.Addrs})
			}
			var b []byte
			if shard.Type == 0x68b3 {
				//LRC
				task := new(pb.TaskDescription)
				task.Id = Int64ToBytes(shard.ID)
				task.Hashs = hashs
				task.ParityShardCount = shard.ParityShardCount
				task.RecoverId = int32(shard.ID - shard.BlockID)
				task.Locations = locations
				b, err = proto.Marshal(task)
				if err != nil {
					entry.WithField(MinerID, miner.ID).WithError(err).Errorf("marshaling LRC task: %d", shard.ID)
					i--
					continue
				}
			} else if shard.Type == 0xc258 {
				//replication
				task := new(pb.TaskDescriptionCP)
				task.Id = Int64ToBytes(shard.ID)
				task.DataHash = shard.VHF
				task.Locations = locations
				b, err = proto.Marshal(task)
				if err != nil {
					entry.WithField(MinerID, miner.ID).WithError(err).Errorf("marshaling replication task: %d", shard.ID)
					i--
					continue
				}
			}
			if (shard.Type == 0x68b3 && len(locations) < int(block.AR)) || (shard.Type == 0xc258 && len(locations) == 0) {
				entry.WithField(MinerID, miner.ID).WithError(err).Errorf("sibling shards are not enough for shard %d, only %d shards", shard.ID, len(hashs))
				collectionRU.InsertOne(context.Background(), shard)
				collectionRS.UpdateOne(context.Background(), bson.M{"_id": shard.ID}, bson.M{"$set": bson.M{"timestamp": Int64Max}})
				i--
				continue
			}
			btask := append(Uint16ToBytes(uint16(shard.Type)), b...)
			tasks.Tasklist = append(tasks.Tasklist, btask)
		}
		cur2.Close(context.Background())
		if i == 0 {
			continue
		}
		return tasks, nil
	}
	return nil, errors.New("no tasks can be allocated")
}

//UpdateTaskStatus update task status
func (rebuilder *Rebuilder) UpdateTaskStatus(result *pb.MultiTaskOpResult) {
	entry := log.WithFields(log.Fields{Function: "UpdateTaskStatus"})
	collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
	for i, b := range result.Id {
		id := BytesToInt64(b)
		ret := result.RES[i]
		if ret == 0 {
			entry.Debugf("task %d rebuilt success", id)
			collectionRS.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": bson.M{"timestamp": Int64Max}})
		} else if ret == 1 {
			entry.Debugf("task %d rebuilt failed", id)
			collectionRS.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": bson.M{"timestamp": int64(0)}})
		}
	}
}
