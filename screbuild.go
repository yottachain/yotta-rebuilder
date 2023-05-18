package ytrebuilder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	proto "github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	pb "github.com/yottachain/yotta-rebuilder/pbrebuilder"
)

type SCShard struct {
	ID int64 `json:"id"`
	// NodeID    int32 `json:"nodeId"`
	// NodeID2   int32 `json:"nodeId2"`
	MinerID   int32 `json:"minerId"`
	Timestamp int64 `json:"timestamp"`
}

type RebuildQueueResp struct {
	Index string   `json:"_index"`
	ID    string   `json:"_id"`
	Score float64  `json:"_score"`
	Shard *SCShard `json:"_source"`
}

func (rebuilder *Rebuilder) CheckIndex(ctx context.Context) error {
	entry := log.WithFields(log.Fields{Function: "CheckIndex"})

	index := "rebuildqueue"
	mapping := `
    {
      "settings": {
        "number_of_shards": 1,
		"number_of_replicas": 1
      },
      "mappings": {
        "properties": {
          "id": {
            "type": "long"
          },
		  "minerId": {
            "type": "integer"
          },
		  "timestamp": {
            "type": "long"
          }
        }
      }
    }`

	res, err := esapi.IndicesExistsRequest{
		Index: []string{index},
	}.Do(ctx, rebuilder.ESClient)
	if err != nil {
		entry.WithError(err).Error("create index rebuildqueue failed")
		return err
	}
	if res.StatusCode == 200 {
		res.Body.Close()
		return nil
	}
	if res.StatusCode != 404 {
		var err error
		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			entry.WithError(err).Error("decode error message of failed search response")
			err = errors.New("decode error message of failed search response")
		} else {
			// Print the response status and error information.
			err = fmt.Errorf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
			entry.WithError(err).Error("failed search response")
		}
		res.Body.Close()
		return err
	}
	entry.Info("index rebuildqueue is not exists, create new")
	res, err = rebuilder.ESClient.Indices.Create(
		index,
		rebuilder.ESClient.Indices.Create.WithBody(strings.NewReader(mapping)),
	)
	if err != nil {
		entry.WithError(err).Error("create index rebuildqueue failed")
		return err
	}
	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			entry.WithError(err).Error("decode error message of failed search response")
		} else {
			// Print the response status and error information.
			err := fmt.Errorf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
			entry.WithError(err).Error("failed search response")
		}
		res.Body.Close()
		return err
	}
	return nil
}

func (rebuilder *Rebuilder) VerifySelfCheckShards(ctx context.Context) {
	entry := log.WithFields(log.Fields{Function: "VerifySelfCheckShards"})
	query := `{"query": {"match_all" : {}},"sort":[{"timestamp":"asc"}],"size": 1000}`
	var b strings.Builder
	b.WriteString(query)
	read := strings.NewReader(b.String())
	for {
		res, err := rebuilder.ESClient.Search(
			rebuilder.ESClient.Search.WithContext(ctx),
			rebuilder.ESClient.Search.WithIndex("rebuilderr"),
			rebuilder.ESClient.Search.WithBody(read),
			rebuilder.ESClient.Search.WithTrackTotalHits(true),
			rebuilder.ESClient.Search.WithPretty(),
		)
		if err != nil {
			entry.WithError(err).Error("search self check shards failed")
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		if res.IsError() {
			var e map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
				entry.WithError(err).Error("decode error message of failed search response")
			} else {
				// Print the response status and error information.
				err := fmt.Errorf("[%s] %s: %s",
					res.Status(),
					e["error"].(map[string]interface{})["type"],
					e["error"].(map[string]interface{})["reason"],
				)
				entry.WithError(err).Error("failed search response")
			}
			res.Body.Close()
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		var resb bytes.Buffer
		resb.ReadFrom(res.Body)
		hitCount := gjson.Get(resb.String(), "hits.total.value")
		entry.Debugf("document hits: %d\n", hitCount.Int())
		if hitCount.Int() == 0 {
			res.Body.Close()
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		vals := gjson.Get(resb.String(), "hits.hits")
		entry.Debugf("get shard info from es: %s", vals)
		requests := make([]*NodeRebuildRequest, 0, hitCount.Int())
		err = json.NewDecoder(strings.NewReader(vals.String())).Decode(&requests)
		if err != nil {
			entry.WithError(err).Error("decode search response to json failed")
			res.Body.Close()
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		for i, req := range requests {
			if req.Source.Log.MinerId == 0 {
				entry.WithError(err).Error("miner ID is zero")
			} else {
				shardIDs := make([]int64, 0)
				//shardMap := make(map[int64]*ErrShard)
				for _, s := range req.Source.Log.ErrShards {
					if s.ShardId != 0 {
						shardIDs = append(shardIDs, s.ShardId)
						//shardMap[s.ShardId] = s
					}
				}

				urls := rebuilder.Compensation.AllSyncURLs
				snCount := len(urls)
				snID := req.Source.Log.MinerId % int64(snCount)
				shards, err := GetShards(rebuilder.HttpCli, rebuilder.Compensation.AllSyncURLs[snID], shardIDs)
				if err != nil {
					entry.WithError(err).Error("get shards failed")
					continue
				}

				for _, s1 := range shards {
					if int64(s1.NodeID) == req.Source.Log.MinerId || int64(s1.NodeID2) == req.Source.Log.MinerId {
						var buf bytes.Buffer
						if err := json.NewEncoder(&buf).Encode(SCShard{ID: s1.ID, MinerID: int32(req.Source.Log.MinerId), Timestamp: time.Now().UnixNano() - int64(rebuilder.Params.RebuildShardExpiredTime)*1000000000}); err != nil {
							entry.WithError(err).Error("encode scshard failed")
							continue
						}
						addres, err := rebuilder.ESClient.Index("rebuildqueue", &buf, rebuilder.ESClient.Index.WithContext(ctx), rebuilder.ESClient.Index.WithDocumentID(fmt.Sprintf("%d_%d", req.Source.Log.MinerId, s1.ID)))
						if err != nil {
							entry.WithError(err).Error("index scshard failed")
							continue
						}
						addres.Body.Close()
					}
				}
			}
			var delRes *esapi.Response
			if i == len(requests)-1 {
				delRes, err = rebuilder.ESClient.Delete("rebuilderr", req.ID, rebuilder.ESClient.Delete.WithRefresh("wait_for"))
			} else {
				delRes, err = rebuilder.ESClient.Delete("rebuilderr", req.ID)
			}
			if err != nil {
				entry.WithError(err).Errorf("delete self check shard failed: %d", req.Source.Log.MinerId)
				continue
			}
			if delRes.IsError() {
				var e map[string]interface{}
				if err := json.NewDecoder(delRes.Body).Decode(&e); err != nil {
					entry.WithError(err).Error("decode error message of failed search response")
				} else {
					// Print the response status and error information.
					err := fmt.Errorf("[%s] %s: %s",
						res.Status(),
						e["error"].(map[string]interface{})["type"],
						e["error"].(map[string]interface{})["reason"],
					)
					entry.WithError(err).Error("failed search response")
				}
				delRes.Body.Close()
				time.Sleep(time.Duration(10) * time.Second)
				continue
			}
			delRes.Body.Close()
			entry.Debugf("delete shard info from es: %s", req.ID)
		}
		res.Body.Close()
	}
}

func (rebuilder *Rebuilder) processSelfCheckShards(ctx context.Context) {
	entry := log.WithFields(log.Fields{Function: "processSelfCheckShards"})
	for {
		rebuilder.lock2.RLock()
		checkpoints := make([]int64, 0, len(rebuilder.checkPoints))
		for _, value := range rebuilder.checkPoints {
			checkpoints = append(checkpoints, value)
		}
		rebuilder.lock2.RUnlock()
		checkpoint := Min(checkpoints...)
		now := time.Now().UnixNano()
		threshold := Min(checkpoint, now)
		threshold -= int64(rebuilder.Params.RebuildShardExpiredTime) * 1000000000

		query := fmt.Sprintf(`{"query":{"range":{"timestamp":{"lt":%d}}},"sort":[{"timestamp":{"order":"asc"}}],"size": 1000}`, threshold)
		entry.Debugf("query: %s", query)
		var b strings.Builder
		b.WriteString(query)
		read := strings.NewReader(b.String())
		res, err := rebuilder.ESClient.Search(
			rebuilder.ESClient.Search.WithContext(ctx),
			rebuilder.ESClient.Search.WithIndex("rebuildqueue"),
			rebuilder.ESClient.Search.WithBody(read),
			rebuilder.ESClient.Search.WithTrackTotalHits(true),
			rebuilder.ESClient.Search.WithPretty(),
		)
		if err != nil {
			entry.WithError(err).Error("search rebuildqueue shards failed")
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		if res.IsError() {
			var e map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
				entry.WithError(err).Error("decode error message of failed search response")
			} else {
				// Print the response status and error information.
				err := fmt.Errorf("[%s] %s: %s",
					res.Status(),
					e["error"].(map[string]interface{})["type"],
					e["error"].(map[string]interface{})["reason"],
				)
				entry.WithError(err).Error("failed search response")
			}
			res.Body.Close()
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		var resb bytes.Buffer
		resb.ReadFrom(res.Body)
		hitCount := gjson.Get(resb.String(), "hits.total.value")
		entry.Debugf("document hits: %d\n", hitCount.Int())
		if hitCount.Int() == 0 {
			res.Body.Close()
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		vals := gjson.Get(resb.String(), "hits.hits")
		entry.Debugf("get shard info from es: %s", vals)
		responses := make([]*RebuildQueueResp, 0, hitCount.Int())
		err = json.NewDecoder(strings.NewReader(vals.String())).Decode(&responses)
		if err != nil {
			entry.WithError(err).Error("decode search response to json failed")
			res.Body.Close()
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		fixMap := make(map[int32][]*SCShard)
		for _, scresp := range responses {
			fixMap[scresp.Shard.MinerID] = append(fixMap[scresp.Shard.MinerID], scresp.Shard)
		}
		i := 0
		for minerID, rbshards := range fixMap {
			i++
			rbnode := rebuilder.NodeManager.GetNode(minerID)
			if rbnode.Status != 1 {
				for _, shard := range rbshards {
					update := strings.NewReader(fmt.Sprintf(`{"doc": {"timestamp": %d}}`, now))
					updateRes, err := rebuilder.ESClient.Update("rebuildqueue", fmt.Sprintf("%d_%d", shard.MinerID, shard.ID), update, rebuilder.ESClient.Update.WithContext(ctx))
					if err != nil {
						entry.WithError(err).Errorf("update timestamp of shard %d failed: %d", shard.ID, minerID)
						continue
					}
					if updateRes.IsError() {
						var e map[string]interface{}
						if err := json.NewDecoder(updateRes.Body).Decode(&e); err != nil {
							entry.WithError(err).Error("decode error message of failed search response")
						} else {
							// Print the response status and error information.
							err := fmt.Errorf("[%s] %s: %s",
								updateRes.Status(),
								e["error"].(map[string]interface{})["type"],
								e["error"].(map[string]interface{})["reason"],
							)
							entry.WithError(err).Error("failed update response")
						}
					}
					updateRes.Body.Close()
				}
				continue
			}
			for i := 0; i < len(rbshards); i += rebuilder.Params.RebuildShardMinerTaskBatchSize {
				end := i + rebuilder.Params.RebuildShardMinerTaskBatchSize
				if end > len(rbshards) {
					end = len(rbshards)
				}
				waitFor := false
				if end == len(rbshards) {
					waitFor = true
				}
				chunk := rbshards[i:end]
				err = rebuilder.BuildSelfCheckTasks(ctx, minerID, chunk, waitFor)
				if err != nil {
					entry.WithError(err).Error("build self check rebuild task failed")
				}
			}
		}
		res.Body.Close()
	}
}

func (rebuilder *Rebuilder) BuildSelfCheckTasks(ctx context.Context, minerID int32, scshards []*SCShard, waitFor bool) error {
	entry := log.WithFields(log.Fields{Function: "BuildSelfCheckTasks", MinerID: minerID})
	tgtNode := rebuilder.NodeManager.GetNode(int32(minerID))
	if tgtNode == nil {
		err := errors.New("target node not found")
		entry.WithError(err).Error("fetch target miner")
		if waitFor {
			time.Sleep(time.Duration(1) * time.Second)
		}
		return err
	}
	if tgtNode.Status == 2 || tgtNode.Status == 3 {
		err := errors.New("target node is under rebuilding")
		entry.WithError(err).Error("fetch target miner")
		if waitFor {
			time.Sleep(time.Duration(1) * time.Second)
		}
		return err
	}
	shards := make([]*Shard, 0)
	for _, v := range scshards {
		//if v.ShardId == "" || v.ShardId == "0" {
		// if v.ShardId == 0 {
		// 	continue
		// }
		// shardID, err := strconv.ParseInt(v.ShardId, 10, 64)
		// if err != nil {
		// 	entry.WithError(err).Errorf("decode shard ID %s failed", v.ShardId)
		// 	continue
		// }
		shardID := v.ID
		shard, err := FindNodeShard(ctx, rebuilder.tikvCli, shardID, minerID)
		if err != nil {
			entry.WithError(err).Errorf("find node shard %s failed", v.ID)
			continue
		}
		shards = append(shards, shard)
	}
	if len(shards) == 0 {
		entry.WithError(ErrNoTaskAlloc).Error("find no node shard from tikv")
		if waitFor {
			time.Sleep(time.Duration(1) * time.Second)
		}
		return ErrNoTaskAlloc
	}
	bindexes := make([]uint64, 0)
	for _, s := range shards {
		bindexes = append(bindexes, s.BIndex)
	}
	var err error
	hcli := rebuilder.HttpCli
	// if strings.HasPrefix(rebuilder.Compensation.SyncClientURL, "https") {
	// 	hcli = rebuilder.httpCli2
	// }
	blocksMap, err := GetBlocksRetries(hcli, rebuilder.Compensation.SyncClientURL, bindexes, 3)
	if err != nil {
		entry.WithError(err).Error("fetching blocks")
		if waitFor {
			time.Sleep(time.Duration(1) * time.Second)
		}
		return err
	}
	rebuildShards := make([]*RebuildShard, 0)
	//idx := 0
OUTER:
	for _, shard := range shards {
		rshard := new(RebuildShard)
		rshard.ID = shard.ID
		rshard.BlockID = shard.ID - int64(shard.Offset)
		rshard.MinerID = shard.NodeID
		rshard.MinerID2 = shard.NodeID2
		rshard.VHF = shard.VHF
		if rebuilder.Symmetric {
			rshard.SNID = 0
		} else {
			rshard.SNID = snIDFromID(uint64(shard.ID))
		}

		block := blocksMap[shard.BIndex]
		if block == nil {
			entry.WithField(ShardID, shard.ID).Warn("block of shard have deleted")
			continue
		}
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

		var siblingShards []*Shard
		hashs := make([][]byte, 0)
		nodeIDs := make([]*NodePair, 0)

		for i, s := range block.Shards {
			siblingShards = append(siblingShards, &Shard{ID: int64(block.ID) + int64(i), VHF: s.VHF, BIndex: shard.BIndex, Offset: uint8(i), NodeID: int32(s.NodeID), NodeID2: int32(s.NodeID2)})
		}
		entry.Debugf("fetch %d sibling shards for shard %d", len(siblingShards), rshard.ID)

		i := rshard.BlockID
		needHash := false
		if rshard.Type == 0x68b3 {
			needHash = true
		}
		for _, s := range siblingShards {
			entry.WithField(ShardID, shard.ID).Tracef("decode sibling shard info %d: %d", i, s.ID)
			if s.ID != i {
				entry.WithField(ShardID, shard.ID).Errorf("sibling shard %d not found: %d", i, s.ID)
				continue OUTER
			}
			if needHash {
				hashs = append(hashs, s.VHF)
			}
			nodeIDs = append(nodeIDs, &NodePair{s.NodeID, s.NodeID2})
			i++
		}
		if len(nodeIDs) != int(rshard.VNF) {
			entry.WithField(ShardID, shard.ID).WithError(err).Errorf("count of sibling shard is %d, not equal to VNF %d", len(hashs), rshard.VNF)
			continue
		}
		rshard.Hashs = hashs
		rshard.NodeIDs = nodeIDs

		rebuildShards = append(rebuildShards, rshard)
		// idx++
		// if i == int64(rebuilder.Params.RebuildShardMinerTaskBatchSize) {
		// 	idx = 0
		// 	pkg := &SCRebuildPkg{MinerId: minerID, Shards: rebuildShards}
		// 	rebuildShards = make([]*RebuildShard, 0)
		// 	rebuilder.scTaskCh <- pkg
		// 	entry.Infof("put self check shards task to chan,size: %d", len(rebuildShards))
		// }
	}
	if len(rebuildShards) != 0 {
		pkg := &SCRebuildPkg{MinerId: minerID, Shards: rebuildShards}
		rebuilder.scTaskCh <- pkg
		now := time.Now().UnixNano()
		pkg.Timestamp = now
		entry.Infof("put self check shards task to chan, size: %d", len(rebuildShards))
		for i, shard := range scshards {
			update := strings.NewReader(fmt.Sprintf(`{"doc": {"timestamp": %d}}`, now))
			var updateRes *esapi.Response
			if i == len(scshards)-1 && waitFor {
				updateRes, err = rebuilder.ESClient.Update("rebuildqueue", fmt.Sprintf("%d_%d", shard.MinerID, shard.ID), update, rebuilder.ESClient.Update.WithContext(ctx), rebuilder.ESClient.Update.WithRefresh("wait_for"))
			} else {
				updateRes, err = rebuilder.ESClient.Update("rebuildqueue", fmt.Sprintf("%d_%d", shard.MinerID, shard.ID), update, rebuilder.ESClient.Update.WithContext(ctx))
			}
			if err != nil {
				entry.WithError(err).Errorf("update timestamp of shard %d failed: %d", shard.ID, minerID)
			}
			if updateRes.IsError() {
				var e map[string]interface{}
				if err := json.NewDecoder(updateRes.Body).Decode(&e); err != nil {
					entry.WithError(err).Error("decode error message of failed search response")
				} else {
					// Print the response status and error information.
					err := fmt.Errorf("[%s] %s: %s",
						updateRes.Status(),
						e["error"].(map[string]interface{})["type"],
						e["error"].(map[string]interface{})["reason"],
					)
					entry.WithError(err).Error("failed update response")
				}
			}
		}
	} else {
		if waitFor {
			time.Sleep(time.Duration(1) * time.Second)
		}
	}
	return nil
}

func (rebuilder *Rebuilder) GetSCRebuildTasks(ctx context.Context, id int32) (*pb.MultiTaskDescription, error) {
	entry := log.WithFields(log.Fields{Function: "GetSCRebuildTasks", RebuilderID: id})
	rbNode := rebuilder.NodeManager.GetNode(id)
	if rbNode == nil {
		err := fmt.Errorf("node %d not found", id)
		entry.WithError(err).Error("fetch rebuilder miner")
		return nil, err
	}
	startTime := time.Now().UnixNano()
	//如果被分配重建任务的矿机状态大于1或者权重为零则不分配重建任务
	if rbNode.Rebuilding > 200 || rbNode.Status != 1 || rbNode.Valid == 0 || rbNode.Weight < float64(rebuilder.Params.WeightThreshold) || rbNode.AssignedSpace <= 0 || rbNode.Quota <= 0 || rbNode.Version < int32(rebuilder.Params.MinerVersionThreshold) {
		entry.WithError(ErrInvalidNode).Debugf("status of rebuilder miner is %d, weight is %f, rebuilding is %d, version is %d", rbNode.Status, rbNode.Weight, rbNode.Rebuilding, rbNode.Version)
		return nil, ErrInvalidNode
	}
	var sctask *SCRebuildPkg
	select {
	case t, ok := <-rebuilder.scTaskCh:
		if !ok {
			err := errors.New("self check task chan is closed")
			entry.Debug("self check task chan is closed")
			return nil, err
		}
		sctask = t
		if sctask.Timestamp == 0 {
			time.Sleep(time.Duration(500) * time.Millisecond)
		}
	default:
		entry.Debug("no self check task get")
	}
	if sctask == nil {
		return nil, ErrNoTaskAlloc
	}
	if sctask.Timestamp < time.Now().UnixNano()-int64(rebuilder.Params.RebuildShardExpiredTime)*1000000000*4/5 {
		return nil, ErrTaskTimeout
	}

	expiredTime := sctask.Timestamp/1000000000 + int64(rebuilder.Params.RebuildShardExpiredTime)
	//一个任务包，多个分片的重建任务均在该任务包内
	tasks := new(pb.MultiTaskDescription)
	//rbshards为RebuildShard结构体数组
	for _, shard := range sctask.Shards {
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
				if !rebuilder.Params.RemoveMinerAddrs {
					loc.Addrs = []string{"/ip4/127.0.0.1/tcp/59999"}
				}
			} else {
				loc.NodeId = n1.NodeID
				if !rebuilder.Params.RemoveMinerAddrs {
					loc.Addrs = n1.Addrs
				}
			}
			if n2 == nil {
				//如果矿机不存在则制造一个假地址
				entry.Debugf("invalid node2 for shard %d/%d: nodeId %d", shard.ID, idx, id.NodeID2)
				loc.NodeId2 = "16Uiu2HAmKg7EXBqx3SXbE2XkqbPLft8NGkzQcsbJymVB9uw7fW1r"
				if !rebuilder.Params.RemoveMinerAddrs {
					loc.Addrs2 = []string{"/ip4/127.0.0.1/tcp/59999"}
				}
			} else {
				loc.NodeId2 = n2.NodeID
				if !rebuilder.Params.RemoveMinerAddrs {
					loc.Addrs2 = n2.Addrs
				}
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
				if sctask.MinerId == shard.MinerID { //如果被重建分片的NodeID为被重建矿机，则使用NodeID2作为重建备份节点
					backNode := rebuilder.NodeManager.GetNode(shard.MinerID2)
					if backNode != nil {
						task.BackupLocation = &pb.P2PLocation{NodeId: backNode.NodeID}
						if !rebuilder.Params.RemoveMinerAddrs {
							task.BackupLocation.Addrs = backNode.Addrs
						}
					}
				} else if sctask.MinerId == shard.MinerID2 { //如果被重建分片的NodeID2为被重建矿机，则使用NodeID作为重建备份节点
					backNode := rebuilder.NodeManager.GetNode(shard.MinerID)
					if backNode != nil {
						task.BackupLocation = &pb.P2PLocation{NodeId: backNode.NodeID}
						if !rebuilder.Params.RemoveMinerAddrs {
							task.BackupLocation.Addrs = backNode.Addrs
						}
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
	tasks.SrcNodeID = sctask.MinerId
	//设置超时时间（相对时间，单位为秒）
	tasks.ExpiredTimeGap = int32(rebuilder.Params.RebuildShardExpiredTime)
	entry.WithField(MinerID, sctask.MinerId).Debugf("length of task list is %d, expired time is %d, total time: %dms", len(tasks.Tasklist), tasks.ExpiredTime, (time.Now().UnixNano()-startTime)/1000000)
	return tasks, nil
}
