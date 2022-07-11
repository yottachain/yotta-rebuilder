package ytrebuilder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	proto "github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	pb "github.com/yottachain/yotta-rebuilder/pbrebuilder"
)

func (rebuilder *Rebuilder) processSelfCheckShards(ctx context.Context) {
	entry := log.WithFields(log.Fields{Function: "processSelfCheckShards"})
	query := `{"query": {"match_all" : {}},"size": 100}`
	var b strings.Builder
	b.WriteString(query)
	read := strings.NewReader(b.String())
	for {
		res, err := rebuilder.ESClient.Search(
			rebuilder.ESClient.Search.WithContext(ctx),
			rebuilder.ESClient.Search.WithIndex("verifyerr"),
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
		log.Debugf("document hits: %d\n", hitCount.Int())
		vals := gjson.Get(resb.String(), "hits.hits")
		requests := make([]*NodeRebuildRequest, 0, hitCount.Int())
		err = json.NewDecoder(strings.NewReader(vals.String())).Decode(&requests)
		if err != nil {
			entry.WithError(err).Error("decode search response to json failed")
			res.Body.Close()
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		for _, req := range requests {
			delRes, err := rebuilder.ESClient.Delete("verifyerr", req.ID)
			if err != nil {
				entry.WithError(err).Errorf("delete self check shard failed: %d", req.Source.Log.MinerId)
				continue
			}
			delRes.Body.Close()
			err = rebuilder.BuildSelfCheckTasks(ctx, req)
			if err != nil {
				entry.WithError(err).Error("build self check rebuild task failed")
				continue
			}
		}
		res.Body.Close()
	}
}

func (rebuilder *Rebuilder) BuildSelfCheckTasks(ctx context.Context, request *NodeRebuildRequest) error {
	entry := log.WithFields(log.Fields{Function: "BuildSelfCheckTasks", MinerID: request.Source.Log.MinerId})
	tgtNode := rebuilder.NodeManager.GetNode(int32(request.Source.Log.MinerId))
	if tgtNode == nil {
		err := errors.New("target node not found")
		entry.WithError(err).Error("fetch target miner")
		return err
	}
	if tgtNode.Status == 2 || tgtNode.Status == 3 {
		err := errors.New("target node is under rebuilding")
		entry.WithError(err).Error("fetch target miner")
		return err
	}
	shards := make([]*Shard, 0)
	for _, v := range request.Source.Log.ErrShards {
		if v.ShardId == "" || v.ShardId == "0" {
			continue
		}
		shardID, err := strconv.ParseInt(v.ShardId, 10, 64)
		if err != nil {
			entry.WithError(err).Errorf("decode shard ID %s failed", v.ShardId)
			continue
		}
		shard, err := FindNodeShard(ctx, rebuilder.tikvCli, shardID, int32(request.Source.Log.MinerId))
		if err != nil {
			entry.WithError(err).Errorf("find node shard %s failed", v.ShardId)
			continue
		}
		shards = append(shards, shard)
	}
	if len(shards) == 0 {
		entry.WithError(ErrNoTaskAlloc).Error("find no node shard from tikv")
		return ErrNoTaskAlloc
	}
	bindexes := make([]uint64, 0)
	for _, s := range shards {
		bindexes = append(bindexes, s.BIndex)
	}
	var err error
	blocksMap, err := GetBlocks(rebuilder.httpCli, rebuilder.Compensation.SyncClientURL, bindexes)
	if err != nil {
		entry.WithError(err).Error("fetching blocks")
		return err
	}
	rebuildShards := make([]*RebuildShard, 0)
	idx := 0
OUTER:
	for _, shard := range shards {
		rshard := new(RebuildShard)
		rshard.ID = shard.ID
		rshard.BlockID = shard.ID - int64(shard.Offset)
		rshard.MinerID = shard.NodeID
		rshard.MinerID2 = shard.NodeID2
		rshard.VHF = shard.VHF
		rshard.SNID = snIDFromID(uint64(shard.ID))

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
		idx++
		if i == int64(rebuilder.Params.RebuildShardMinerTaskBatchSize) {
			idx = 0
			pkg := &SCRebuildPkg{MinerId: int32(request.Source.Log.MinerId), Shards: rebuildShards}
			rebuildShards = make([]*RebuildShard, 0)
			rebuilder.scTaskCh <- pkg
			entry.Infof("put self check shards task to chan,size: %d", len(rebuildShards))
		}
	}
	if len(rebuildShards) != 0 {
		pkg := &SCRebuildPkg{MinerId: int32(request.Source.Log.MinerId), Shards: rebuildShards}
		rebuilder.scTaskCh <- pkg
		entry.Infof("put self check shards task to chan,size: %d", len(rebuildShards))
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
	if rbNode.Rebuilding > 1 || rbNode.Status != 1 || rbNode.Valid == 0 || rbNode.Weight < float64(rebuilder.Params.WeightThreshold) || rbNode.AssignedSpace <= 0 || rbNode.Quota <= 0 || rbNode.Version < int32(rebuilder.Params.MinerVersionThreshold) {
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
	default:
		entry.Debug("no self check task get")
	}
	if sctask == nil {
		return nil, errors.New("no self check task get")
	}

	expiredTime := time.Now().Unix() + int64(rebuilder.Params.RebuildShardExpiredTime)
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
				if sctask.MinerId == shard.MinerID { //如果被重建分片的NodeID为被重建矿机，则使用NodeID2作为重建备份节点
					backNode := rebuilder.NodeManager.GetNode(shard.MinerID2)
					if backNode != nil {
						task.BackupLocation = &pb.P2PLocation{NodeId: backNode.NodeID, Addrs: backNode.Addrs}
					}
				} else if sctask.MinerId == shard.MinerID2 { //如果被重建分片的NodeID2为被重建矿机，则使用NodeID作为重建备份节点
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
	tasks.SrcNodeID = sctask.MinerId
	//设置超时时间（相对时间，单位为秒）
	tasks.ExpiredTimeGap = int32(rebuilder.Params.RebuildShardExpiredTime)
	entry.WithField(MinerID, sctask.MinerId).Debugf("length of task list is %d, expired time is %d, total time: %dms", len(tasks.Tasklist), tasks.ExpiredTime, (time.Now().UnixNano()-startTime)/1000000)
	return tasks, nil
}
