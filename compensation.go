package ytrebuilder

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/ivpusic/grpool"
	log "github.com/sirupsen/logrus"
	net "github.com/yottachain/YTCoreService/net"
	pkt "github.com/yottachain/YTCoreService/pkt"
	pb "github.com/yottachain/yotta-rebuilder/pbrebuilder"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

//ShardRebuildMeta metadata of shards rebuilt
type ShardRebuildMeta struct {
	ID          int64 `json:"_id"`
	VFI         int64 `json:"VFI"`
	DestMinerID int32 `json:"nid"`
	SrcMinerID  int32 `json:"sid"`
}

//BlockItem struct
type BlockItem struct {
	ID     int64        `bson:"_id" json:"_id"`
	VHP    []byte       `bson:"VHP" json:"VHP"`
	VHB    []byte       `bson:"VHB" json:"VHB"`
	KED    []byte       `bson:"KED" json:"KED"`
	VNF    int32        `bson:"VNF" json:"VNF"`
	AR     int32        `bson:"AR" json:"AR"`
	Shards []*ShardItem `bson:"-" json:"shards"`
}

//ShardItem struct
type ShardItem struct {
	ID      int64  `bson:"_id" json:"_id"`
	NodeID  int32  `bson:"nodeId" json:"nid"`
	VHF     []byte `bson:"VHF" json:"VHF"`
	BlockID int64  `bson:"blockid" json:"bid"`
	NodeID2 int32  `bson:"nodeId2" json:"nid2"`
}

type DataResp struct {
	SNID   int          `json:"SN"`
	Blocks []*BlockItem `json:"blocks"`
	From   int64        `json:"from"`
	Next   int64        `json:"next"`
	Size   int64        `json:"size"`
	More   bool         `json:"more"`
}

//ShardsRebuild struct
type ShardsRebuild struct {
	ShardsRebuild []*ShardRebuildMeta
	More          bool
	Next          int64
}

//CPSRecord struct
type CPSRecord struct {
	ID        int32 `bson:"_id"`
	Start     int64 `bson:"start"`
	Timestamp int64 `bson:"timestamp"`
}

//BSFRecord struct
type BSFRecord struct {
	ID      int32 `bson:"_id"`
	Start   int64 `bson:"start"`
	Current int64 `bson:"current"`
	//StartTime int64 `bson:"starttime"`
	LastTime int64 `bson:"lasttime"`
	//EndTime  int64 `bson:"endtime"`
}

//GetRebuildShards find rebuilt shards
func GetRebuildShards(httpCli *http.Client, url string, from int64, count int, skipTime int64) (*ShardsRebuild, error) {
	entry := log.WithFields(log.Fields{Function: "GetRebuildShards"})
	count++
	fullURL := fmt.Sprintf("%s/sync/getShardRebuildMetas?start=%d&count=%d", url, from, count)
	entry.Debugf("fetching rebuilt shards by URL: %s", fullURL)

	request, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		entry.WithError(err).Errorf("create request failed: %s", fullURL)
		return nil, err
	}
	request.Header.Add("Accept-Encoding", "gzip")
	resp, err := httpCli.Do(request)
	if err != nil {
		entry.WithError(err).Errorf("get rebuilt shards meta failed: %s", fullURL)
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
	response := make([]*ShardRebuildMeta, 0)
	err = json.NewDecoder(reader).Decode(&response)
	if err != nil {
		entry.WithError(err).Errorf("decode rebuilt shards metadata failed: %s", fullURL)
		return nil, err
	}
	skipByte32 := Int32ToBytes(int32(skipTime))
	padding := []byte{0x00, 0x00, 0x00, 0x00}
	skipTime64 := BytesToInt64(append(skipByte32, padding...))
	i := 0
	for _, item := range response {
		if item.ID > skipTime64 {
			break
		}
		i++
	}
	response = response[0:i]
	next := int64(0)
	entry.Debugf("fetched %d shards rebuilt", len(response))
	if len(response) == count {
		next = response[count-1].ID
		return &ShardsRebuild{ShardsRebuild: response[0 : count-1], More: true, Next: next}, nil
	}
	return &ShardsRebuild{ShardsRebuild: response, More: false, Next: 0}, nil
}

// //Preprocess dealing with orphan shards which has committed by SN but not tagged in rebuild service
// func (rebuilder *Rebuilder) Preprocess(ctx context.Context) {
// 	entry := log.WithFields(log.Fields{Function: "Preprocess"})
// 	urls := rebuilder.Compensation.AllSyncURLs
// 	snCount := len(urls)
// 	//collectionRS := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(RebuildShardTab)
// 	collectionCR := rebuilder.rebuilderdbClient.Database(RebuilderDB).Collection(CPSRecordTab)
// 	var wg sync.WaitGroup
// 	wg.Add(snCount)
// 	for i := 0; i < snCount; i++ {
// 		snID := int32(i)
// 		go func() {
// 			entry.Infof("starting preprocess SN%d", snID)
// 			for {
// 				record := new(CPSRecord)
// 				err := collectionCR.FindOne(ctx, bson.M{"_id": snID}).Decode(record)
// 				if err != nil {
// 					if err == mongo.ErrNoDocuments {
// 						record = &CPSRecord{ID: snID, Start: 0, Timestamp: time.Now().Unix()}
// 						_, err := collectionCR.InsertOne(ctx, record)
// 						if err != nil {
// 							entry.WithError(err).Errorf("insert compensation record: %d", snID)
// 							time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
// 							continue
// 						}
// 					} else {
// 						entry.WithError(err).Errorf("find compensation record: %d", snID)
// 						time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
// 						continue
// 					}
// 				}
// 				shardsRebuilt, err := GetRebuildShards(rebuilder.httpCli, rebuilder.Compensation.AllSyncURLs[snID], record.Start, rebuilder.Compensation.BatchSize, time.Now().Unix()-int64(rebuilder.Compensation.SkipTime))
// 				if err != nil {
// 					time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
// 					continue
// 				}
// 				for _, sr := range shardsRebuilt.ShardsRebuild {
// 					entry.WithField(ShardID, sr.VFI).Debug("shard rebuilt")
// 				}
// 				if shardsRebuilt.More {
// 					collectionCR.UpdateOne(ctx, bson.M{"_id": snID}, bson.M{"$set": bson.M{"start": shardsRebuilt.Next, "timestamp": time.Now().Unix()}})
// 				} else {
// 					if len(shardsRebuilt.ShardsRebuild) > 0 {
// 						next := shardsRebuilt.ShardsRebuild[len(shardsRebuilt.ShardsRebuild)-1].ID + 1
// 						collectionCR.UpdateOne(ctx, bson.M{"_id": snID}, bson.M{"$set": bson.M{"start": next, "timestamp": time.Now().Unix()}})
// 					}
// 					break
// 				}
// 			}
// 			entry.Infof("finished pre-process SN%d", snID)
// 			wg.Done()
// 		}()
// 	}
// 	wg.Wait()
// }

//Compensate dealing with orphan shards which has been committed by SN when service is running
func (rebuilder *Rebuilder) Compensate(ctx context.Context) {
	entry := log.WithFields(log.Fields{Function: "Compensate"})
	urls := rebuilder.Compensation.AllSyncURLs
	snCount := len(urls)
	collectionCR := rebuilder.RebuilderdbClient.Database(RebuilderDB).Collection(CPSRecordTab)
	for i := 0; i < snCount; i++ {
		snID := int32(i)
		go func() {
			entry.Infof("starting compensate SN%d", snID)
			for {
				record := new(CPSRecord)
				err := collectionCR.FindOne(ctx, bson.M{"_id": snID}).Decode(record)
				if err != nil {
					if err == mongo.ErrNoDocuments {
						record = &CPSRecord{ID: snID, Start: 0, Timestamp: time.Now().Unix()}
						_, err := collectionCR.InsertOne(ctx, record)
						if err != nil {
							entry.WithError(err).Errorf("insert compensation record: %d", snID)
							time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
							continue
						}
					} else {
						entry.WithError(err).Errorf("find compensation record: %d", snID)
						time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
						continue
					}
				}
				hcli := rebuilder.HttpCli
				// if strings.HasPrefix(rebuilder.Compensation.AllSyncURLs[snID], "https") {
				// 	hcli = rebuilder.httpCli2
				// }
				shardsRebuilt, err := GetRebuildShards(hcli, rebuilder.Compensation.AllSyncURLs[snID], record.Start, rebuilder.Compensation.BatchSize, time.Now().Unix()-int64(rebuilder.Compensation.SkipTime))
				if err != nil {
					time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
					continue
				}
				for _, sr := range shardsRebuilt.ShardsRebuild {
					entry.WithField(ShardID, sr.VFI).Debug("shard rebuilt")
				}
				if shardsRebuilt.More {
					collectionCR.UpdateOne(ctx, bson.M{"_id": snID}, bson.M{"$set": bson.M{"start": shardsRebuilt.Next, "timestamp": time.Now().Unix()}})
				} else {
					if len(shardsRebuilt.ShardsRebuild) > 0 {
						next := shardsRebuilt.ShardsRebuild[len(shardsRebuilt.ShardsRebuild)-1].ID + 1
						collectionCR.UpdateOne(ctx, bson.M{"_id": snID}, bson.M{"$set": bson.M{"start": next, "timestamp": time.Now().Unix()}})
					}
					time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
				}
			}
		}()
	}
}

//GetBlockData find block data
func GetBlockData(httpCli *http.Client, url string, from int64, size int, skip int) (*DataResp, error) {
	entry := log.WithFields(log.Fields{Function: "GetBlockData"})
	fullURL := fmt.Sprintf("%s/sync/getSyncData?from=%d&size=%d&skip=%d&bsonly=true", url, from, size, skip)
	entry.Debugf("fetching blocks data by URL: %s", fullURL)
	request, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		entry.WithError(err).Errorf("create request failed: %s", fullURL)
		return nil, err
	}
	request.Header.Add("Accept-Encoding", "gzip")
	resp, err := httpCli.Do(request)
	if err != nil {
		entry.WithError(err).Errorf("get sync data failed: %s", fullURL)
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
	response := new(DataResp)
	err = json.NewDecoder(reader).Decode(response)
	if err != nil {
		entry.WithError(err).Errorf("decode sync data failed: %s", fullURL)
		return nil, err
	}
	return response, nil
}

//ShardsFix fix shards without nodeID2
func (rebuilder *Rebuilder) ShardsFix(ctx context.Context) {
	entry := log.WithFields(log.Fields{Function: "ShardsFix"})
	pool := grpool.NewPool(10000, 10000)
	urls := rebuilder.Compensation.AllSyncURLs
	snCount := len(urls)
	collectionBSF := rebuilder.RebuilderdbClient.Database(RebuilderDB).Collection(BSFRecordTab)
	for i := 0; i < snCount; i++ {
		snID := int32(i)
		go func() {
			entry.Infof("starting shards fixing SN%d", snID)
			for {
				record := new(BSFRecord)
				err := collectionBSF.FindOne(ctx, bson.M{"_id": snID}).Decode(record)
				if err != nil {
					if err == mongo.ErrNoDocuments {
						record = &BSFRecord{ID: snID, Start: 0, Current: 0, LastTime: 0}
						_, err := collectionBSF.InsertOne(ctx, record)
						if err != nil {
							entry.WithError(err).Errorf("insert shard fixing record: %d", snID)
							time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
							continue
						}
					} else {
						entry.WithError(err).Errorf("find shard fixing record: %d", snID)
						time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
						continue
					}
				}

				flag := record.Start != record.Current
				fixMap := make(map[int32][]*ShardItem)

				for {
					hcli := rebuilder.HttpCli
					// if strings.HasPrefix(rebuilder.Compensation.AllSyncURLs[snID], "https") {
					// 	hcli = rebuilder.httpCli2
					// }
					resp, err := GetBlockData(hcli, rebuilder.Compensation.AllSyncURLs[snID], record.Current, rebuilder.Compensation.BatchSize, rebuilder.Compensation.SkipTime)
					if err != nil {
						entry.WithError(err).Errorf("Get block data of SN%d", snID)
						time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
						continue
					}
					for _, block := range resp.Blocks {
						for _, shard := range block.Shards {
							if shard.NodeID2 == 0 && shard.NodeID != 0 {
								flag = true
								fixMap[shard.NodeID] = append(fixMap[shard.NodeID], shard)
							}
						}
					}
					maxlen := 0
					for _, ss := range fixMap {
						if len(ss) >= maxlen {
							maxlen = len(ss)
						}
					}
					if maxlen >= rebuilder.Params.RebuildShardMinerTaskBatchSize || (maxlen > 0 && !resp.More) {
						_, err := collectionBSF.UpdateOne(ctx, bson.M{"_id": snID}, bson.M{"$set": bson.M{"current": resp.Next}})
						if err != nil {
							entry.WithError(err).Errorf("update current of SN%d", snID)
						}
						//record.Current = resp.Next
						//TODO: 组装任务，发给矿机，先改current再发送为了防止重启后重复发送同一批数据

						for minerID, rbshards := range fixMap {
							//一个任务包，多个分片的重建任务均在该任务包内
							tasks := new(pb.MultiTaskDescription)
							tasks.Type = 1
							//rbshards为RebuildShard结构体数组
							//for _, shard := range rbshards {
							for j := 0; j < len(rbshards); j++ {
								shard := rbshards[j]
								entry.Debugf("fetch shard %d: nodeId %d/%d", shard.ID, shard.NodeID, shard.NodeID2)
								//被重建分片的全部关联分片（包括自身）的MD5哈希（仅当status=2且分片类型为0x68b3，即LRC分片时才需要该值，否则为空）
								//hashs := shard.Hashs
								//所有关联分片所在矿机的P2P地址（只使用NodeID，没有NodeID2的，为和旧代码兼容）
								locations := make([]*pb.P2PLocation, 0)
								loc := new(pb.P2PLocation)
								n := rebuilder.NodeManager.GetNode(shard.NodeID)
								if n == nil {
									loc.NodeId = "16Uiu2HAmKg7EXBqx3SXbE2XkqbPLft8NGkzQcsbJymVB9uw7fW1r"
									if !rebuilder.Params.RemoveMinerAddrs {
										loc.Addrs = []string{"/ip4/127.0.0.1/tcp/59999"}
									}
								} else {
									loc.NodeId = n.NodeID
									if !rebuilder.Params.RemoveMinerAddrs {
										loc.Addrs = n.Addrs
									}
								}
								locations = append(locations, loc)
								var b []byte
								var err error
								task := new(pb.TaskDescriptionCP)
								//拼接任务ID
								task.Id = append(Int64ToBytes(shard.ID), Uint16ToBytes(uint16(snID))...)
								//被重建分片的MD5哈希
								task.DataHash = shard.VHF
								//被重建分片的全部副本所在Node的地址列表
								task.Locations = locations
								//序列化任务为字节数组
								b, err = proto.Marshal(task)
								if err != nil {
									entry.WithField(MinerID, shard.NodeID).WithField(ShardID, shard.ID).WithError(err).Errorf("marshaling replication task: %d", shard.ID)
									continue
								}
								//将分片类型加到重建数据前面，固定为副本类型
								btask := append(Uint16ToBytes(uint16(0xc258)), b...)
								//将组装好的一个重建任务加入任务包
								tasks.Tasklist = append(tasks.Tasklist, btask)

								if len(tasks.Tasklist) == rebuilder.Params.RebuildShardMinerTaskBatchSize || j == len(rbshards)-1 {
									var execNode *Node
									for {
										qctx, cancel := context.WithTimeout(ctx, 3*time.Second)
										ele, err := rebuilder.taskSender.queue.DequeueOrWaitForNextElementContext(qctx)
										cancel()
										if err != nil {
											entry.WithError(err).Debug("get node from queue")
											continue
										}
										execNode = ele.(*Node)
										entry.WithField(MinerID, execNode.ID).Infof("pop rebuilding node: %d", execNode.ID)
										if execNode.Rebuilding > 200 || execNode.Status != 1 || execNode.Valid == 0 || execNode.Weight < float64(rebuilder.Params.WeightThreshold) || execNode.AssignedSpace <= 0 || execNode.Quota <= 0 || execNode.Version < int32(rebuilder.Params.MinerVersionThreshold) || time.Now().Unix()-execNode.Timestamp > 300 {
											entry.WithField(MinerID, execNode.ID).Info("invalid rebuilding node")
											rebuilder.taskSender.m.Delete(execNode.ID)
											continue
										}
										if execNode.ID == minerID {
											entry.WithField(MinerID, execNode.ID).Info("conflict rebuilding node")
											rebuilder.taskSender.m.Delete(execNode.ID)
											continue
										}
										break
									}
									pool.JobQueue <- (func(tlist [][]byte, enode *Node, mid, msgType int32) func() {
										return func() {
											n := &net.Node{Id: enode.ID, Nodeid: enode.NodeID, Pubkey: enode.PubKey, Addrs: enode.Addrs}
											req := &pkt.TaskList{Tasklist: tlist, ExpiredTime: time.Now().Unix() + int64(rebuilder.Params.RebuildShardExpiredTime), SrcNodeID: mid, ExpiredTimeGap: int32(rebuilder.Params.RebuildShardExpiredTime), Padding: msgType}
											_, e := net.RequestDN(req, n, false)
											if e != nil {
												entry.WithField(MinerID, enode.ID).WithField("SrcNodeID", mid).WithError(err).Errorf("Send rebuild task failed: %d--%s", e.Code, e.Msg)
											} else {
												entry.WithField(MinerID, enode.ID).WithField("SrcNodeID", mid).Infof("Send rebuild task OK,count %d", len(tlist))
											}
											rebuilder.taskSender.m.Delete(enode.ID)
										}
									})(tasks.Tasklist, execNode, minerID, tasks.Type)
									tasks = new(pb.MultiTaskDescription)
									tasks.Type = 1
								}
							}
							//任务超时时间（绝对时间），为当前时间加上预设的任务超时时间，单位是秒
							//expiredTime := time.Now().Unix() + int64(rebuilder.Params.RebuildShardExpiredTime)
							//设置超时时间
							//tasks.ExpiredTime = expiredTime
							//设置分片所属原矿机ID
							//tasks.SrcNodeID = minerID
							//设置超时时间（相对时间，单位为秒）
							//tasks.ExpiredTimeGap = int32(rebuilder.Params.RebuildShardExpiredTime)

							// var execNode *Node
							// for {
							// 	ele, err := rebuilder.taskSender.queue.DequeueOrWaitForNextElement()
							// 	if err != nil {
							// 		entry.WithError(err).Error("get node from queue")
							// 		continue
							// 	}
							// 	execNode = ele.(*Node)
							// 	if execNode.Rebuilding > 1 || execNode.Status != 1 || execNode.Valid == 0 || execNode.Weight < float64(rebuilder.Params.WeightThreshold) || execNode.AssignedSpace <= 0 || execNode.Quota <= 0 || execNode.Version < int32(rebuilder.Params.MinerVersionThreshold) || time.Now().Unix()-execNode.Timestamp > 300 {
							// 		entry.WithField(MinerID, execNode.ID).Info("invalid rebuilding node")
							// 		rebuilder.taskSender.m.Delete(execNode.ID)
							// 		continue
							// 	}
							// 	if execNode.ID == minerID {
							// 		entry.WithField(MinerID, execNode.ID).Info("conflict rebuilding node")
							// 		continue
							// 	}
							// 	break
							// }
							// pool.JobQueue <- func() {
							// 	n := &net.Node{Id: execNode.ID, Nodeid: execNode.NodeID, Pubkey: execNode.PubKey, Addrs: execNode.Addrs}
							// 	req := &pkt.TaskList{Tasklist: tasks.Tasklist, ExpiredTime: time.Now().Unix() + int64(rebuilder.Params.RebuildShardExpiredTime), SrcNodeID: minerID, ExpiredTimeGap: int32(rebuilder.Params.RebuildShardExpiredTime)}
							// 	_, e := net.RequestDN(req, n, "")
							// 	if e != nil {
							// 		entry.WithField(MinerID, execNode.ID).WithField("SrcNodeID", minerID).WithError(err).Errorf("Send rebuild task failed: %d--%s", e.Code, e.Msg)
							// 	} else {
							// 		entry.WithField(MinerID, execNode.ID).WithField("SrcNodeID", minerID).Infof("Send rebuild task OK,count %d", len(tasks.Tasklist))
							// 	}
							// 	rebuilder.taskSender.m.Delete(execNode.ID)
							// }
						}

						lasttime := time.Now().Unix()
						_, err = collectionBSF.UpdateOne(ctx, bson.M{"_id": snID}, bson.M{"$set": bson.M{"lasttime": lasttime}})
						if err != nil {
							entry.WithError(err).Errorf("update lasttime of SN%d", snID)
						}
						record.LastTime = lasttime
						fixMap = make(map[int32][]*ShardItem)
					}
					if maxlen == 0 && !flag {
						_, err := collectionBSF.UpdateOne(ctx, bson.M{"_id": snID}, bson.M{"$set": bson.M{"start": resp.Next, "current": resp.Next}})
						if err != nil {
							entry.WithError(err).Errorf("update start and current of SN%d", snID)
							time.Sleep(time.Duration(rebuilder.Compensation.WaitTime) * time.Second)
							continue
						}
						record.Start = resp.Next
					}
					record.Current = resp.Next
					if !resp.More {
						break
					}
				}
				sleepTime := int64(rebuilder.Params.RebuildShardExpiredTime) - (time.Now().Unix() - record.LastTime)
				if sleepTime > 0 {
					time.Sleep(time.Duration(sleepTime) * time.Second)
				}
				_, err = collectionBSF.UpdateOne(ctx, bson.M{"_id": snID}, bson.M{"$set": bson.M{"current": record.Start, "lasttime": 0}})
				if err != nil {
					entry.WithError(err).Errorf("reset lasttime of SN%d", snID)
				}
			}
		}()
	}
}

//GetShards find shard data
func GetShards(httpCli *http.Client, url string, shardIDs []int64) ([]*Shard, error) {
	entry := log.WithFields(log.Fields{Function: "GetShards"})
	shardIDStrs := make([]string, 0)
	for _, id := range shardIDs {
		idstr := strconv.FormatInt(id, 10)
		shardIDStrs = append(shardIDStrs, idstr)
	}
	fullURL := fmt.Sprintf("%s/sync/GetShards?ids=%s", url, strings.Join(shardIDStrs, ","))
	entry.Debugf("fetching shards data by URL: %s", fullURL)
	request, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		entry.WithError(err).Errorf("create request failed: %s", fullURL)
		return nil, err
	}
	request.Header.Add("Accept-Encoding", "gzip")
	resp, err := httpCli.Do(request)
	if err != nil {
		entry.WithError(err).Errorf("get shards data failed: %s", fullURL)
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
	response := make([]*Shard, 0)
	err = json.NewDecoder(reader).Decode(&response)
	if err != nil {
		entry.WithError(err).Errorf("decode shard data failed: %s", fullURL)
		return nil, err
	}
	return response, nil
}
