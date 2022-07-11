package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	ytrebuilder "github.com/yottachain/yotta-rebuilder"
	"github.com/yottachain/yotta-rebuilder/cmd"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	cmd.Execute()
}

func main0() {
	mongoURL := os.Args[1] //要校验的SN数据库URL,例如mongodb://192.168.1.145:27137
	tikvURL := os.Args[2]  //要校验的tikv连接URL，例如192.168.1.21:2379
	syncURL := os.Args[3]  //同步服务的URL，例如http://127.0.0.1:8080
	//创建tikv API客户端
	tikvCli, err := rawkv.NewClient(context.Background(), []string{tikvURL}, config.Default())
	if err != nil {
		panic(err)
	}
	//创建mongoDB客户端
	mongoCli, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		panic(err)
	}
	//创建http客户端，用于调用同步服务的数组文件接口获取block信息
	httpCli := &http.Client{}
	collectionB := mongoCli.Database("metabase").Collection("blocks")
	collectionS := mongoCli.Database("metabase").Collection("shards")
	//首先遍历mongoDB中的blocks表，以该表数据为基准校验其他数据
	cur, err := collectionB.Find(context.Background(), bson.M{})
	if err != nil {
		panic(err)
	}
	for cur.Next(context.Background()) {
		block := new(ytrebuilder.Block)
		err := cur.Decode(block)
		if err != nil {
			panic(err)
		}
		fmt.Printf("fetch block %d in mongo, VNF: %d, AR: %d\n", block.ID, block.VNF, block.AR)
		//根据当前遍历到的block ID去tikv中反查在数组文件中的索引bindex（需要查询ShardMetaMsg数据）
		meta, err := ytrebuilder.FindShardMeta(context.Background(), tikvCli, uint64(block.ID))
		if err != nil {
			panic(err)
		}
		if meta.Id == 0 {
			fmt.Printf(fmt.Sprintf("block %d not found in tikv\n", block.ID))
			continue
		}
		fmt.Printf("fetch block %d in tikv,  VNF: %d, AR: %d, BIndex: %d, Offset: %d\n", meta.Id, meta.Vnf, meta.Ar, meta.Bindex, meta.Offset)
		//根据反查到的bindex去数组文件查询block数据
		bm, err := ytrebuilder.GetBlocks(httpCli, syncURL, []uint64{meta.Bindex})
		if err != nil {
			panic(err)
		}
		if len(bm) == 0 {
			panic(fmt.Sprintf("block %d not found in arraybase: %d\n", block.ID, meta.Bindex))
		}
		blockAB := bm[meta.Bindex]
		if blockAB.ID == 0 {
			fmt.Printf("	block %d is deleted\n", block.ID)
			continue
		}
		fmt.Printf("fetch block %d in array, VNF: %d, AR: %d, ShardLength: %d\n", blockAB.ID, blockAB.VNF, blockAB.AR, len(blockAB.Shards))
		//在mongoDB中遍历当前block关联到的所有shard数据（关联shard的ID范围是从block ID到block ID+VNF-1）
		cur2, err := collectionS.Find(context.Background(), bson.M{"_id": bson.M{"$gte": block.ID, "$lt": block.ID + int64(block.VNF)}})
		if err != nil {
			panic(err)
		}
		ok := true
		i := 0
		//便利shards并与数组文件查询出的对应shard对比
		for cur2.Next(context.Background()) {
			shard := new(ytrebuilder.Shard2)
			err := cur2.Decode(shard)
			if err != nil {
				panic(err)
			}
			fmt.Printf("	shard %d in mongo, VHF: %s, NodeID: %d, NodeID2: %d\n", shard.ID, hex.EncodeToString(shard.VHF), shard.NodeID, shard.NodeID2)
			shardAB := blockAB.Shards[i]
			//node ID不一致的情况
			if shard.NodeID != int32(shardAB.NodeID) || shard.NodeID2 != int32(shardAB.NodeID2) {
				fmt.Printf("	shard %d not match: %d %d / %d %d\n", shard.ID, shard.NodeID, shard.NodeID, shardAB.NodeID, shardAB.NodeID2)
				ok = false
			}
			//VHF不一致的情况
			if !bytes.Equal(shard.VHF, shardAB.VHF) {
				fmt.Printf("	shard %d not match between tikv and arraybase: %s\n", shard.ID, hex.EncodeToString(shardAB.VHF))
				ok = false
			}
			//如果nodeId不为0，再和tikv中对应的ShardMsg数据对比
			if shardAB.NodeID != 0 {
				s1, err := ytrebuilder.FindNodeShard2(context.Background(), tikvCli, uint64(shard.ID), shardAB.NodeID)
				if err != nil || s1 == nil {
					fmt.Printf("	shard %d not found in miner1 %d: %s\n", shard.ID, shardAB.NodeID, err.Error())
					ok = false
				}
				//VHF不一致的情况
				if !bytes.Equal(s1.Vhf, shardAB.VHF) {
					fmt.Printf("	shard %d not match in miner1 %d: %s\n", shard.ID, shardAB.NodeID, hex.EncodeToString(s1.Vhf))
					ok = false
				}
			}
			//如果nodeId2不为0，再和tikv中对应的ShardMsg数据对比
			if shardAB.NodeID2 != 0 {
				s2, err := ytrebuilder.FindNodeShard2(context.Background(), tikvCli, uint64(shard.ID), shardAB.NodeID2)
				if err != nil || s2 == nil {
					fmt.Printf("	shard %d not found in miner2 %d: %s\n", shard.ID, shardAB.NodeID2, err.Error())
					ok = false
				}
				//VHF不一致的情况
				if !bytes.Equal(s2.Vhf, shardAB.VHF) {
					fmt.Printf("	shard %d not match in miner2 %d: %s\n", shard.ID, shardAB.NodeID2, hex.EncodeToString(s2.Vhf))
					ok = false
				}
			}
			i++
		}
		if i != int(block.VNF) {
			fmt.Printf("	shards length of block %d mismatch: %d\n", block.ID, i)
		}
		if ok {
			fmt.Printf("block %d is correct\n\n", block.ID)
		} else {
			fmt.Printf("block %d is incorrect\n\n", block.ID)
		}
	}
}

func main1() {
	urls := []string{os.Args[1]}
	tikvCli, err := rawkv.NewClient(context.TODO(), urls, config.Default())
	if err != nil {
		panic(err)
	}
	// if os.Args[2] == "block" {
	// 	blockID, err := strconv.ParseInt(os.Args[3], 10, 64)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	block, err := ytrebuilder.FetchBlock(context.TODO(), tikvCli, blockID)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	fmt.Printf("ID: %d, VNF: %d, AR: %d, SNID: %d, %d shards\n", block.ID, block.VNF, block.AR, block.SNID, len(block.Shards))
	// } else
	if os.Args[2] == "shardmeta" {
		shardID, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			panic(err)
		}
		shard, err := ytrebuilder.FetchShardMeta(context.TODO(), tikvCli, shardID)
		if err != nil {
			panic(err)
		}
		if shard != nil {
			fmt.Printf("ID: %d, BIndex: %d, Offset: %d, VNF: %d, AR: %d, Timestamp: %d\n", shard.Id, shard.Bindex, shard.Offset, shard.Vnf, shard.Ar, shard.Timestamp)
		} else {
			fmt.Println("no value")
		}
	} else if os.Args[2] == "nodeshard" {
		shardID, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			panic(err)
		}
		nodeID, err := strconv.ParseInt(os.Args[4], 10, 64)
		if err != nil {
			panic(err)
		}
		shards, err := ytrebuilder.FetchNodeShards(context.TODO(), tikvCli, int32(nodeID), shardID, 9223372036854775807, 1)
		if err != nil {
			panic(err)
		}
		fmt.Printf("ID: %d, VHF: %s, BlockID: %d, NodeID: %d, NodeID2: %d\n", shards[0].ID, base64.StdEncoding.EncodeToString(shards[0].VHF), shards[0].ID-int64(shards[0].Offset), shards[0].NodeID, shards[0].NodeID2)
	} else if os.Args[2] == "nodeshards" {
		nodeID, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			panic(err)
		}
		var shardFrom int64 = 0
		for {
			shards, err := ytrebuilder.FetchNodeShards(context.TODO(), tikvCli, int32(nodeID), shardFrom, 9223372036854775807, 20)
			if err != nil {
				panic(err)
			}
			if len(shards) == 0 {
				fmt.Println("finished!")
				return
			}
			for _, s := range shards {
				fmt.Printf("ID: %d, VHF: %s, BlockID: %d, NodeID: %d, NodeID2: %d, Offset: %d\n", s.ID, base64.StdEncoding.EncodeToString(s.VHF), s.ID-int64(s.Offset), s.NodeID, s.NodeID2, s.Offset)
				shardFrom = s.ID + 1
			}
			buf := bufio.NewReader(os.Stdin)
			sentence, err := buf.ReadBytes('\n')
			if err != nil {
				panic(err)
			} else {
				if (string(sentence)) == "stop" {
					return
				}
			}
		}
	} else if os.Args[2] == "nodeshardsrevert" {
		nodeID, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			panic(err)
		}
		shardFrom, err := strconv.ParseInt(os.Args[4], 10, 64)
		if err != nil {
			panic(err)
		}
		shardTo, err := strconv.ParseInt(os.Args[5], 10, 64)
		if err != nil {
			panic(err)
		}
		for {
			_, values, err := tikvCli.ReverseScan(context.TODO(), append([]byte(fmt.Sprintf("%s_%d_%19d", ytrebuilder.PFX_SHARDNODES, nodeID, shardTo)), '\x00'), append([]byte(fmt.Sprintf("%s_%d_%19d", ytrebuilder.PFX_SHARDNODES, nodeID, shardFrom)), '\x00'), 20)
			if err != nil {
				panic(err)
			}
			shards := make([]*ytrebuilder.Shard, 0)
			for _, buf := range values {
				s := new(ytrebuilder.Shard)
				err := s.FillBytes(buf)
				if err != nil {
					panic(err)
				}
				if s.NodeID != int32(nodeID) && s.NodeID2 != int32(nodeID) {
					continue
				}
				shards = append(shards, s)
			}
			if len(shards) == 0 {
				fmt.Println("finished!")
				return
			}
			for _, s := range shards {
				fmt.Printf("ID: %d, VHF: %s, BlockID: %d, NodeID: %d, NodeID2: %d\n", s.ID, base64.StdEncoding.EncodeToString(s.VHF), s.ID-int64(s.Offset), s.NodeID, s.NodeID2)
				shardFrom = s.ID + 1
			}
			buf := bufio.NewReader(os.Stdin)
			sentence, err := buf.ReadBytes('\n')
			if err != nil {
				panic(err)
			} else {
				if (string(sentence)) == "stop" {
					return
				}
			}
		}
		// } else if os.Args[2] == "blocksize" {
		// 	var blockFrom int64 = 0
		// 	total := 0
		// 	for {
		// 		blocks, err := ytrebuilder.FetchBlocks(context.TODO(), tikvCli, blockFrom, 9223372036854775807, 10000)
		// 		if err != nil {
		// 			panic(err)
		// 		}
		// 		total += len(blocks)
		// 		if len(blocks) == 0 {
		// 			fmt.Printf("Total: %d\n", total)
		// 			return
		// 		}
		// 		blockFrom = blocks[len(blocks)-1].ID + 1
		// 	}
	} else if os.Args[2] == "nodeshardsize" {
		nodeID, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			panic(err)
		}
		var shardFrom int64 = 0
		total := 0
		for {
			shards, err := ytrebuilder.FetchNodeShards(context.TODO(), tikvCli, int32(nodeID), shardFrom, 9223372036854775807, 10000)
			if err != nil {
				panic(err)
			}
			total += len(shards)
			if len(shards) == 0 {
				fmt.Printf("NodeID: %d, Total: %d\n", nodeID, total)
				return
			}
			shardFrom = shards[len(shards)-1].ID + 1
		}
	}
	// else if os.Args[2] == "nodeshardsize2" {
	// 	start := time.Now().Unix()
	// 	nodeID, err := strconv.ParseInt(os.Args[3], 10, 64)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	taskCount, err := strconv.ParseInt(os.Args[4], 10, 64)
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	rangeFrom := int64(0)
	// 	rangeTo := int64(0)
	// 	shardFrom, err := ytrebuilder.FetchFirstNodeShard(context.Background(), tikvCli, int32(nodeID))
	// 	if err != nil {
	// 		if err != ytrebuilder.NoValError {
	// 			fmt.Printf("error when finding starting shard: %s\n", err.Error())
	// 		} else {
	// 			fmt.Println("no shards for rebuilding")
	// 		}
	// 		return
	// 	} else {
	// 		rangeFrom = shardFrom.ID
	// 	}
	// 	shardTo, err := ytrebuilder.FetchLastNodeShard(context.Background(), tikvCli, int32(nodeID))
	// 	if err != nil {
	// 		if err != ytrebuilder.NoValError {
	// 			fmt.Printf("error when finding ending shard: %s\n", err.Error())
	// 		} else {
	// 			fmt.Printf("last shard cannot be found: %s\n", err.Error())
	// 		}
	// 		return
	// 	} else {
	// 		rangeTo = shardTo.ID
	// 	}
	// 	segs := make([]int64, 0)
	// 	grids := make(map[int64]int64)
	// 	gap := (rangeTo - rangeFrom) / taskCount
	// 	if gap < 1000000000 {
	// 		fmt.Println("range too small, calculating by one goroutine")
	// 		segs = append(segs, rangeFrom, rangeTo+1)
	// 		grids[rangeFrom] = rangeFrom
	// 	} else {
	// 		for i := rangeFrom; i < rangeTo; i += gap {
	// 			segs = append(segs, i)
	// 			grids[i] = i
	// 		}
	// 		segs = append(segs, rangeTo+1)
	// 	}
	// 	var total int64 = 0
	// 	wg := sync.WaitGroup{}
	// 	wg.Add(len(grids))
	// 	for i := 0; i < len(segs)-1; i++ {
	// 		index := i
	// 		begin := segs[i]
	// 		from := grids[begin]
	// 		to := segs[i+1]
	// 		if from == -1 {
	// 			wg.Done()
	// 			continue
	// 		}
	// 		go func() {
	// 			fmt.Printf("starting goroutine%d from %d to %d, checkpoint is %d\n", index, begin, to, from)
	// 			defer wg.Done()
	// 			for {
	// 				rebuildShards, err := ytrebuilder.FetchNodeShards(context.Background(), tikvCli, int32(nodeID), from, to, 10000)
	// 				if err != nil {
	// 					fmt.Printf("error when fetching shard-rebuilding tasks for caching: %s\n", err.Error())
	// 					time.Sleep(time.Duration(3) * time.Second)
	// 					continue
	// 				}
	// 				if len(rebuildShards) == 0 {
	// 					fmt.Printf("finished goroutine%d from %d to %d\n", index, begin, to)
	// 					break
	// 				}
	// 				tasks, err := ytrebuilder.BuildTasks2(context.Background(), tikvCli, rebuildShards)
	// 				if err != nil {
	// 					fmt.Printf("error when building tasks: %s\n", err.Error())
	// 					time.Sleep(time.Duration(3) * time.Second)
	// 					continue
	// 				}
	// 				from = rebuildShards[len(rebuildShards)-1].ID + 1
	// 				for _ = range tasks {
	// 					atomic.AddInt64(&total, 1)
	// 				}
	// 			}
	// 		}()
	// 	}
	// 	wg.Wait()
	// 	fmt.Printf("total shards: %d, cost time: %d", total, time.Now().Unix()-start)

	// 	// var shardFrom int64 = 0
	// 	// total := 0
	// 	// for {
	// 	// 	shards, err := ytrebuilder.FetchNodeShards(context.TODO(), tikvCli, int32(nodeID), shardFrom, 9223372036854775807, 10000)
	// 	// 	if err != nil {
	// 	// 		panic(err)
	// 	// 	}
	// 	// 	total += len(shards)
	// 	// 	if len(shards) == 0 {
	// 	// 		fmt.Printf("NodeID: %d, Total: %d\n", nodeID, total)
	// 	// 		return
	// 	// 	}
	// 	// 	shardFrom = shards[len(shards)-1].ID + 1
	// 	// }
	// }
}
