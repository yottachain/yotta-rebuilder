package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	ytrebuilder "github.com/yottachain/yotta-rebuilder"
	"github.com/yottachain/yotta-rebuilder/cmd"
)

func main() {
	cmd.Execute()
}

func main1() {
	urls := []string{os.Args[1]}
	tikvCli, err := rawkv.NewClient(context.TODO(), urls, config.Default())
	if err != nil {
		panic(err)
	}
	if os.Args[2] == "block" {
		blockID, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			panic(err)
		}
		block, err := ytrebuilder.FetchBlock(context.TODO(), tikvCli, blockID)
		if err != nil {
			panic(err)
		}
		fmt.Printf("ID: %d, VNF: %d, AR: %d, SNID: %d, %d shards\n", block.ID, block.VNF, block.AR, block.SNID, len(block.Shards))
	} else if os.Args[2] == "shard" {
		shardID, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			panic(err)
		}
		shard, err := ytrebuilder.FetchShard(context.TODO(), tikvCli, shardID)
		if err != nil {
			panic(err)
		}
		fmt.Printf("ID: %d, VHF: %s, NodeID: %d, BlockID: %d, NodeID2: %d\n", shard.ID, base64.StdEncoding.EncodeToString(shard.VHF), shard.NodeID, shard.BlockID, shard.NodeID2)
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
		fmt.Printf("ID: %d, VHF: %s, NodeID: %d, BlockID: %d, NodeID2: %d\n", shards[0].ID, base64.StdEncoding.EncodeToString(shards[0].VHF), shards[0].NodeID, shards[0].BlockID, shards[0].NodeID2)
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
				fmt.Printf("ID: %d, VHF: %s, NodeID: %d, BlockID: %d, NodeID2: %d\n", s.ID, base64.StdEncoding.EncodeToString(s.VHF), s.NodeID, s.BlockID, s.NodeID2)
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
				fmt.Printf("ID: %d, VHF: %s, NodeID: %d, BlockID: %d, NodeID2: %d\n", s.ID, base64.StdEncoding.EncodeToString(s.VHF), s.NodeID, s.BlockID, s.NodeID2)
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
	} else if os.Args[2] == "blocksize" {
		var blockFrom int64 = 0
		total := 0
		for {
			blocks, err := ytrebuilder.FetchBlocks(context.TODO(), tikvCli, blockFrom, 9223372036854775807, 10000)
			if err != nil {
				panic(err)
			}
			total += len(blocks)
			if len(blocks) == 0 {
				fmt.Printf("Total: %d\n", total)
				return
			}
			blockFrom = blocks[len(blocks)-1].ID + 1
		}
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
	} else if os.Args[2] == "nodeshardsize2" {
		start := time.Now().Unix()
		nodeID, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			panic(err)
		}
		taskCount, err := strconv.ParseInt(os.Args[4], 10, 64)
		if err != nil {
			panic(err)
		}

		rangeFrom := int64(0)
		rangeTo := int64(0)
		shardFrom, err := ytrebuilder.FetchFirstNodeShard(context.Background(), tikvCli, int32(nodeID))
		if err != nil {
			if err != ytrebuilder.NoValError {
				fmt.Printf("error when finding starting shard: %s\n", err.Error())
			} else {
				fmt.Println("no shards for rebuilding")
			}
			return
		} else {
			rangeFrom = shardFrom.ID
		}
		shardTo, err := ytrebuilder.FetchLastNodeShard(context.Background(), tikvCli, int32(nodeID))
		if err != nil {
			if err != ytrebuilder.NoValError {
				fmt.Printf("error when finding ending shard: %s\n", err.Error())
			} else {
				fmt.Printf("last shard cannot be found: %s\n", err.Error())
			}
			return
		} else {
			rangeTo = shardTo.ID
		}
		segs := make([]int64, 0)
		grids := make(map[int64]int64)
		gap := (rangeTo - rangeFrom) / taskCount
		if gap < 1000000000 {
			fmt.Println("range too small, calculating by one goroutine")
			segs = append(segs, rangeFrom, rangeTo+1)
			grids[rangeFrom] = rangeFrom
		} else {
			for i := rangeFrom; i < rangeTo; i += gap {
				segs = append(segs, i)
				grids[i] = i
			}
			segs = append(segs, rangeTo+1)
		}
		var total int64 = 0
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
				fmt.Printf("starting goroutine%d from %d to %d, checkpoint is %d\n", index, begin, to, from)
				defer wg.Done()
				for {
					rebuildShards, err := ytrebuilder.FetchNodeShards(context.Background(), tikvCli, int32(nodeID), from, to, 10000)
					if err != nil {
						fmt.Printf("error when fetching shard-rebuilding tasks for caching: %s\n", err.Error())
						time.Sleep(time.Duration(3) * time.Second)
						continue
					}
					if len(rebuildShards) == 0 {
						fmt.Printf("finished goroutine%d from %d to %d\n", index, begin, to)
						break
					}
					tasks, err := ytrebuilder.BuildTasks2(context.Background(), tikvCli, rebuildShards)
					if err != nil {
						fmt.Printf("error when building tasks: %s\n", err.Error())
						time.Sleep(time.Duration(3) * time.Second)
						continue
					}
					from = rebuildShards[len(rebuildShards)-1].ID + 1
					for _ = range tasks {
						atomic.AddInt64(&total, 1)
					}
				}
			}()
		}
		wg.Wait()
		fmt.Printf("total shards: %d, cost time: %d", total, time.Now().Unix()-start)

		// var shardFrom int64 = 0
		// total := 0
		// for {
		// 	shards, err := ytrebuilder.FetchNodeShards(context.TODO(), tikvCli, int32(nodeID), shardFrom, 9223372036854775807, 10000)
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// 	total += len(shards)
		// 	if len(shards) == 0 {
		// 		fmt.Printf("NodeID: %d, Total: %d\n", nodeID, total)
		// 		return
		// 	}
		// 	shardFrom = shards[len(shards)-1].ID + 1
		// }
	}

}
