package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"

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
		fmt.Printf("ID: %d, VNF: %d, AR: %d, SNID: %d\n", block.ID, block.VNF, block.AR, block.SNID)
	} else if os.Args[2] == "shard" {
		shardID, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			panic(err)
		}
		shard, err := ytrebuilder.FetchShard(context.TODO(), tikvCli, shardID)
		if err != nil {
			panic(err)
		}
		fmt.Printf("ID: %d, VHF: %s, NodeID: %d, BlockID: %d\n", shard.ID, base64.StdEncoding.EncodeToString(shard.VHF), shard.NodeID, shard.BlockID)
	} else if os.Args[2] == "nodeshards" {
		nodeID, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			panic(err)
		}
		var shardFrom int64 = 0
		for {
			shards, err := ytrebuilder.FetchNodeShards(context.TODO(), tikvCli, int32(nodeID), shardFrom, 20)
			if err != nil {
				panic(err)
			}
			if len(shards) == 0 {
				fmt.Println("finished!")
				return
			}
			for _, s := range shards {
				fmt.Printf("ID: %d, VHF: %s, NodeID: %d, BlockID: %d\n", s.ID, base64.StdEncoding.EncodeToString(s.VHF), s.NodeID, s.BlockID)
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
	} else if os.Args[2] == "nodeshardsize" {
		nodeID, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			panic(err)
		}
		var shardFrom int64 = 0
		total := 0
		for {
			shards, err := ytrebuilder.FetchNodeShards(context.TODO(), tikvCli, int32(nodeID), shardFrom, 10000)
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

}
