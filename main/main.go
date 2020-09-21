package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/golang/protobuf/proto"
	ytrebuilder "github.com/yottachain/yotta-rebuilder"
	"github.com/yottachain/yotta-rebuilder/cmd"
	pb "github.com/yottachain/yotta-rebuilder/pbrebuilder"
)

func main() {
	cmd.Execute()
}

func main1() {
	start := int64(0)
	i := 0
	for {
		shardsRebuilt, err := ytrebuilder.GetRebuildShards(new(http.Client), "http://192.168.36.132:8091", start, 100, time.Now().Unix()-int64(180))
		if err != nil {
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		for _, sr := range shardsRebuilt.ShardsRebuild {
			i++
			fmt.Printf("%d. %d: %d\n", i, sr.ID, sr.VFI)
		}
		if shardsRebuilt.More {
			start = shardsRebuilt.Next
			continue
		} else {
			if len(shardsRebuilt.ShardsRebuild) > 0 {
				start = shardsRebuilt.ShardsRebuild[len(shardsRebuilt.ShardsRebuild)-1].ID + 1
			}
			time.Sleep(time.Duration(10) * time.Second)
		}
	}
}

func main0() {
	cli, err := ytrebuilder.NewClient("192.168.36.132:8080")
	if err != nil {
		panic(err)
	}
	result, err := cli.GetRebuildTasks(context.Background(), 1)
	if err != nil {
		panic(err)
	}
	fmt.Println(len(result.Tasklist))
	ids := make([][]byte, 0)
	res := make([]int32, 0)
	for _, b := range result.Tasklist {
		t := b[0:2]
		tp := ytrebuilder.BytesToUint16(t)
		if tp == 0x68b3 {
			task := new(pb.TaskDescription)
			err := proto.Unmarshal(b[2:], task)
			if err != nil {
				panic(err)
			}
			fmt.Printf("id: %d -> %+v\n", ytrebuilder.BytesToInt64(task.Id), task)
			ids = append(ids, task.Id)
			res = append(res, 0)
		} else if tp == 0xc258 {
			task := new(pb.TaskDescriptionCP)
			err := proto.Unmarshal(b[2:], task)
			if err != nil {
				panic(err)
			}
			fmt.Printf("id: %d -> %+v\n", ytrebuilder.BytesToInt64(task.Id), task)
			ids = append(ids, task.Id)
			res = append(res, 0)
		}
	}
	err = cli.UpdateTaskStatus(context.Background(), &pb.MultiTaskOpResult{Id: ids, RES: res})
	if err != nil {
		panic(err)
	}
}
