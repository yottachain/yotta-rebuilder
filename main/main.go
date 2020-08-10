package main

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	ytrebuilder "github.com/yottachain/yotta-rebuilder"
	"github.com/yottachain/yotta-rebuilder/cmd"
	pb "github.com/yottachain/yotta-rebuilder/pbrebuilder"
)

func main() {
	cmd.Execute()
}

func main2() {
	b := ytrebuilder.Int64ToBytes(6850675130134606064)
	t := ytrebuilder.BytesToInt32(b[0:4])
	fmt.Println(t)
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
