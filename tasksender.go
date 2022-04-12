package ytrebuilder

import (
	"context"
	"sync"
	"time"

	"github.com/enriquebris/goconcurrentqueue"
	"github.com/ivpusic/grpool"
	log "github.com/sirupsen/logrus"
	net "github.com/yottachain/YTCoreService/net"
	pkt "github.com/yottachain/YTCoreService/pkt"
	pb "github.com/yottachain/yotta-rebuilder/pbrebuilder"
)

type TaskSender struct {
	queue *goconcurrentqueue.FIFO
	m     sync.Map
	//lock  sync.RWMutex
}

func NewTaskSender() *TaskSender {
	return &TaskSender{queue: goconcurrentqueue.NewFIFO()}
}

func (taskSender *TaskSender) AddToQueue(node *Node) {
	entry := log.WithFields(log.Fields{Function: "AddToQueue", MinerID: node.ID})
	// taskSender.lock.Lock()
	// defer taskSender.lock.Unlock()
	ele, ok := taskSender.m.Load(node.ID)
	if !ok {
		taskSender.m.Store(node.ID, node)
		taskSender.queue.Enqueue(node)
		entry.Debugf("push node to queue")
	} else {
		n := ele.(*Node)
		n.NodeID = node.NodeID
		n.PubKey = node.PubKey
		n.Owner = node.Owner
		n.ProfitAcc = node.ProfitAcc
		n.PoolID = node.PoolID
		n.PoolOwner = node.PoolOwner
		n.Quota = node.Quota
		n.Addrs = node.Addrs
		n.CPU = node.CPU
		n.Memory = node.Memory
		n.Bandwidth = node.Bandwidth
		n.MaxDataSpace = node.MaxDataSpace
		n.AssignedSpace = node.AssignedSpace
		n.ProductiveSpace = node.ProductiveSpace
		n.UsedSpace = node.UsedSpace
		n.Uspaces = node.Uspaces
		n.Weight = node.Weight
		n.Valid = node.Valid
		n.Relay = node.Relay
		n.Status = node.Status
		n.Timestamp = node.Timestamp
		n.Version = node.Version
		n.Rebuilding = node.Rebuilding
		n.RealSpace = node.RealSpace
		n.Tx = node.Tx
		n.Rx = node.Rx
		n.Ext = node.Ext
		entry.Debugf("update node in queue")
	}
}

func (rebuilder *Rebuilder) SendTasks(ctx context.Context) {
	entry := log.WithFields(log.Fields{Function: "SendTasks"})
	pool := grpool.NewPool(10000, 10000)
	for {
		var node *Node
		for {
			ele, err := rebuilder.taskSender.queue.DequeueOrWaitForNextElement()
			if err != nil {
				entry.WithError(err).Error("get node from queue")
				continue
			}
			node = ele.(*Node)
			if node.Rebuilding > 1 || node.Status != 1 || node.Valid == 0 || node.Weight < float64(rebuilder.Params.WeightThreshold) || node.AssignedSpace <= 0 || node.Quota <= 0 || node.Version < int32(rebuilder.Params.MinerVersionThreshold) || time.Now().Unix()-node.Timestamp > 300 {
				entry.WithField(MinerID, node.ID).Info("invalid rebuilding node")
				rebuilder.taskSender.m.Delete(node.ID)
				continue
			}
			break
		}
		var task *pb.MultiTaskDescription
		var err error
		for {
			task, err = rebuilder.GetRebuildTasks(ctx, node.ID)
			if err != nil {
				if err == ErrInvalidNode {
					entry.WithField(MinerID, node.ID).WithError(err).Errorf("invalid node %d", node.ID)
					rebuilder.taskSender.m.Delete(node.ID)
					break
				} else {
					entry.WithField(MinerID, node.ID).WithError(err).Errorf("get rebuild task for node %d", node.ID)
					time.Sleep(time.Duration(1) * time.Second)
					continue
				}
			}
			break
		}
		if task == nil {
			rebuilder.taskSender.m.Delete(node.ID)
			continue
		}
		pool.JobQueue <- func() {
			n := &net.Node{Id: node.ID, Nodeid: node.NodeID, Pubkey: node.PubKey, Addrs: node.Addrs}
			req := &pkt.TaskList{Tasklist: task.Tasklist, ExpiredTime: task.ExpiredTime, SrcNodeID: task.SrcNodeID, ExpiredTimeGap: task.ExpiredTimeGap}
			_, e := net.RequestDN(req, n, "")
			if e != nil {
				entry.WithField(MinerID, node.ID).WithField("SrcNodeID", task.SrcNodeID).WithError(err).Errorf("Send rebuild task failed: %d--%s", e.Code, e.Msg)
			} else {
				entry.WithField(MinerID, node.ID).WithField("SrcNodeID", task.SrcNodeID).Infof("Send rebuild task OK,count %d", len(task.Tasklist))
			}
			// rebuilder.taskSender.lock.Lock()
			// defer rebuilder.taskSender.lock.Unlock()
			rebuilder.taskSender.m.Delete(node.ID)
		}

	}
}

// var runningNode = struct {
// 	sync.RWMutex
// 	nodes map[int32]*Node
// }{nodes: make(map[int32]*Node)}

// func (rebuilder *Rebuilder) SendTask(ctx context.Context, node *Node) {
// 	entry := log.WithFields(log.Fields{Function: "SendTask", MinerID: node.ID})
// 	runningNode.Lock()
// 	defer runningNode.Unlock()
// 	if _, ok := runningNode.nodes[node.ID]; ok {
// 		entry.Debug("node is sending task")
// 		return
// 	}
// 	runningNode.nodes[node.ID] = node
// 	entry.Info("allocate rebuilding executing miner")
// 	go rebuilder.SendTaskLoop(ctx, node)
// }

// func (rebuilder *Rebuilder) SendTaskLoop(ctx context.Context, node *Node) {
// 	defer func() {
// 		runningNode.Lock()
// 		delete(runningNode.nodes, node.ID)
// 		runningNode.Unlock()
// 	}()
// 	entry := log.WithFields(log.Fields{Function: "SendTaskLoop", MinerID: node.ID})
// 	total := 8000
// 	i := 0
// 	f := 0
// 	for {
// 		tasks, err := rebuilder.GetRebuildTasks(ctx, node.ID)
// 		if err != nil {
// 			entry.WithError(err).Errorf("Total send %d tasks, %d failed", i, f)
// 			return
// 		}
// 		entry.Debugf("%d rebuild tasks ready", len(tasks.Tasklist))
// 		n := &net.Node{Id: node.ID, Nodeid: node.NodeID, Pubkey: node.PubKey, Addrs: node.Addrs}
// 		req := &pkt.TaskList{Tasklist: tasks.Tasklist, ExpiredTime: tasks.ExpiredTime, SrcNodeID: tasks.SrcNodeID, ExpiredTimeGap: tasks.ExpiredTimeGap}
// 		_, e := net.RequestDN(req, n, "")
// 		if e != nil {
// 			entry.WithError(err).Errorf("Send rebuild task failed: %d--%s", e.Code, e.Msg)
// 			f += len(tasks.Tasklist)
// 		} else {
// 			entry.Infof("Send rebuild task OK,count %d", len(tasks.Tasklist))
// 		}
// 		i += len(tasks.Tasklist)
// 		if total-i < rebuilder.Params.RebuildShardMinerTaskBatchSize {
// 			entry.Infof("Total send %d tasks, %d failed", i, f)
// 			return
// 		}
// 	}
// }
