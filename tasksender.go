package ytrebuilder

import (
	"context"
	"sync"

	log "github.com/sirupsen/logrus"
	net "github.com/yottachain/YTCoreService/net"
	pkt "github.com/yottachain/YTCoreService/pkt"
)

var runningNode = struct {
	sync.RWMutex
	nodes map[int32]*Node
}{nodes: make(map[int32]*Node)}

func (rebuilder *Rebuilder) SendTask(ctx context.Context, node *Node) {
	entry := log.WithFields(log.Fields{Function: "SendTask", MinerID: node.ID})
	runningNode.Lock()
	defer runningNode.Unlock()
	if _, ok := runningNode.nodes[node.ID]; ok {
		entry.Debug("node is sending task")
		return
	}
	runningNode.nodes[node.ID] = node
	entry.Info("allocate rebuilding executing miner")
	go rebuilder.SendTaskLoop(ctx, node)
}

func (rebuilder *Rebuilder) SendTaskLoop(ctx context.Context, node *Node) {
	defer func() {
		runningNode.Lock()
		delete(runningNode.nodes, node.ID)
		runningNode.Unlock()
	}()
	entry := log.WithFields(log.Fields{Function: "SendTaskLoop", MinerID: node.ID})
	total := 8000
	i := 0
	f := 0
	for {
		tasks, err := rebuilder.GetRebuildTasks(ctx, node.ID)
		if err != nil {
			entry.WithError(err).Errorf("Total send %d tasks, %d failed", i, f)
			return
		}
		entry.Debugf("%d rebuild tasks ready", len(tasks.Tasklist))
		n := &net.Node{Id: node.ID, Nodeid: node.NodeID, Pubkey: node.PubKey, Addrs: node.Addrs}
		req := &pkt.TaskList{Tasklist: tasks.Tasklist, ExpiredTime: tasks.ExpiredTime, SrcNodeID: tasks.SrcNodeID, ExpiredTimeGap: tasks.ExpiredTimeGap}
		_, e := net.RequestDN(req, n, "")
		if e != nil {
			entry.WithError(err).Errorf("Send rebuild task failed: %d--%s", e.Code, e.Msg)
			f += len(tasks.Tasklist)
		} else {
			entry.Infof("Send rebuild task OK,count %d", len(tasks.Tasklist))
		}
		i += len(tasks.Tasklist)
		if total-i < rebuilder.Params.RebuildShardMinerTaskBatchSize {
			entry.Infof("Total send %d tasks, %d failed", i, f)
			return
		}
	}
}
