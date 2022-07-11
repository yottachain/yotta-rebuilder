package ytrebuilder

import (
	"context"

	cmap "github.com/fanliao/go-concurrentMap"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

//NodeManager node manager struct
type NodeManager struct {
	Nodes *cmap.ConcurrentMap
	//Nodes  map[int32]*Node
	//rwlock *sync.RWMutex
}

//NewNodeManager create new node manager
func NewNodeManager(ctx context.Context, cli *mongo.Client) (*NodeManager, error) {
	nodeMgr := new(NodeManager)
	nodeMgr.Nodes = cmap.NewConcurrentMap(16384, float32(0.75), 16384)
	//nodeMgr.Nodes = make(map[int32]*Node)
	//nodeMgr.rwlock = new(sync.RWMutex)
	collection := cli.Database(RebuilderDB).Collection(NodeTab)
	cur, err := collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		node := new(Node)
		err := cur.Decode(node)
		if err != nil {
			continue
		}
		nodeMgr.Nodes.Put(node.ID, node)
	}
	return nodeMgr, nil
}

//UpdateNode update node in node manager
func (nodeMgr *NodeManager) UpdateNode(node *Node) {
	if node == nil || node.ID <= 0 {
		return
	}
	//nodeMgr.rwlock.Lock()
	//defer nodeMgr.rwlock.Unlock()
	val, _ := nodeMgr.Nodes.Get(node.ID)
	if val == nil {
		nodeMgr.Nodes.Put(node.ID, node)
	} else {
		oldNode := val.(*Node)
		oldNode.Addrs = node.Addrs
		oldNode.AssignedSpace = node.AssignedSpace
		oldNode.Bandwidth = node.Bandwidth
		oldNode.CPU = node.CPU
		oldNode.ErrorCount = node.ErrorCount
		oldNode.Ext = node.Ext
		oldNode.MaxDataSpace = node.MaxDataSpace
		oldNode.Memory = node.Memory
		oldNode.NodeID = node.NodeID
		oldNode.Owner = node.Owner
		oldNode.PoolID = node.PoolID
		oldNode.PoolOwner = node.PoolOwner
		oldNode.ProductiveSpace = node.ProductiveSpace
		oldNode.ProfitAcc = node.ProfitAcc
		oldNode.PubKey = node.PubKey
		oldNode.Quota = node.Quota
		oldNode.RealSpace = node.RealSpace
		oldNode.Rebuilding = node.Rebuilding
		oldNode.Relay = node.Relay
		oldNode.Round = node.Round
		oldNode.Rx = node.Rx
		oldNode.Status = node.Status
		oldNode.Timestamp = node.Timestamp
		oldNode.Tx = node.Tx
		oldNode.UsedSpace = node.UsedSpace
		oldNode.Uspaces = node.Uspaces
		oldNode.Valid = node.Valid
		oldNode.Version = node.Version
		oldNode.Weight = node.Weight
	}
}

//GetNode get node by node ID
func (nodeMgr *NodeManager) GetNode(id int32) *Node {
	//nodeMgr.rwlock.RLock()
	//defer nodeMgr.rwlock.RUnlock()
	//return nodeMgr.Nodes[id]
	val, _ := nodeMgr.Nodes.Get(id)
	if val != nil {
		return val.(*Node)
	} else {
		return nil
	}
}
