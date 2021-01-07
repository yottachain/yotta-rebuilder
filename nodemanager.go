package ytrebuilder

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

//NodeManager node manager struct
type NodeManager struct {
	Nodes  map[int32]*Node
	rwlock *sync.RWMutex
}

//NewNodeManager create new node manager
func NewNodeManager(ctx context.Context, cli *mongo.Client) (*NodeManager, error) {
	nodeMgr := new(NodeManager)
	nodeMgr.Nodes = make(map[int32]*Node)
	nodeMgr.rwlock = new(sync.RWMutex)
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
		nodeMgr.Nodes[node.ID] = node
	}
	return nodeMgr, nil
}

//UpdateNode update node in node manager
func (nodeMgr *NodeManager) UpdateNode(node *Node) {
	if node == nil || node.ID <= 0 {
		return
	}
	nodeMgr.rwlock.Lock()
	defer nodeMgr.rwlock.Unlock()
	nodeMgr.Nodes[node.ID] = node
}

//GetNode get node by node ID
func (nodeMgr *NodeManager) GetNode(id int32) *Node {
	nodeMgr.rwlock.RLock()
	defer nodeMgr.rwlock.RUnlock()
	return nodeMgr.Nodes[id]
}
