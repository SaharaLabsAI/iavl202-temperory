package iavl

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

var GlobalPoolId atomic.Uint64

type NodePool struct {
	syncPool *sync.Pool

	poolID atomic.Uint64
}

func NewNodePool() *NodePool {
	np := &NodePool{
		syncPool: &sync.Pool{
			New: func() any {
				return &Node{}
			},
		},
	}

	if GlobalPoolId.Load() == math.MaxUint64 {
		np.poolID.Store(1)
		GlobalPoolId.Store(1)
	} else {
		GlobalPoolId.Add(1)
		np.poolID.Store(GlobalPoolId.Load())
	}

	return np
}

func (np *NodePool) Get() *Node {
	n := np.syncPool.Get().(*Node)
	n.poolID = np.poolID.Load()
	n.source = PoolNode

	return n
}

func (np *NodePool) Put(node *Node) {
	if node.poolID == 0 {
		panic(fmt.Sprintf("NodePool.Put: detected attempt to Put node with poolId 0 (key: %s). Possible double Put or invalid node.", node.key))
	}
	if node.poolID != np.poolID.Load() {
		panic(fmt.Sprintf("NodePool.Put: attempt to Put node to wrong pool, node %d pool %d", node.poolID, np.poolID.Load()))
	}

	node.leftNodeKey = emptyNodeKey
	node.rightNodeKey = emptyNodeKey
	node.rightNode = nil
	node.leftNode = nil
	node.nodeKey = emptyNodeKey
	node.hash = nil
	node.key = nil
	node.value = nil
	node.subtreeHeight = -1
	node.size = 0
	node.dirty = false
	node.evict = false
	node.source = PoolNode

	node.poolID = 0
	np.syncPool.Put(node)
}
