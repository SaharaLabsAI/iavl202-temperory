package iavl

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

type NodePool struct {
	syncPool *sync.Pool

	free  chan int
	nodes []Node

	poolId atomic.Uint64
}

func NewNodePool() *NodePool {
	np := &NodePool{
		syncPool: &sync.Pool{
			New: func() any {
				return &Node{}
			},
		},
		free: make(chan int, 1000),
	}
	return np
}

func (np *NodePool) Get() *Node {
	if np.poolId.Load() == math.MaxUint64 {
		np.poolId.Store(1)
	} else {
		np.poolId.Add(1)
	}
	n := np.syncPool.Get().(*Node)
	n.poolId = np.poolId.Load()
	n.source = PoolNode
	return n
}

func (np *NodePool) Put(node *Node) {
	if node.poolId == 0 {
		panic(fmt.Sprintf("NodePool.Put: detected attempt to Put node with poolId 0 (key: %s). Possible double Put or invalid node.", node.key))
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

	node.poolId = 0
	np.syncPool.Put(node)
}
