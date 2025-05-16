package iavl

import (
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
	return n
}

func (np *NodePool) Put(node *Node) {
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

	node.poolId = 0
	np.syncPool.Put(node)
}
