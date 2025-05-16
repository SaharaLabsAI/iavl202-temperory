package iavl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cosmos/iavl/v2/metrics"
)

const (
	metricsNamespace  = "iavl_v2"
	leafSequenceStart = uint32(1 << 31)
)

type nodeDelete struct {
	// the sequence in which this deletion was processed
	deleteKey NodeKey
	// the leaf key to delete in `latest` table (if maintained)
	leafKey []byte
}

type Tree struct {
	version      atomic.Int64
	root         *Node
	metrics      metrics.Proxy
	sql          *SqliteDb
	sqlWriter    *sqlWriter
	writerCancel context.CancelFunc
	pool         *NodePool

	// options
	maxWorkingSize  uint64
	workingBytes    uint64
	workingSize     int64
	storeLeafValues bool
	heightFilter    int8
	metricsProxy    metrics.Proxy

	// state
	branches       []*Node
	leaves         []*Node
	branchOrphans  []NodeKey
	leafOrphans    []NodeKey
	deletes        []*nodeDelete
	leafSequence   uint32
	branchSequence uint32
	isReplaying    bool
	evictionDepth  int8

	immutable  bool
	rootHashed bool
	cache      map[string][]byte
	deleted    map[string]bool

	rw sync.RWMutex
}

type TreeOptions struct {
	StateStorage  bool
	HeightFilter  int8
	EvictionDepth int8
	MetricsProxy  metrics.Proxy
}

func DefaultTreeOptions() TreeOptions {
	return TreeOptions{
		StateStorage:  true,
		HeightFilter:  1,
		EvictionDepth: 32,
		MetricsProxy:  &metrics.NilMetrics{},
	}
}

func NewTree(sql *SqliteDb, pool *NodePool, opts TreeOptions) *Tree {
	ctx, cancel := context.WithCancel(context.Background())
	if sql != nil {
		sql.useReadPool = false
	}

	tree := &Tree{
		sql:             sql,
		sqlWriter:       sql.newSQLWriter(),
		writerCancel:    cancel,
		pool:            pool,
		metrics:         opts.MetricsProxy,
		maxWorkingSize:  1.5 * 1024 * 1024 * 1024,
		storeLeafValues: opts.StateStorage,
		heightFilter:    opts.HeightFilter,
		metricsProxy:    opts.MetricsProxy,
		evictionDepth:   opts.EvictionDepth,
		leafSequence:    leafSequenceStart,
		immutable:       false,
		rootHashed:      false,
		cache:           make(map[string][]byte),
		deleted:         make(map[string]bool),
	}

	tree.version.Store(0)
	if tree.sql != nil {
		tree.sql.readPool.LinkTreeVersion(&tree.version)
	}

	tree.sqlWriter.start(ctx)

	return tree
}

func (tree *Tree) LoadVersion(version int64) (err error) {
	if tree.sql == nil {
		return errors.New("sql is nil")
	}

	if version == 0 {
		return nil
	}

	tree.version.Store(version)
	if tree.immutable {
		exists, err := tree.sql.HasRoot(version)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("root not found for version %d", version)
		}

		return nil
	}

	tree.rw.Lock()
	defer tree.rw.Unlock()

	tree.workingBytes = 0
	tree.workingSize = 0

	tree.root, err = tree.sql.LoadRoot(version)
	if err != nil {
		return err
	}

	tree.cache = make(map[string][]byte)
	tree.deleted = make(map[string]bool)

	return nil
}

func (tree *Tree) LoadSnapshot(version int64, traverseOrder TraverseOrderType) (err error) {
	tree.rw.Lock()
	defer tree.rw.Unlock()

	var v int64
	tree.root, v, err = tree.sql.ImportMostRecentSnapshot(version, traverseOrder, true)
	if err != nil {
		return err
	}
	if v < version {
		return fmt.Errorf("requested %d found snapshot %d, replay not yet supported", version, v)
	}
	tree.version.Store(v)
	tree.cache = make(map[string][]byte)
	tree.deleted = make(map[string]bool)
	return nil
}

func (tree *Tree) SaveSnapshot() (err error) {
	tree.rw.Lock()
	defer tree.rw.Unlock()

	ctx := context.Background()
	return tree.sql.Snapshot(ctx, tree)
}

func (tree *Tree) SaveVersion() ([]byte, int64, error) {
	tree.rw.Lock()
	defer tree.rw.Unlock()

	tree.version.Add(1)
	tree.resetSequences()

	treeVersion := tree.version.Load()

	if err := tree.sql.closeHangingIterators(); err != nil {
		return nil, 0, err
	}

	tree.sql.readPool.SetSavingTree()
	defer tree.sql.readPool.UnsetSavingTree()

	rootHash := tree.computeHash()

	err := tree.sqlWriter.saveTree(tree)
	if err != nil {
		return nil, treeVersion, err
	}

	tree.branchOrphans = nil
	tree.deleted = make(map[string]bool)
	tree.cache = make(map[string][]byte)

	if err := tree.sql.resetReadConn(); err != nil {
		return nil, treeVersion, err
	}

	if err := tree.sql.ResetShardQueries(); err != nil {
		return nil, treeVersion, err
	}

	tree.leafOrphans = nil
	tree.leaves = nil
	tree.branches = nil
	tree.deletes = nil

	return rootHash, treeVersion, nil
}

// ComputeHash the node and its descendants recursively. This usually mutates all
// descendant nodes. Returns the tree root node hash.
// If the tree is empty (i.e. the node is nil), returns the hash of an empty input,
// to conform with RFC-6962.
func (tree *Tree) computeHash() []byte {
	if tree.root == nil {
		return sha256.New().Sum(nil)
	}
	if tree.rootHashed && tree.root.hash != nil {
		return tree.root.hash
	}
	tree.deepHash(tree.root, 0)
	tree.rootHashed = true
	return tree.root.hash
}

func (tree *Tree) deepHash(node *Node, depth int8) {
	type nodeWithDepth struct {
		node    *Node
		depth   int8
		visited bool // Flag to track if children have been visited
	}

	treeVersion := tree.version.Load()

	// Estimate stack capacity based on tree height to avoid reallocations
	// 2^height is roughly the maximum number of nodes
	estimatedCapacity := min(1<<(node.subtreeHeight+1), 1024)
	stack := make([]nodeWithDepth, 0, estimatedCapacity)
	stack = append(stack, nodeWithDepth{node: node, depth: depth, visited: false})

	// Pre-allocate slices for append operations to avoid reallocations
	tree.branches = make([]*Node, 0, estimatedCapacity/2)
	tree.leaves = make([]*Node, 0, estimatedCapacity/2)

	// Track nodes that should be evicted but not returned to pool yet
	nodesToEvict := make(map[*Node]bool)
	// Track nodes that should be returned to pool after hash calculation
	nodesToReturn := make(map[*Node]bool)

	// Process nodes in a depth-first manner using a stack
	for len(stack) > 0 {
		// Pop from stack instead of peeking - reduces slice operations
		lastIdx := len(stack) - 1
		current := stack[lastIdx]
		stack = stack[:lastIdx]

		if current.node == nil {
			panic(fmt.Sprintf("node is nil; sql.path=%s", tree.sql.opts.Path))
		}

		// Check if this is a leaf node
		if current.node.isLeaf() {
			// new leaves are written every version
			if current.node.nodeKey.Version() == treeVersion {
				tree.leaves = append(tree.leaves, current.node)
			}
			continue // No further processing for leaf nodes
		}

		// Skip non-dirty or non-current version branch nodes
		if !current.node.dirty || current.node.nodeKey.Version() != treeVersion {
			continue
		}

		// If it's the first visit, process children
		if !current.visited {
			// Clear cached hash
			current.node.hash = nil

			// Push back the current node with visited flag set
			current.visited = true
			stack = append(stack, current)

			// Fetch both children at once to reduce function calls
			leftNode := current.node.leftNode
			rightNode := current.node.rightNode

			// Only fetch from storage if needed
			var err error
			if leftNode == nil {
				leftNode, err = current.node.getLeftNode(tree)
				if err != nil {
					panic(err) // Consistent with existing error handling
				}
			}

			if rightNode == nil {
				rightNode, err = current.node.getRightNode(tree)
				if err != nil {
					panic(err) // Consistent with existing error handling
				}
			}

			// Push children to process first (post-order traversal)
			// Add right then left, so left is processed first due to LIFO stack
			stack = append(stack, nodeWithDepth{node: rightNode, depth: current.depth + 1, visited: false})
			stack = append(stack, nodeWithDepth{node: leftNode, depth: current.depth + 1, visited: false})
			continue
		}

		// Process the node after its children have been visited
		tree.branches = append(tree.branches, current.node)
		current.node._hash()

		// Apply height filter if enabled - combined conditional checks
		if tree.heightFilter > 0 {
			leftNode := current.node.leftNode
			rightNode := current.node.rightNode

			if leftNode != nil && leftNode.isLeaf() {
				if !leftNode.dirty {
					nodesToReturn[leftNode] = true
				}
				current.node.leftNode = nil
			}

			if rightNode != nil && rightNode.isLeaf() {
				if !rightNode.dirty {
					nodesToReturn[rightNode] = true
				}
				current.node.rightNode = nil
			}
		}

		// Apply eviction if at or beyond the eviction depth
		if current.depth >= tree.evictionDepth {
			nodesToEvict[current.node] = true
		}
	}

	for node := range nodesToEvict {
		node.evictChildren()
	}

	for node := range nodesToReturn {
		tree.returnNode(node)
	}
}

func (tree *Tree) Get(key []byte) ([]byte, error) {
	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), metricsNamespace, "tree_get")
	}

	tree.rw.RLock()
	defer tree.rw.RUnlock()

	treeVersion := tree.version.Load()

	if tree.immutable {
		return tree.sql.GetAt(treeVersion, key)
	}

	if val, exists := tree.cache[string(key)]; exists {
		return val, nil
	}
	if _, exists := tree.deleted[string(key)]; exists {
		return nil, nil
	}

	return tree.sql.GetAt(treeVersion, key)

	// var (
	// 	res []byte
	// 	err error
	// )
	//
	// 	if tree.root == nil {
	// 		return nil, nil
	// 	}
	// 	_, res, err = tree.root.get(tree, key)
	//
	// return res, err
}

func (tree *Tree) Has(key []byte) (bool, error) {
	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), metricsNamespace, "tree_has")
	}

	tree.rw.RLock()
	defer tree.rw.RUnlock()

	treeVersion := tree.version.Load()

	if tree.immutable {
		val, err := tree.sql.GetAt(treeVersion, key)
		if err != nil {
			return false, err
		}
		return val != nil, nil
	}

	if val, exists := tree.cache[string(key)]; exists {
		return val != nil, nil
	}
	if _, exists := tree.deleted[string(key)]; exists {
		return false, nil
	}

	val, err := tree.sql.GetAt(treeVersion, key)
	if err != nil {
		return false, err
	}

	return val != nil, nil

	// var (
	// 	err error
	// 	val []byte
	// )
	// 	if tree.root == nil {
	// 		return false, nil
	// 	}
	// 	_, val, err = tree.root.get(tree, key)
	// if err != nil {
	// 	return false, err
	// }
	// return val != nil, nil
}

// Set sets a key in the working tree. Nil values are invalid. The given
// key/value byte slices must not be modified after this call, since they point
// to slices stored within IAVL. It returns true when an existing value was
// updated, while false means it was a new key.
func (tree *Tree) Set(key, value []byte) (updated bool, err error) {
	if tree.immutable {
		panic("set on immutable tree")
	}

	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), metricsNamespace, "tree_set")
	}

	tree.rw.Lock()
	defer tree.rw.Unlock()

	tree.rootHashed = false

	updated, err = tree.set(key, value)
	if err != nil {
		return false, err
	}
	if updated {
		tree.metrics.IncrCounter(1, metricsNamespace, "tree_update")
	} else {
		tree.metrics.IncrCounter(1, metricsNamespace, "tree_new_node")
	}

	tree.cache[string(key)] = value
	delete(tree.deleted, string(key))

	return updated, nil
}

func (tree *Tree) set(key []byte, value []byte) (updated bool, err error) {
	if value == nil {
		return updated, fmt.Errorf("attempt to store nil value at key '%s'", key)
	}

	if tree.root == nil {
		tree.root = tree.NewLeafNode(key, value)
		return updated, nil
	}

	tree.root, updated, err = tree.recursiveSet(tree.root, key, value)
	return updated, err
}

func (tree *Tree) recursiveSet(node *Node, key []byte, value []byte) (
	newSelf *Node, updated bool, err error,
) {
	// Define a struct to track our traversal state
	type setFrame struct {
		node    *Node
		key     []byte
		value   []byte
		goLeft  bool // Whether we should go left or right from this node
		visited bool // Whether this node's children have been processed
	}

	// Create stack of frames to process
	stack := make([]setFrame, 0, 32) // Initial capacity to reduce allocations
	stack = append(stack, setFrame{
		node:    node,
		key:     key,
		value:   value,
		goLeft:  bytes.Compare(key, node.key) < 0,
		visited: false,
	})

	// Process frames in a loop until stack is empty
	var currentNode *Node
	childMap := make(map[*Node]*Node) // Maps parent nodes to their processed children

	for len(stack) > 0 {
		// Get current frame from the top of stack
		currentIndex := len(stack) - 1
		currentFrame := &stack[currentIndex]
		currentNode = currentFrame.node

		// Handle nil node (should never happen)
		if currentNode == nil {
			panic("node is nil")
		}

		// Handle leaf nodes directly - no recursion needed
		if currentNode.isLeaf() {
			// Pop the frame
			stack = stack[:currentIndex]

			switch bytes.Compare(currentFrame.key, currentNode.key) {
			case -1: // setKey < leafKey
				tree.metrics.IncrCounter(2, metricsNamespace, "pool_get")
				parent := tree.pool.Get()
				parent.nodeKey = tree.nextNodeKey()
				parent.key = currentNode.key
				parent.subtreeHeight = 1
				parent.size = 2
				parent.dirty = true
				parent.setLeft(tree.NewLeafNode(currentFrame.key, currentFrame.value))
				parent.setRight(currentNode)

				tree.workingBytes += parent.sizeBytes()
				tree.workingSize++

				// Store result for parent frame
				if len(stack) > 0 {
					parentFrame := &stack[len(stack)-1]
					childMap[parentFrame.node] = parent
				} else {
					return parent, false, nil
				}
			case 1: // setKey > leafKey
				tree.metrics.IncrCounter(2, metricsNamespace, "pool_get")
				parent := tree.pool.Get()
				parent.nodeKey = tree.nextNodeKey()
				parent.key = currentFrame.key
				parent.subtreeHeight = 1
				parent.size = 2
				parent.dirty = true
				parent.setLeft(currentNode)
				parent.setRight(tree.NewLeafNode(currentFrame.key, currentFrame.value))

				tree.workingBytes += parent.sizeBytes()
				tree.workingSize++

				// Store result for parent frame
				if len(stack) > 0 {
					parentFrame := &stack[len(stack)-1]
					childMap[parentFrame.node] = parent
				} else {
					return parent, false, nil
				}
			default: // Equal keys - update the value
				tree.addOrphan(currentNode)
				wasDirty := currentNode.dirty
				tree.mutateNode(currentNode)
				if tree.isReplaying {
					currentNode.hash = currentFrame.value
				} else {
					if wasDirty {
						tree.workingBytes -= currentNode.sizeBytes()
					}
					currentNode.value = currentFrame.value
					currentNode._hash()
					if !tree.storeLeafValues {
						currentNode.value = nil
					}
					tree.workingBytes += currentNode.sizeBytes()
				}

				// Store result for parent frame
				if len(stack) > 0 {
					parentFrame := &stack[len(stack)-1]
					childMap[parentFrame.node] = currentNode
					updated = true
				} else {
					return currentNode, true, nil
				}
			}
		} else {
			// Handle internal nodes
			if !currentFrame.visited {
				// Mark as visited to avoid repeated work
				currentFrame.visited = true
				stack[currentIndex] = *currentFrame

				// First visit: add orphan and mutate node
				tree.addOrphan(currentNode)
				tree.mutateNode(currentNode)

				// Add frame for child node traversal
				var childNode *Node
				if currentFrame.goLeft {
					childNode = currentNode.left(tree)
				} else {
					childNode = currentNode.right(tree)
				}

				// Push child onto stack
				stack = append(stack, setFrame{
					node:    childNode,
					key:     currentFrame.key,
					value:   currentFrame.value,
					goLeft:  bytes.Compare(currentFrame.key, childNode.key) < 0,
					visited: false,
				})
			} else {
				// Pop frame from stack
				stack = stack[:currentIndex]

				// Get processed child node
				childNode, exists := childMap[currentNode]
				if !exists {
					return nil, false, fmt.Errorf("internal error: child not found for node")
				}

				// Delete from map to prevent memory leaks
				delete(childMap, currentNode)

				// Apply the child node to the current node
				if currentFrame.goLeft {
					currentNode.setLeft(childNode)
				} else {
					currentNode.setRight(childNode)
				}

				// If the child was updated, no need for balancing
				if updated {
					// Store result for parent frame if not at root
					if len(stack) > 0 {
						parentFrame := &stack[len(stack)-1]
						childMap[parentFrame.node] = currentNode
					} else {
						return currentNode, updated, nil
					}
				} else {
					// Perform height/size calculation and balancing
					err = currentNode.calcHeightAndSize(tree)
					if err != nil {
						return nil, false, err
					}

					newNode, err := tree.balance(currentNode)
					if err != nil {
						return nil, false, err
					}

					// Store result for parent frame if not at root
					if len(stack) > 0 {
						parentFrame := &stack[len(stack)-1]
						childMap[parentFrame.node] = newNode
					} else {
						return newNode, updated, nil
					}
				}
			}
		}
	}

	// We should never reach here if the algorithm is implemented correctly
	return node, false, fmt.Errorf("unexpected exit from recursiveSet")
}

// Remove removes a key from the working tree. The given key byte slice should not be modified
// after this call, since it may point to data stored inside IAVL.
func (tree *Tree) Remove(key []byte) ([]byte, bool, error) {
	if tree.immutable {
		panic("Remove on immutable tree")
	}

	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), metricsNamespace, "tree_remove")
	}

	tree.rw.Lock()
	defer tree.rw.Unlock()

	if tree.root == nil {
		return nil, false, nil
	}
	newRoot, _, value, removed, err := tree.recursiveRemove(tree.root, key)
	if err != nil {
		return nil, false, err
	}
	if !removed {
		return nil, false, nil
	}

	delete(tree.cache, string(key))
	tree.deleted[string(key)] = true

	tree.metrics.IncrCounter(1, metricsNamespace, "tree_delete")

	tree.root = newRoot
	return value, true, nil
}

// removes the node corresponding to the passed key and balances the tree.
// It returns:
// - the hash of the new node (or nil if the node is the one removed)
// - the node that replaces the orig. node after remove
// - new leftmost leaf key for tree after successfully removing 'key' if changed.
// - the removed value
func (tree *Tree) recursiveRemove(node *Node, key []byte) (newSelf *Node, newKey []byte, newValue []byte, removed bool, err error) {
	// Define a struct to track our traversal state
	type removeFrame struct {
		node    *Node
		key     []byte
		visited bool // Whether this node's children have been processed
		goLeft  bool // Whether we go left or right from this node
	}

	// Create stack of frames to process
	stack := make([]removeFrame, 0, 32) // Initial capacity to reduce allocations
	stack = append(stack, removeFrame{
		node:    node,
		key:     key,
		visited: false,
		goLeft:  bytes.Compare(key, node.key) < 0,
	})

	// Maps parent nodes to their processed children and results
	type resultInfo struct {
		node       *Node  // The new node replacing the old one
		newKey     []byte // New key (if any)
		value      []byte // Value removed (if any)
		wasRemoved bool   // Whether a removal occurred in this subtree
	}
	resultMap := make(map[*Node]resultInfo)

	// Track nodes that should be returned to the pool after the operation
	nodesToReturn := make(map[*Node]bool)

	// Process frames until the stack is empty
	for len(stack) > 0 {
		// Get current frame from the top of stack
		currentIndex := len(stack) - 1
		currentFrame := &stack[currentIndex]
		currentNode := currentFrame.node

		// Handle leaf nodes directly
		if currentNode.isLeaf() {
			// Pop the frame
			stack = stack[:currentIndex]

			var result resultInfo

			// Check if this is the leaf we're looking for
			if bytes.Equal(currentFrame.key, currentNode.key) {
				// Found the node to remove
				tree.addDelete(currentNode)
				nodesToReturn[currentNode] = true

				result = resultInfo{
					node:       nil,
					newKey:     nil,
					value:      currentNode.value,
					wasRemoved: true,
				}
			} else {
				// This leaf doesn't match the key, keep it
				result = resultInfo{
					node:       currentNode,
					newKey:     nil,
					value:      nil,
					wasRemoved: false,
				}
			}

			// Store result for parent frame
			if len(stack) > 0 {
				parentFrame := &stack[len(stack)-1]
				resultMap[parentFrame.node] = result
			} else {
				// We're at the root
				// Return nodes to pool now that we're done
				for n := range nodesToReturn {
					tree.returnNode(n)
				}
				return result.node, result.newKey, result.value, result.wasRemoved, nil
			}
			continue
		}

		// Handle internal nodes
		if !currentFrame.visited {
			// Mark as visited for the next iteration
			currentFrame.visited = true
			stack[currentIndex] = *currentFrame

			// Visit the appropriate child node based on key comparison
			var childNode *Node
			if currentFrame.goLeft {
				childNode = currentNode.left(tree)
			} else {
				childNode = currentNode.right(tree)
			}

			// Add child to the stack
			stack = append(stack, removeFrame{
				node:    childNode,
				key:     currentFrame.key,
				visited: false,
				goLeft:  bytes.Compare(currentFrame.key, childNode.key) < 0,
			})
		} else {
			// Pop the frame since we've processed its children
			stack = stack[:currentIndex]

			// Get the result from the processed child
			childResult, exists := resultMap[currentNode]
			if !exists {
				return nil, nil, nil, false, fmt.Errorf("internal error: child result not found")
			}

			// Clean up the result map to prevent memory leaks
			delete(resultMap, currentNode)

			// If nothing was removed in the subtree, just pass it up
			if !childResult.wasRemoved {
				if len(stack) > 0 {
					parentFrame := &stack[len(stack)-1]
					resultMap[parentFrame.node] = childResult
				} else {
					// We're at the root
					// Return nodes to pool now that we're done
					for n := range nodesToReturn {
						tree.returnNode(n)
					}
					return childResult.node, childResult.newKey, childResult.value, childResult.wasRemoved, nil
				}
				continue
			}

			// We need to update the current node based on the removal result
			tree.addOrphan(currentNode)

			var resultNode *Node
			var resultKey []byte

			if currentFrame.goLeft {
				// Left child was affected
				if childResult.node == nil {
					// Left node held value, was removed
					// Collapse `node.rightNode` into `node`
					rightNode := currentNode.right(tree)
					resultKey = currentNode.key // Important: pass the current node's key up
					nodesToReturn[currentNode] = true
					resultNode = rightNode
				} else {
					// Left subtree changed but node wasn't removed
					tree.mutateNode(currentNode)
					currentNode.setLeft(childResult.node)

					// Update node's height and size
					err := currentNode.calcHeightAndSize(tree)
					if err != nil {
						return nil, nil, nil, false, err
					}

					// Balance the node
					resultNode, err = tree.balance(currentNode)
					if err != nil {
						return nil, nil, nil, false, err
					}

					// Important: propagate the new key if there is one from child
					resultKey = childResult.newKey
				}
			} else {
				// Right child was affected
				if childResult.node == nil {
					// Right node held value, was removed
					// Collapse `node.leftNode` into `node`
					leftNode := currentNode.left(tree)
					nodesToReturn[currentNode] = true
					resultNode = leftNode
					// No new key when right node is removed and replaced with left node
				} else {
					// Right subtree changed but node wasn't removed
					tree.mutateNode(currentNode)
					currentNode.setRight(childResult.node)

					// Update key if needed (this is crucial for correct hash calculation)
					if childResult.newKey != nil {
						currentNode.key = childResult.newKey
					}

					// Update node's height and size
					err := currentNode.calcHeightAndSize(tree)
					if err != nil {
						return nil, nil, nil, false, err
					}

					// Balance the node
					resultNode, err = tree.balance(currentNode)
					if err != nil {
						return nil, nil, nil, false, err
					}
				}
			}

			// Create result for this node
			result := resultInfo{
				node:       resultNode,
				newKey:     resultKey,
				value:      childResult.value,
				wasRemoved: true,
			}

			// Store result for parent frame or return final result
			if len(stack) > 0 {
				parentFrame := &stack[len(stack)-1]
				resultMap[parentFrame.node] = result
			} else {
				// We're at the root
				// Return nodes to pool now that we're done
				for n := range nodesToReturn {
					tree.returnNode(n)
				}
				return result.node, result.newKey, result.value, result.wasRemoved, nil
			}
		}
	}

	// We should never reach here
	// If we somehow do, make sure to clean up
	for n := range nodesToReturn {
		tree.returnNode(n)
	}
	return nil, nil, nil, false, fmt.Errorf("unexpected exit from recursiveRemove")
}

func (tree *Tree) Size() int64 {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	return tree.root.size
}

func (tree *Tree) Height() int8 {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	return tree.root.subtreeHeight
}

func (tree *Tree) nextNodeKey() NodeKey {
	tree.branchSequence++
	nk := NewNodeKey(tree.version.Load()+1, tree.branchSequence)
	return nk
}

func (tree *Tree) nextLeafNodeKey() NodeKey {
	tree.leafSequence++
	if tree.leafSequence < leafSequenceStart {
		panic("leaf sequence underflow")
	}
	nk := NewNodeKey(tree.version.Load()+1, tree.leafSequence)
	return nk
}

func (tree *Tree) resetSequences() {
	tree.leafSequence = leafSequenceStart
	tree.branchSequence = 0
}

func (tree *Tree) mutateNode(node *Node) {
	// Check if the node has already been mutated for the next version
	alreadyMutatedForNextVersion := node.hash == nil && node.nodeKey.Version() == tree.version.Load()+1

	// Even if node appears to be mutated for next version, we always need to:
	// 1. Set the hash to nil to force recalculation
	// 2. Mark the node as dirty to ensure tracking

	// Always reset the hash (no early return)
	node.hash = nil

	// Only update the node key if it's not already for the next version
	if !alreadyMutatedForNextVersion {
		if node.isLeaf() {
			node.nodeKey = tree.nextLeafNodeKey()
		} else {
			node.nodeKey = tree.nextNodeKey()
		}
	}

	// Only update dirty flag and working stats if not already dirty
	if !node.dirty {
		node.dirty = true
		tree.workingSize++
		if !node.isLeaf() {
			tree.workingBytes += node.sizeBytes()
		}
	}
}

func (tree *Tree) addOrphan(node *Node) {
	if node.hash == nil {
		return
	}
	if !node.isLeaf() {
		tree.branchOrphans = append(tree.branchOrphans, node.nodeKey)
	} else if node.isLeaf() && !node.dirty {
		tree.leafOrphans = append(tree.leafOrphans, node.nodeKey)
	}
}

func (tree *Tree) addDelete(node *Node) {
	// added and removed in the same version; no op.
	if node.nodeKey.Version() == tree.version.Load()+1 {
		return
	}
	del := &nodeDelete{
		deleteKey: tree.nextLeafNodeKey(),
		leafKey:   node.key,
	}
	tree.deletes = append(tree.deletes, del)
}

// NewLeafNode returns a new node from a key, value and version.
func (tree *Tree) NewLeafNode(key []byte, value []byte) *Node {
	node := tree.pool.Get()

	node.nodeKey = tree.nextLeafNodeKey()

	node.key = key
	node.subtreeHeight = 0
	node.size = 1

	if tree.isReplaying {
		node.hash = value
	} else {
		node.value = value
		node._hash()
		if !tree.storeLeafValues {
			node.value = nil
		}
	}

	node.dirty = true
	tree.workingBytes += node.sizeBytes()
	tree.workingSize++
	return node
}

func (tree *Tree) returnNode(node *Node) {
	if node.dirty {
		tree.workingBytes -= node.sizeBytes()
		tree.workingSize--
	}
	tree.pool.Put(node)
}

func (tree *Tree) Close() error {
	if tree.writerCancel != nil {
		tree.writerCancel()
	}
	return tree.sql.Close()
}

func (tree *Tree) Hash() []byte {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	if tree.root == nil {
		return emptyHash
	}
	return tree.root.hash
}

func (tree *Tree) Version() int64 {
	return tree.version.Load()
}

func (tree *Tree) replayChangelog(toVersion int64, targetHash []byte) error {
	return tree.sql.replayChangelog(tree, toVersion, targetHash)
}

func (tree *Tree) DeleteVersionsTo(toVersion int64) error {
	tree.sqlWriter.treePruneCh <- &pruneSignal{pruneVersion: toVersion}
	tree.sqlWriter.leafPruneCh <- &pruneSignal{pruneVersion: toVersion}
	return nil
}

func (tree *Tree) WorkingBytes() uint64 {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	return tree.workingBytes
}

func (tree *Tree) GetWithIndex(key []byte) (int64, []byte, error) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	if tree.root == nil {
		return 0, nil, nil
	}

	return tree.root.get(tree, key)
}

func (tree *Tree) GetByIndex(index int64) (key []byte, value []byte, err error) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	if tree.root == nil {
		return nil, nil, nil
	}

	return tree.getByIndex(tree.root, index)
}

func (tree *Tree) getByIndex(node *Node, index int64) (key []byte, value []byte, err error) {
	if node.isLeaf() {
		if index == 0 {
			return node.key, node.value, nil
		}
		return nil, nil, nil
	}

	leftNode, err := node.getLeftNode(tree)
	if err != nil {
		return nil, nil, err
	}

	if index < leftNode.size {
		return tree.getByIndex(leftNode, index)
	}

	rightNode, err := node.getRightNode(tree)
	if err != nil {
		return nil, nil, err
	}

	return tree.getByIndex(rightNode, index-leftNode.size)
}

func (tree *Tree) SetInitialVersion(version int64) error {
	if tree.immutable {
		panic("set initial version on immutable tree")
	}

	var err error

	tree.version.Store(version - 1)

	return err
}

func (tree *Tree) GetRecent(version int64, key []byte) (bool, []byte, error) {
	got, root := tree.getRecentRoot(version)
	if !got {
		return false, nil, nil
	}
	if root == nil {
		return true, nil, nil
	}

	tree.rw.RLock()
	defer tree.rw.RUnlock()

	_, val, err := root.get(tree, key)
	return true, val, err
}

func (tree *Tree) getRecentRoot(version int64) (bool, *Node) {
	if version != tree.version.Load() {
		return false, nil
	}

	// Version == 0
	if tree.root == nil {
		return false, nil
	}

	root := *tree.root

	return true, &root
}

func (tree *Tree) IterateRecent(version int64, start, end []byte, ascending bool) (bool, Iterator) {
	got, root := tree.getRecentRoot(version)
	if !got {
		return false, nil
	}
	itr := &TreeIterator{
		tree:      tree,
		start:     start,
		end:       end,
		ascending: ascending,
		inclusive: false,
		stack:     []*Node{root},
		valid:     true,
		metrics:   tree.metricsProxy,
	}
	itr.Next()
	return true, itr
}

func (tree *Tree) Import(version int64) (*Importer, error) {
	return newImporter(tree, version)
}

func (tree *Tree) WorkingHash() []byte {
	tree.rw.Lock()
	defer tree.rw.Unlock()

	if tree.root == nil {
		return emptyHash
	}

	if tree.root.hash != nil {
		return tree.root.hash
	}

	oldVersion := tree.version.Load()
	tree.version.Add(1)
	tree.resetSequences()

	// if err := tree.sql.closeHangingIterators(); err != nil {
	// 	panic(err)
	// }

	hash := tree.computeHash()

	tree.version.Store(oldVersion)
	tree.resetSequences()

	return hash
}

func (tree *Tree) GetImmutable(version int64) (*Tree, error) {
	exists, err := tree.sql.HasRoot(version)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("root not found for version %d", version)
	}

	// We can discard whole pool after usage
	pool := NewNodePool()

	sql := &SqliteDb{
		opts:        tree.sql.opts,
		pool:        pool,
		readPool:    tree.sql.readPool,
		metrics:     tree.sql.metrics,
		logger:      tree.sql.logger,
		useReadPool: true,
	}

	imTree := &Tree{
		immutable:       true,
		sql:             sql,
		sqlWriter:       nil,
		writerCancel:    nil,
		pool:            sql.pool,
		metrics:         tree.metrics,
		maxWorkingSize:  tree.maxWorkingSize,
		storeLeafValues: tree.storeLeafValues,
		heightFilter:    tree.heightFilter,
		metricsProxy:    tree.metricsProxy,
		evictionDepth:   tree.evictionDepth,
		leafSequence:    leafSequenceStart,
		rootHashed:      true,
		cache:           make(map[string][]byte),
		deleted:         make(map[string]bool),
	}

	imTree.version.Store(version)

	return imTree, nil
}

func (tree *Tree) GetImmutableProvable(version int64) (*Tree, error) {
	// We can discard whole pool after usage
	pool := NewNodePool()

	sql := &SqliteDb{
		opts:        tree.sql.opts,
		pool:        pool,
		readPool:    tree.sql.readPool,
		metrics:     tree.sql.metrics,
		logger:      tree.sql.logger,
		useReadPool: true,
	}

	imTree := &Tree{
		sql:             sql,
		sqlWriter:       nil,
		writerCancel:    nil,
		pool:            sql.pool,
		metrics:         tree.metrics,
		maxWorkingSize:  tree.maxWorkingSize,
		storeLeafValues: tree.storeLeafValues,
		heightFilter:    tree.heightFilter,
		metricsProxy:    tree.metricsProxy,
		evictionDepth:   tree.evictionDepth,
		leafSequence:    leafSequenceStart,
		rootHashed:      true,
		cache:           make(map[string][]byte),
		deleted:         make(map[string]bool),
	}

	if err := imTree.LoadVersion(version); err != nil {
		return nil, err
	}

	imTree.immutable = true

	return imTree, nil
}

func (tree *Tree) DiscardImmutableTree() error {
	tree.sql.leafWrite = nil
	tree.sql.treeWrite = nil
	tree.sql.read = nil
	tree.sql.readPool = nil
	tree.sql.pool = nil
	return tree.Close()
}

func (tree *Tree) VersionExists(version int64) (bool, error) {
	exists, err := tree.sql.HasRoot(version)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (tree *Tree) GetFromRoot(key []byte) ([]byte, error) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	_, val, err := tree.root.get(tree, key)
	return val, err
}

func (tree *Tree) IteratorLeavesAt(version int64) (Iterator, error) {
	return tree.IteratorAt(version, nil, nil, true)
}

func (tree *Tree) Path() string {
	return tree.sql.opts.Path
}

func (tree *Tree) Revert(version int64) error {
	return tree.sql.Revert(version)
}
