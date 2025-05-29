package iavl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"time"

	"github.com/eatonphil/gosqlite"

	"github.com/cosmos/iavl/v2/metrics"
)

type Iterator interface {
	// Domain returns the start (inclusive) and end (exclusive) limits of the iterator.
	// CONTRACT: start, end readonly []byte
	Domain() (start []byte, end []byte)

	// Valid returns whether the current iterator is valid. Once invalid, the TreeIterator remains
	// invalid forever.
	Valid() bool

	// Next moves the iterator to the next key in the database, as defined by order of iteration.
	// If Valid returns false, this method will panic.
	Next()

	// Key returns the key at the current position. Panics if the iterator is invalid.
	// CONTRACT: key readonly []byte
	Key() (key []byte)

	// Value returns the value at the current position. Panics if the iterator is invalid.
	// CONTRACT: value readonly []byte
	Value() (value []byte)

	// Error returns the last error encountered by the iterator, if any.
	Error() error

	// Close closes the iterator, releasing any allocated resources.
	Close() error
}

var (
	_ Iterator = (*TreeIterator)(nil)
	_ Iterator = (*KVIterator)(nil)
	_ Iterator = (*WrongBranchHashIterator)(nil)
)

type iteratorStackEntry struct {
	node  *Node
	state int // 0: process left, 1: process right, 2: process self
}

type TreeIterator struct {
	tree       *Tree
	start, end []byte // iteration domain
	ascending  bool   // ascending traversal
	inclusive  bool   // end key inclusiveness

	stack   []iteratorStackEntry
	started bool

	key, value []byte // current key, value
	err        error  // current error
	valid      bool   // iteration status

	metrics metrics.Proxy
}

func (i *TreeIterator) Domain() (start []byte, end []byte) {
	return i.start, i.end
}

func (i *TreeIterator) Valid() bool {
	return i.valid
}

func (i *TreeIterator) Next() {
	defer func() {
		if !i.valid {
			i.Close()
		}
	}()

	if i.metrics != nil {
		defer i.metrics.MeasureSince(time.Now(), "iavl2", "iterator", "next")
	}
	if !i.valid {
		return
	}
	if len(i.stack) == 0 {
		i.valid = false
		return
	}
	if i.ascending {
		i.stepAscend()
	} else {
		i.stepDescend()
	}
	i.started = true
}

func (i *TreeIterator) stepAscend() {
	for len(i.stack) > 0 {
		currentEntry := &i.stack[len(i.stack)-1]
		node := currentEntry.node

		if node.isLeaf() {
			if !i.started && bytes.Compare(node.Key(), i.start) < 0 {
				// Skip this leaf and remove from stack
				i.removeNodeFromStack()
				continue
			}
			if i.isPastEndAscend(node.Key()) {
				i.valid = false
				return
			}

			// Found valid leaf
			i.key = node.Key()
			i.value = node.Value()

			i.removeNodeFromStack()
			return
		}

		// Handle internal node based on state
		switch currentEntry.state {
		case 0: // Process left subtree first (for ascending order)
			currentEntry.state = 1 // Advance state for next iteration

			// For ascending order, we need to check if we should traverse left subtree
			// We traverse left if start is less than current node's key
			if i.start == nil || bytes.Compare(i.start, node.Key()) < 0 {
				left, err := node.getLeftNode(i.tree)
				if err != nil {
					i.err = err
					i.valid = false
					return
				}
				if left != nil {
					i.addNodeToStack(left, 0)
					// Continue to process the left child
					continue
				}
			}
			// No left child to process or shouldn't traverse left, continue to state 1 in next iteration

		case 1: // Process right subtree
			currentEntry.state = 2 // Advance state for next iteration

			right, err := node.getRightNode(i.tree)
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
			if right != nil {
				i.addNodeToStack(right, 0)
				// Continue to process the right child
				continue
			}
			// No right child to process, continue to state 2 in next iteration

		case 2: // Done with this internal node
			i.removeNodeFromStack()
		}
	}

	i.valid = false
}

func (i *TreeIterator) stepDescend() {
	for len(i.stack) > 0 {
		currentEntry := &i.stack[len(i.stack)-1]
		node := currentEntry.node

		if node.isLeaf() {
			if !i.started && i.end != nil {
				res := bytes.Compare(i.end, node.Key())
				// if end is inclusive and the key is greater than end, skip
				if i.inclusive && res < 0 {
					// Skip this leaf and remove from stack
					i.removeNodeFromStack()
					continue
				}
				// if end is not inclusive (default) and the key is greater than or equal to end, skip
				if res <= 0 {
					// Skip this leaf and remove from stack
					i.removeNodeFromStack()
					continue
				}
			}
			if i.isPastEndDescend(node.Key()) {
				i.valid = false
				return
			}

			// Found valid leaf
			i.key = node.Key()
			i.value = node.Value()

			i.removeNodeFromStack()
			return
		}

		// Handle internal node based on state
		switch currentEntry.state {
		case 0: // Process right subtree first (for descending order)
			currentEntry.state = 1 // Advance state for next iteration

			// For descending order, we need to check if we should traverse right subtree
			// We traverse right if end is nil or current node's key is <= end
			if i.end == nil || bytes.Compare(node.Key(), i.end) <= 0 {
				right, err := node.getRightNode(i.tree)
				if err != nil {
					i.err = err
					i.valid = false
					return
				}
				if right != nil {
					i.addNodeToStack(right, 0)
					// Continue to process the right child
					continue
				}
			}
			// No right child to process or shouldn't traverse right, continue to state 1 in next iteration

		case 1: // Process left subtree
			currentEntry.state = 2 // Advance state for next iteration

			left, err := node.getLeftNode(i.tree)
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
			if left != nil {
				i.addNodeToStack(left, 0)
				// Continue to process the left child
				continue
			}
			// No left child to process, continue to state 2 in next iteration

		case 2: // Done with this internal node
			// Remove from stack, mark as processed and notify parent
			i.removeNodeFromStack()
		}
	}

	// Stack is empty
	i.valid = false
}

func (i *TreeIterator) isPastEndAscend(key []byte) bool {
	if i.end == nil {
		return false
	}
	if i.inclusive {
		return bytes.Compare(key, i.end) > 0
	}
	return bytes.Compare(key, i.end) >= 0
}

func (i *TreeIterator) isPastEndDescend(key []byte) bool {
	if i.start == nil {
		return false
	}
	return bytes.Compare(key, i.start) < 0
}

func (i *TreeIterator) Key() (key []byte) {
	return i.key
}

func (i *TreeIterator) Value() (value []byte) {
	v := make([]byte, len(i.value))
	copy(v, i.value)

	return v
}

func (i *TreeIterator) Error() error {
	return i.err
}

func (i *TreeIterator) Close() error {
	i.stack = nil
	i.valid = false
	i.tree.sql.pool = nil
	return i.err
}

func (i *TreeIterator) addNodeToStack(node *Node, state int) {
	i.stack = append(i.stack, iteratorStackEntry{node: node, state: state})
}

func (i *TreeIterator) removeNodeFromStack() *Node {
	if len(i.stack) == 0 {
		return nil
	}

	lastEntry := i.stack[len(i.stack)-1]
	node := lastEntry.node

	i.stack = i.stack[:len(i.stack)-1]

	return node
}

func (i *TreeIterator) initializeIteratorStack(root *Node) {
	if root != nil {
		i.addNodeToStack(root, 0)
	}
}

func newIterTree(tree *Tree) *Tree {
	pool := NewNodePool()

	sql := &SqliteDb{
		opts:        tree.sql.opts,
		pool:        pool,
		readPool:    tree.sql.readPool,
		metrics:     tree.sql.metrics,
		logger:      tree.sql.logger,
		useReadPool: true,
	}

	itTree := &Tree{
		sql:            sql,
		sqlWriter:      nil,
		writerCancel:   nil,
		pool:           sql.pool,
		metrics:        tree.metrics,
		maxWorkingSize: tree.maxWorkingSize,
		heightFilter:   tree.heightFilter,
		metricsProxy:   tree.metricsProxy,
		leafSequence:   leafSequenceStart,
		hashedVersion:  tree.version.Load(),
		cache:          make(map[string][]byte),
		deleted:        make(map[string]bool),
		immutable:      true,
	}

	if tree.root != nil {
		key := make([]byte, len(tree.root.key))
		copy(key, tree.root.key)

		value := make([]byte, len(tree.root.value))
		copy(value, tree.root.value)

		root := &Node{
			subtreeHeight: tree.root.SubTreeHeight(),
			nodeKey:       tree.root.NodeKey(),
			size:          tree.root.size,
			key:           key,
			hash:          tree.root.Hash(),
			value:         value,
			leftNodeKey:   tree.root.leftNodeKey,
			rightNodeKey:  tree.root.rightNodeKey,
			leftNode:      tree.root.leftNode,
			rightNode:     tree.root.rightNode,
			source:        ManualNode,
		}

		itTree.root = root
	}

	return itTree
}

func (tree *Tree) Iterator(start, end []byte, inclusive bool) (itr Iterator, err error) {
	// if tree.immutable {
	// 	return tree.IteratorAt(tree.version.Load(), start, end, inclusive)
	// }

	tree.rw.RLock()
	defer tree.rw.RUnlock()

	itTree := newIterTree(tree)

	itr = &TreeIterator{
		tree:      itTree,
		start:     start,
		end:       end,
		ascending: true,
		inclusive: inclusive,
		valid:     itTree.root != nil,
		stack:     nil, // Will be initialized properly below
		metrics:   tree.metricsProxy,
	}

	// Properly initialize the stack and index map
	treeItr := itr.(*TreeIterator)
	treeItr.initializeIteratorStack(itTree.root)

	if tree.metricsProxy != nil {
		tree.metricsProxy.IncrCounter(1, "iavl2", "iterator", "open")
	}

	if itTree.root != nil {
		itr.Next()
	}
	return itr, err
}

func (tree *Tree) ReverseIterator(start, end []byte) (itr Iterator, err error) {
	// if tree.immutable {
	// 	return tree.ReverseIteratorAt(tree.version.Load(), start, end)
	// }

	tree.rw.RLock()
	defer tree.rw.RUnlock()

	itTree := newIterTree(tree)

	itr = &TreeIterator{
		tree:      itTree,
		start:     start,
		end:       end,
		ascending: false,
		inclusive: false,
		valid:     itTree.root != nil,
		stack:     nil, // Will be initialized properly below
		metrics:   tree.metricsProxy,
	}

	// Properly initialize the stack and index map
	treeItr := itr.(*TreeIterator)
	treeItr.initializeIteratorStack(itTree.root)

	if tree.metricsProxy != nil {
		tree.metricsProxy.IncrCounter(1, "iavl2", "iterator", "open")
	}

	if itTree.root != nil {
		itr.Next()
	}
	return itr, nil
}

func (tree *Tree) IterateRecent(version int64, start, end []byte, ascending bool) (bool, Iterator) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	got, _ := tree.getRecentRoot(version)
	if !got {
		return false, nil
	}

	itTree := newIterTree(tree)

	itr := &TreeIterator{
		tree:      itTree,
		start:     start,
		end:       end,
		ascending: ascending,
		inclusive: false,
		valid:     true,
		stack:     nil, // Will be initialized properly below
		metrics:   tree.metricsProxy,
	}

	// Properly initialize the stack and index map
	itr.initializeIteratorStack(itTree.root)

	if tree.metricsProxy != nil {
		tree.metricsProxy.IncrCounter(1, "iavl2", "iterator", "open")
	}

	if itTree.root != nil {
		itr.Next()
	}

	return true, itr
}

type KVIterator struct {
	sql       *SqliteDb
	itrStmt   *gosqlite.Stmt
	start     []byte
	end       []byte
	valid     bool
	err       error
	key       []byte
	value     []byte
	metrics   metrics.Proxy
	itrIdx    int
	ascending bool
	inclusive bool
}

func (i *KVIterator) Domain() (start []byte, end []byte) {
	return i.start, i.end
}

func (i *KVIterator) Valid() bool {
	return i.valid
}

func (i *KVIterator) Next() {
	if i.metrics != nil {
		defer i.metrics.MeasureSince(time.Now(), "iavl2", "kv iterator", "next")
	}
	if !i.valid {
		return
	}

	hasRow, err := i.itrStmt.Step()
	if err != nil {
		closeErr := i.Close()
		if closeErr != nil {
			i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}
	if !hasRow {
		closeErr := i.Close()
		if closeErr != nil {
			i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}

	var nodeBz gosqlite.RawBytes
	if err = i.itrStmt.Scan(&i.key, &nodeBz); err != nil {
		closeErr := i.Close()
		if closeErr != nil {
			i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}

	i.value, err = extractValue(nodeBz)
	if err != nil {
		closeErr := i.Close()
		if closeErr != nil {
			i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}
}

func (i *KVIterator) Key() (key []byte) {
	return i.key
}

func (i *KVIterator) Value() (value []byte) {
	return i.value
}

func (i *KVIterator) Error() error {
	return i.err
}

func (i *KVIterator) Close() error {
	if i.valid {
		if i.metrics != nil {
			i.metrics.IncrCounter(1, "iavl2", "iterator", "close")
		}
		i.valid = false
		return i.sql.readPool.CloseKVIterstor(i.itrIdx)
	}
	return nil
}

func (tree *Tree) IteratorAt(version int64, start, end []byte, inclusive bool) (Iterator, error) {
	var err error
	kvItr := &KVIterator{
		sql:     tree.sql,
		start:   start,
		end:     end,
		valid:   true,
		metrics: tree.metricsProxy,
	}

	kvItr.itrStmt, kvItr.itrIdx, err = tree.sql.getKVIteratorQuery(version, start, end, true, inclusive)
	if err != nil {
		return nil, err
	}

	if tree.metricsProxy != nil {
		tree.metricsProxy.IncrCounter(1, "iavl2", "iterator", "open")
	}

	kvItr.Next()

	return kvItr, err
}

func (tree *Tree) ReverseIteratorAt(version int64, start, end []byte) (Iterator, error) {
	var err error
	kvItr := &KVIterator{
		sql:     tree.sql,
		start:   start,
		end:     end,
		valid:   true,
		metrics: tree.metricsProxy,
	}

	kvItr.itrStmt, kvItr.itrIdx, err = tree.sql.getKVIteratorQuery(tree.version.Load(), start, end, false, false)
	if err != nil {
		return nil, err
	}

	if tree.metricsProxy != nil {
		tree.metricsProxy.IncrCounter(1, "iavl2", "iterator", "open")
	}

	kvItr.Next()

	return kvItr, nil
}

func (tree *Tree) IteratorVersionDescLeaves(version int64, limit int) (Iterator, error) {
	var err error
	kvItr := &KVIterator{
		sql:     tree.sql,
		valid:   true,
		metrics: tree.metricsProxy,
	}

	kvItr.itrStmt, kvItr.itrIdx, err = tree.sql.readPool.GetVersionDescLeafIterator(version, limit)
	if err != nil {
		return nil, err
	}

	if tree.metricsProxy != nil {
		tree.metricsProxy.IncrCounter(1, "iavl2", "iterator", "open")
	}

	kvItr.Next()

	return kvItr, err

}

func (tree *Tree) WrongBranchHashIterator(start, end int64) (Iterator, error) {
	var err error
	itr := &WrongBranchHashIterator{
		sql:     tree.sql,
		start:   start,
		end:     end,
		valid:   true,
		metrics: tree.metricsProxy,
	}

	itr.itrStmt, err = tree.sql.getHeightOneBranchesIteratorQuery(start, end)
	if err != nil {
		return nil, err
	}

	if tree.metricsProxy != nil {
		tree.metricsProxy.IncrCounter(1, "iavl2", "iterator", "open")
	}

	itr.Next()

	return itr, err
}

type WrongBranchHashIterator struct {
	sql     *SqliteDb
	itrStmt *gosqlite.Stmt
	valid   bool
	start   int64
	end     int64
	err     error
	key     []byte
	value   []byte
	metrics metrics.Proxy
}

func (i *WrongBranchHashIterator) Domain() (strat []byte, end []byte) {
	s := make([]byte, 8)
	binary.BigEndian.PutUint64(s, uint64(i.start))

	e := make([]byte, 8)
	binary.BigEndian.PutUint64(e, uint64(i.end))

	return s, e
}

func (i *WrongBranchHashIterator) Valid() bool {
	return i.valid
}

func (i *WrongBranchHashIterator) Next() {
	if i.metrics != nil {
		defer i.metrics.MeasureSince(time.Now(), "iavl2", "kv iterator", "next")
	}
	if !i.valid {
		return
	}

	for {
		hasRow, err := i.itrStmt.Step()
		if err != nil {
			closeErr := i.Close()
			if closeErr != nil {
				i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
			}
			return
		}

		if !hasRow {
			closeErr := i.Close()
			if closeErr != nil {
				i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
			}
			return
		}

		var (
			version  int64
			sequence int
			nodeBz   gosqlite.RawBytes
		)

		if err = i.itrStmt.Scan(&version, &sequence, &nodeBz); err != nil {
			closeErr := i.Close()
			if closeErr != nil {
				i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
			}
			return
		}

		nodeKey := NewNodeKey(version, uint32(sequence))
		node, err := MakeNode(i.sql.pool, nodeKey, nodeBz)
		if err != nil {
			closeErr := i.Close()
			if closeErr != nil {
				i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
			}
			return
		}

		if node.subtreeHeight != 1 {
			continue
		}

		node.leftNode, err = i.sql.getLeaf(node.leftNodeKey)
		if err != nil {
			closeErr := i.Close()
			if closeErr != nil {
				i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
			}
			return
		}

		node.rightNode, err = i.sql.getLeaf(node.rightNodeKey)
		if err != nil {
			closeErr := i.Close()
			if closeErr != nil {
				i.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
			}
			return
		}

		oldHash := slices.Clone(node.hash)
		node.hash = nil
		node._hash()

		if !bytes.Equal(node.hash, oldHash) {
			i.key = node.leftNode.key

			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(node.Version()))
			i.value = b

			break
		}
	}
}

func (i *WrongBranchHashIterator) Key() (key []byte) {
	return i.key
}

func (i *WrongBranchHashIterator) Value() (key []byte) {
	return i.value
}

func (i *WrongBranchHashIterator) Error() error {
	return i.err
}

func (i *WrongBranchHashIterator) Close() error {
	if i.valid {
		if i.metrics != nil {
			i.metrics.IncrCounter(1, "iavl2", "iterator", "close")
		}
		i.valid = false

		return i.itrStmt.Close()
	}
	return nil
}
