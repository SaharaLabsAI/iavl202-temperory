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

type TreeIterator struct {
	tree       *Tree
	start, end []byte // iteration domain
	ascending  bool   // ascending traversal
	inclusive  bool   // end key inclusiveness

	stack   []*Node
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

func (i *TreeIterator) push(node *Node) {
	i.stack = append(i.stack, node)
}

func (i *TreeIterator) pop() (node *Node) {
	if len(i.stack) == 0 {
		return nil
	}
	node = i.stack[len(i.stack)-1]
	i.stack = i.stack[:len(i.stack)-1]
	return
}

func (i *TreeIterator) stepAscend() {
	var n *Node
	for {
		n = i.pop()
		if n == nil {
			i.valid = false
			return
		}
		if n.isLeaf() {
			if !i.started && bytes.Compare(n.key, i.start) < 0 {
				continue
			}
			if i.isPastEndAscend(n.key) {
				i.valid = false
				return
			}
			break
		}
		right, err := n.getRightNode(i.tree)
		if err != nil {
			i.err = err
			i.valid = false
			return
		}

		if bytes.Compare(i.start, n.key) < 0 {
			left, err := n.getLeftNode(i.tree)
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
			i.push(right)
			i.push(left)
		} else {
			i.push(right)
		}

	}
	i.key = n.key
	i.value = n.value
}

func (i *TreeIterator) stepDescend() {
	var n *Node
	for {
		n = i.pop()
		if n == nil {
			i.valid = false
			return
		}
		if n.isLeaf() {
			if !i.started && i.end != nil {
				res := bytes.Compare(i.end, n.key)
				// if end is inclusive and the key is greater than end, skip
				if i.inclusive && res < 0 {
					continue
				}
				// if end is not inclusive (default) and the key is greater than or equal to end, skip
				if res <= 0 {
					continue
				}
			}
			if i.isPastEndDescend(n.key) {
				i.valid = false
				return
			}
			break
		}
		left, err := n.getLeftNode(i.tree)
		if err != nil {
			i.err = err
			i.valid = false
			return
		}

		if i.end == nil || bytes.Compare(n.key, i.end) <= 0 {
			right, err := n.getRightNode(i.tree)
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
			i.push(left)
			i.push(right)
		} else {
			i.push(left)
		}
	}
	i.key = n.key
	i.value = n.value
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
	return i.value
}

func (i *TreeIterator) Error() error {
	return i.err
}

func (i *TreeIterator) Close() error {
	i.stack = nil
	i.valid = false
	return i.err
}

func (tree *Tree) Iterator(start, end []byte, inclusive bool) (itr Iterator, err error) {
	// if tree.immutable {
	// 	return tree.IteratorAt(tree.version.Load(), start, end, inclusive)
	// }

	itr = &TreeIterator{
		tree:      tree,
		start:     start,
		end:       end,
		ascending: true,
		inclusive: inclusive,
		valid:     true,
		stack:     []*Node{tree.root},
		metrics:   tree.metricsProxy,
	}

	if tree.metricsProxy != nil {
		tree.metricsProxy.IncrCounter(1, "iavl2", "iterator", "open")
	}
	itr.Next()
	return itr, err
}

func (tree *Tree) ReverseIterator(start, end []byte) (itr Iterator, err error) {
	// if tree.immutable {
	// 	return tree.ReverseIteratorAt(tree.version.Load(), start, end)
	// }

	itr = &TreeIterator{
		tree:      tree,
		start:     start,
		end:       end,
		ascending: false,
		inclusive: false,
		valid:     true,
		stack:     []*Node{tree.root},
		metrics:   tree.metricsProxy,
	}

	if tree.metricsProxy != nil {
		tree.metricsProxy.IncrCounter(1, "iavl2", "iterator", "open")
	}
	itr.Next()
	return itr, nil
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
