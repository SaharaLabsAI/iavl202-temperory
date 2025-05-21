package iavl

import (
	"errors"
	"fmt"
	"time"
)

// TraverseOrderType is the type of the order in which the tree is traversed.
type TraverseOrderType uint8

const (
	PreOrder TraverseOrderType = iota
	PostOrder
)

const maxOutChanSize = 1024

type Exporter struct {
	tree    *Tree
	out     chan *Node
	errCh   chan error
	count   int
	startAt time.Time
	version int64
}

func (tree *Tree) Export(order TraverseOrderType) *Exporter {
	exporter := &Exporter{
		tree:    tree,
		out:     make(chan *Node, maxOutChanSize),
		errCh:   make(chan error),
		count:   0,
		startAt: time.Now(),
		version: tree.version.Load(),
	}

	go func(traverseOrder TraverseOrderType) {
		defer close(exporter.out)
		defer close(exporter.errCh)

		switch traverseOrder {
		case PostOrder:
			exporter.postOrderNext(tree.root)
		case PreOrder:
			exporter.preOrderNext(tree.root)
		}
	}(order)

	return exporter
}

func (e *Exporter) postOrderNext(root *Node) {
	if root == nil {
		return
	}

	stack := []*Node{root}
	visited := make(map[*Node]bool)

	for len(stack) > 0 {
		node := stack[len(stack)-1]

		if node.isLeaf() {
			stack = stack[:len(stack)-1]

			e.out <- node
			continue
		}

		if visited[node] {
			stack = stack[:len(stack)-1]

			e.out <- node
			continue
		}

		visited[node] = true

		right, err := node.getRightNode(e.tree)
		if err != nil {
			e.errCh <- err
			return
		}

		if right != nil {
			stack = append(stack, right)
		}

		left, err := node.getLeftNode(e.tree)
		if err != nil {
			e.errCh <- err
			return
		}

		if left != nil {
			stack = append(stack, left)
		}
	}
}

func (e *Exporter) preOrderNext(root *Node) {
	if root == nil {
		return
	}

	stack := []*Node{root}

	for len(stack) > 0 {
		n := len(stack) - 1
		node := stack[n]
		stack = stack[:n]

		e.out <- node

		if !node.isLeaf() {
			right, err := node.getRightNode(e.tree)
			if err != nil {
				e.errCh <- err
				return
			}
			if right != nil {
				stack = append(stack, right)
			}

			left, err := node.getLeftNode(e.tree)
			if err != nil {
				e.errCh <- err
				return
			}
			if left != nil {
				stack = append(stack, left)
			}
		}
	}
}

func (e *Exporter) Next() (*SnapshotNode, error) {
	select {
	case node, ok := <-e.out:
		if !ok {
			// Channel is closed, check for errors
			select {
			case err, ok := <-e.errCh:
				if ok {
					return nil, err
				}
			default:
			}
			return nil, ErrorExportDone
		}
		e.count++
		if e.count%200000 == 0 {
			e.tree.sql.logger.Info("%s exported nodes %d duration %d\n", e.tree.Path(), e.count, time.Since(e.startAt).Milliseconds())
			fmt.Printf("progress exported nodes %d duration %d\n", e.count, time.Since(e.startAt).Milliseconds())
		}

		return e.makeSnapshotNode(node), nil
	case err, ok := <-e.errCh:
		if !ok {
			// Error channel closed, check if out channel still has items
			select {
			case node, ok := <-e.out:
				if ok {
					e.count++
					return e.makeSnapshotNode(node), nil
				}
			default:
			}
			return nil, ErrorExportDone
		}
		return nil, err
	}
}

// Primary used for unit tests
func (e *Exporter) NextRawNode() (*Node, error) {
	select {
	case node, ok := <-e.out:
		if !ok {
			// Channel is closed, check for errors
			select {
			case err, ok := <-e.errCh:
				if ok {
					return nil, err
				}
			default:
			}
			return nil, ErrorExportDone
		}
		e.count++
		return node, nil
	case err, ok := <-e.errCh:
		if !ok {
			// Error channel closed, check if out channel still has items
			select {
			case node, ok := <-e.out:
				if ok {
					e.count++
					return node, nil
				}
			default:
			}
			return nil, ErrorExportDone
		}
		return nil, err
	}
}

var ErrorExportDone = errors.New("export done")

func (e *Exporter) Close() error {
	return e.tree.DiscardImmutableTree()
}

func (e *Exporter) makeSnapshotNode(node *Node) *SnapshotNode {
	key := make([]byte, len(node.key[:]))
	copy(key, node.key[:])

	value := make([]byte, len(node.value[:]))
	copy(value, node.value[:])

	version := node.nodeKey.Version()
	height := node.subtreeHeight

	// For branches nodes, depends on gc to reclaim memory
	if node.isLeaf() {
		e.tree.sql.pool.Put(node)
	}

	return &SnapshotNode{
		Key:     key,
		Value:   value,
		Version: version,
		Height:  height,
	}
}

func (tree *Tree) ExportVersion(version int64, order TraverseOrderType) (*Exporter, error) {
	got, _ := tree.getRecentRoot(version)
	if got {
		return tree.Export(order), nil
	}

	imTree, err := tree.GetImmutableProvable(version)
	if err != nil {
		return nil, err
	}

	exporter := &Exporter{
		tree:    imTree,
		out:     make(chan *Node),
		errCh:   make(chan error),
		count:   0,
		version: version,
	}

	if imTree.root == nil {
		close(exporter.out)
		close(exporter.errCh)
		return exporter, nil
	}

	go func(traverseOrder TraverseOrderType) {
		defer close(exporter.out)
		defer close(exporter.errCh)

		switch traverseOrder {
		case PostOrder:
			exporter.postOrderNext(tree.root)
		case PreOrder:
			exporter.preOrderNext(tree.root)
		}
	}(order)

	return exporter, nil
}
