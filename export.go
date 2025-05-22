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

const maxOutChanSize = 64

type stackEntry struct {
	node  *Node
	state int // 0: process left, 1: process right, 2: process self
}

type Exporter struct {
	tree    *Tree
	out     chan *Node
	errCh   chan error
	count   int
	startAt time.Time
	version int64
}

func (tree *Tree) Export(order TraverseOrderType) *Exporter {
	imTree, err := tree.GetImmutableProvable(tree.version.Load())
	if err != nil {
		panic(err)
	}

	exporter := &Exporter{
		tree:    imTree,
		out:     make(chan *Node, maxOutChanSize),
		errCh:   make(chan error),
		count:   0,
		startAt: time.Now(),
		version: imTree.version.Load(),
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

	s := []stackEntry{{node: root, state: 0}}

	for len(s) > 0 {
		currentEntry := &s[len(s)-1]

		if currentEntry.node.isLeaf() {
			e.out <- currentEntry.node
			s = s[:len(s)-1]
			continue
		}

		// For non-leaf nodes, proceed based on state
		switch currentEntry.state {
		case 0: // State 0: Attempt to process the left child
			currentEntry.state = 1 // Advance this node's state for the next time it's processed

			left, err := currentEntry.node.getLeftNode(e.tree)
			if err != nil {
				e.errCh <- err
				return
			}
			if left != nil {
				s = append(s, stackEntry{node: left, state: 0})
				// The loop will now process the new top (left child)
			}
			// If no left child, currentEntry (now in state 1) remains at the top
			// and will be processed in the next iteration.

		case 1: // State 1: Attempt to process the right child
			currentEntry.state = 2

			right, err := currentEntry.node.getRightNode(e.tree)
			if err != nil {
				e.errCh <- err
				return
			}
			if right != nil {
				s = append(s, stackEntry{node: right, state: 0})
				// The loop will now process the new top (right child)
			}
			// If no right child, currentEntry (now in state 2) remains at the top
			// and will be processed in the next iteration.

		case 2: // State 2: Process the node itself (both children have been handled)
			e.out <- currentEntry.node
			s = s[:len(s)-1]
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

		return &SnapshotNode{
			Key:     node.key,
			Value:   node.value,
			Version: node.nodeKey.Version(),
			Height:  node.subtreeHeight,
		}, nil
	case err, ok := <-e.errCh:
		if !ok {
			// Error channel closed, check if out channel still has items
			select {
			case node, ok := <-e.out:
				if ok {
					e.count++
					return &SnapshotNode{
						Key:     node.key,
						Value:   node.value,
						Version: node.nodeKey.Version(),
						Height:  node.subtreeHeight,
					}, nil
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
		out:     make(chan *Node, maxOutChanSize),
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
