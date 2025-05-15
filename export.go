package iavl

import (
	"errors"
)

// TraverseOrderType is the type of the order in which the tree is traversed.
type TraverseOrderType uint8

const (
	PreOrder TraverseOrderType = iota
	PostOrder
)

const maxStackSize = 8_000
const maxOutChanSize = 10_000

type Exporter struct {
	tree    *Tree
	out     chan *Node
	errCh   chan error
	count   int
	version int64
}

func (tree *Tree) Export(order TraverseOrderType) *Exporter {
	exporter := &Exporter{
		tree:    tree,
		out:     make(chan *Node, maxOutChanSize),
		errCh:   make(chan error),
		count:   0,
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
	tempOutCh := make(chan struct{}, 1) // Signal channel for stack flow control

	for len(stack) > 0 {
		node := stack[len(stack)-1]

		if node.isLeaf() {
			stack = stack[:len(stack)-1]

			e.out <- node
			select {
			case tempOutCh <- struct{}{}:
			default:
			}
			continue
		}

		if visited[node] {
			stack = stack[:len(stack)-1]

			e.out <- node
			select {
			case tempOutCh <- struct{}{}:
			default:
			}
			continue
		}

		if len(stack) >= maxStackSize {
			<-tempOutCh
			e.tree.sql.logger.Warn("creating snapshot", "height", e.version, "node exported", e.count)
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
	tempOutCh := make(chan struct{}, 1) // Signal channel for stack flow control

	for len(stack) > 0 {
		if len(stack) >= maxStackSize {
			<-tempOutCh
			e.tree.sql.logger.Warn("creating snapshot", "height", e.version, "node exported", e.count)
			continue
		}

		n := len(stack) - 1
		node := stack[n]
		stack = stack[:n]

		e.out <- node
		select {
		case tempOutCh <- struct{}{}:
		default:
		}

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
