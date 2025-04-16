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

type Exporter struct {
	tree  *Tree
	out   chan *Node
	errCh chan error
}

func (tree *Tree) Export(order TraverseOrderType) *Exporter {
	exporter := &Exporter{
		tree:  tree,
		out:   make(chan *Node),
		errCh: make(chan error),
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

func (e *Exporter) postOrderNext(node *Node) {
	if node.isLeaf() {
		e.out <- node
		return
	}

	left, err := node.getLeftNode(e.tree)
	if err != nil {
		e.errCh <- err
		return
	}
	e.postOrderNext(left)

	right, err := node.getRightNode(e.tree)
	if err != nil {
		e.errCh <- err
		return
	}
	e.postOrderNext(right)

	e.out <- node
}

func (e *Exporter) preOrderNext(node *Node) {
	if node.isLeaf() {
		e.out <- node
		return
	}

	left, err := node.getLeftNode(e.tree)
	if err != nil {
		e.errCh <- err
		return
	}
	e.preOrderNext(left)

	right, err := node.getRightNode(e.tree)
	if err != nil {
		e.errCh <- err
		return
	}
	e.preOrderNext(right)
}

func (e *Exporter) Next() (*SnapshotNode, error) {
	select {
	case node, ok := <-e.out:
		if !ok {
			return nil, ErrorExportDone
		}
		return &SnapshotNode{
			Key:     node.key,
			Value:   node.value,
			Version: node.nodeKey.Version(),
			Height:  node.subtreeHeight,
		}, nil
	case err, ok := <-e.errCh:
		if !ok {
			return nil, ErrorExportDone
		}
		return nil, err
	}
}

func (e *Exporter) NextRawNode() (*Node, error) {
	select {
	case node, ok := <-e.out:
		if !ok {
			return nil, ErrorExportDone
		}
		return node, nil
	case err, ok := <-e.errCh:
		if !ok {
			return nil, ErrorExportDone
		}
		return nil, err
	}
}

var ErrorExportDone = errors.New("export done")

func (e *Exporter) Close() error {
	return e.tree.Close()
}

func (tree *Tree) ExportVersion(version int64, order TraverseOrderType) (*Exporter, error) {
	got, _ := tree.getRecentRoot(version)
	if got {
		return tree.Export(order), nil
	}

	imTree, err := tree.GetImmutable(version)
	if err != nil {
		return nil, err
	}

	exporter := &Exporter{
		tree:  imTree,
		out:   make(chan *Node),
		errCh: make(chan error),
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
