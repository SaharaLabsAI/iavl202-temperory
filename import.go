package iavl

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
)

// maxBatchSize is the maximum size of the import batch before flushing it to the database
const importBatchSize = 400_000

// ErrNoImport is returned when calling methods on a closed importer
var ErrNoImport = errors.New("no import in progress")

// Importer imports data into an empty MutableTree. It is created by MutableTree.Import(). Users
// must call Close() when done.
//
// ExportNodes must be imported in the order returned by Exporter, i.e. depth-first post-order (LRN).
//
// Importer is not concurrency-safe, it is the caller's responsibility to ensure the tree is not
// modified while performing an import.
type Importer struct {
	tree      *Tree
	version   int64
	batchSize uint32
	batch     *sqliteBatch

	stack         []*Node
	leafSequences []uint32
	nodeSequences []uint32

	// inflightCommit tracks a batch commit, if any.
	inflightCommit <-chan error
}

// newImporter creates a new Importer for an empty Tree
//
// version should correspond to the version that was initially exported. It must be greater than
// or equal to the highest ExportNode version number given.
func newImporter(tree *Tree, version int64) (*Importer, error) {
	if version < 0 {
		return nil, errors.New("imported version cannot be negative")
	}
	versionExists, err := tree.sql.latestRoot()
	if err != nil {
		return nil, err
	}
	if versionExists > 0 {
		return nil, fmt.Errorf("found version %v, must be empty", versionExists)
	}

	// NOTE: Must turn off heightFilter, otherwise imported tree root may not be consistent
	tree.heightFilter = 0

	return &Importer{
		tree:    tree,
		version: version,
		batch: &sqliteBatch{
			sql:    tree.sql,
			tree:   tree,
			size:   importBatchSize,
			logger: tree.sqlWriter.logger,
		},
		stack:         make([]*Node, 0, 8),
		leafSequences: make([]uint32, version+1),
		nodeSequences: make([]uint32, version+1),
	}, nil
}

var bufPool = &sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// waitBatch waits for previous batch (if any) to finish.
func (i *Importer) waitBatch() error {
	var err error
	if i.inflightCommit != nil {
		err = <-i.inflightCommit
		i.inflightCommit = nil
	}
	return err
}

// writeNode writes the node content to the storage.
func (i *Importer) writeNode(node *Node) error {
	node._hash()

	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	if err := node.WriteBytes(buf); err != nil {
		return err
	}

	bytesCopy := make([]byte, buf.Len())
	copy(bytesCopy, buf.Bytes())

	if node.isLeaf() {
		i.tree.leaves = append(i.tree.leaves, node)
	} else {
		i.tree.branches = append(i.tree.branches, node)
	}

	i.batchSize++
	if i.batchSize >= importBatchSize {
		err := i.waitBatch()
		if err != nil {
			return err
		}

		result := make(chan error)
		i.inflightCommit = result
		go func() {
			_, leafErr := i.batch.saveLeaves()
			_, branchErr := i.batch.saveBranches()
			result <- errors.Join(leafErr, branchErr)
		}()

		err = i.waitBatch()
		if err != nil {
			return err
		}

		i.tree.leaves = nil
		i.tree.branches = nil
		i.batchSize = 0
	}

	return nil
}

// Close frees all resources. It is safe to call multiple times. Uncommitted nodes may already have
// been flushed to the database, but will not be visible.
func (i *Importer) Close() {
	if i.inflightCommit != nil {
		<-i.inflightCommit
		i.inflightCommit = nil
	}
	i.batch = nil
	i.tree = nil
}

// Add adds an ExportNode to the import. ExportNodes must be added in the order returned by
// Exporter, i.e. depth-first post-order (LRN). Nodes are periodically flushed to the database,
// but the imported version is not visible until Commit() is called.
func (i *Importer) Add(node *Node) error {
	if i.tree == nil {
		return ErrNoImport
	}
	if node == nil {
		return errors.New("node cannot be nil")
	}
	nodeVersion := node.Version()
	if nodeVersion > i.version {
		return fmt.Errorf("node version %v can't be greater than import version %v",
			nodeVersion, i.version)
	}

	// We build the tree from the bottom-left up. The stack is used to store unresolved left
	// children while constructing right children. When all children are built, the parent can
	// be constructed and the resolved children can be discarded from the stack. Using a stack
	// ensures that we can handle additional unresolved left children while building a right branch.
	//
	// We don't modify the stack until we've verified the built node, to avoid leaving the
	// importer in an inconsistent state when we return an error.
	stackSize := len(i.stack)
	if node.subtreeHeight == 0 {
		node.size = 1
	} else if stackSize >= 2 && i.stack[stackSize-1].subtreeHeight < node.subtreeHeight && i.stack[stackSize-2].subtreeHeight < node.subtreeHeight {
		leftNode := i.stack[stackSize-2]
		rightNode := i.stack[stackSize-1]

		node.leftNode = leftNode
		node.rightNode = rightNode
		node.leftNodeKey = leftNode.nodeKey
		node.rightNodeKey = rightNode.nodeKey
		node.size = leftNode.size + rightNode.size

		// Update the stack now.
		if err := i.writeNode(leftNode); err != nil {
			return err
		}
		if err := i.writeNode(rightNode); err != nil {
			return err
		}
		i.stack = i.stack[:stackSize-2]

		// remove the recursive references to avoid memory leak
		leftNode.leftNode = nil
		leftNode.rightNode = nil
		rightNode.leftNode = nil
		rightNode.rightNode = nil
	}

	if node.isLeaf() {
		node.nodeKey = NewNodeKey(nodeVersion, i.nextLeafSequence(nodeVersion))
	} else {
		node.nodeKey = NewNodeKey(nodeVersion, i.nextNodeSequence(nodeVersion))
	}

	i.stack = append(i.stack, node)

	return nil
}

// Commit finalizes the import by flushing any outstanding nodes to the database, making the
// version visible, and updating the tree metadata. It can only be called once, and calls Close()
// internally.
func (i *Importer) Commit() error {
	if i.tree == nil {
		return ErrNoImport
	}

	switch len(i.stack) {
	case 0:
		// Should handle tree.root == nil special case
		if err := i.tree.sql.SaveRoot(i.version, nil); err != nil {
			return err
		}
		i.tree.root = nil
	case 1:
		n := i.stack[0]
		if n.subtreeHeight == 0 {
			n.nodeKey = NewNodeKey(n.Version(), i.nextLeafSequence(n.Version()))
		} else {
			n.nodeKey = NewNodeKey(n.Version(), i.nextNodeSequence(n.Version()))
		}
		if err := i.writeNode(n); err != nil {
			return err
		}
		if err := i.tree.sql.SaveRoot(i.version, n); err != nil {
			return err
		}
		i.tree.root = n
	default:
		return fmt.Errorf("invalid node structure, found stack size %v when committing",
			len(i.stack))
	}
	err := i.waitBatch()
	if err != nil {
		return err
	}
	_, err = i.batch.saveBranches()
	if err != nil {
		return err
	}
	_, err = i.batch.saveLeaves()
	if err != nil {
		return err
	}

	if err := i.tree.sql.treeWrite.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("failed tree checkpoint; %w", err)
	}
	err = i.tree.LoadVersion(i.version)
	if err != nil {
		return err
	}

	i.Close()
	return nil
}

func (i *Importer) nextLeafSequence(version int64) uint32 {
	if i.leafSequences[version] == 0 {
		i.leafSequences[version] = leafSequenceStart
	}

	i.leafSequences[version]++
	if i.leafSequences[version] < leafSequenceStart {
		panic("leaf sequence underflow")
	}

	return i.leafSequences[version]
}

func (i *Importer) nextNodeSequence(version int64) uint32 {
	i.nodeSequences[version]++
	return i.nodeSequences[version]
}
