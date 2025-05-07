package iavl

import (
	"bytes"
	"encoding/binary"
	"sort"
)

type WrongVersionKey struct {
	version int64
	key     []byte
}

func (tree *Tree) DetectWrongBranchHashes(start, end int64) ([]*WrongVersionKey, error) {
	iter, err := tree.WrongBranchHashIterator(start, end)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	keys := make([]*WrongVersionKey, 0)
	for ; iter.Valid(); iter.Next() {
		version := binary.BigEndian.Uint64(iter.Value())

		keys = append(keys, &WrongVersionKey{
			key:     iter.Key(),
			version: int64(version),
		})
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].version < keys[j].version
	})

	return keys, iter.Error()
}

func (tree *Tree) Compare(other *Tree) error {
	_, err := doCompare(tree, tree.root, other, other.root)
	return err
}

func doCompare(tree1 *Tree, node1 *Node, tree2 *Tree, node2 *Node) (bool, error) {
	if node1.isLeaf() {
		if !bytes.Equal(node1.hash, node2.hash) {
			return false, nil
		}
		return true, nil
	}

	if bytes.Equal(node1.hash, node2.hash) {
		return true, nil
	}

	// fmt.Printf("height %d hash %x  2 height %d %x\n", node1.subtreeHeight, node1.hash, node2.subtreeHeight, node2.hash)

	left1, err := tree1.sql.getLeftNode(node1)
	if err != nil {
		return false, err
	}
	left2, err := tree2.sql.getLeftNode(node2)
	if err != nil {
		return false, err
	}
	equal, err := doCompare(tree1, left1, tree2, left2)
	if err != nil {
		return false, err
	}
	if !equal && !left1.isLeaf() {
		return doCompare(tree1, left1, tree2, left2)
	}

	right1, err := tree1.sql.getRightNode(node1)
	if err != nil {
		return false, err
	}
	right2, err := tree2.sql.getRightNode(node2)
	if err != nil {
		return false, err
	}
	equal, err = doCompare(tree1, right1, tree2, right2)
	if err != nil {
		return false, err
	}
	if !equal && !right1.isLeaf() {
		return doCompare(tree1, right1, tree2, right2)
	}

	return true, nil
}
