package iavl

import (
	"fmt"
	"time"
)

type BatchOperationType int

const (
	Set = iota
	Remove
)

type BatchOperation struct {
	Type  BatchOperationType
	Key   []byte
	Value []byte // nil for remove operations
}

func NewSetOperation(key, value []byte) BatchOperation {
	return BatchOperation{
		Type:  Set,
		Key:   key,
		Value: value,
	}
}

func NewRemoveOperation(key []byte) BatchOperation {
	return BatchOperation{
		Type: Remove,
		Key:  key,
	}
}

func (tree *Tree) BatchSetRemove(operations []BatchOperation) error {
	if tree.immutable {
		panic("batch operations on immutable tree")
	}

	if len(operations) == 0 {
		return nil
	}

	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), metricsNamespace, "tree_batch_set_remove_deferred")
	}

	tree.rw.Lock()
	defer tree.rw.Unlock()

	batchMetrics := struct {
		updates   int64
		newNodes  int64
		deletions int64
	}{}

	for i, op := range operations {
		switch op.Type {
		case Set:
			if op.Value == nil {
				return fmt.Errorf("operation %d: attempt to store nil value at key '%s'", i, op.Key)
			}

			updated, err := tree.set(op.Key, op.Value)
			if err != nil {
				return fmt.Errorf("operation %d (set): %w", i, err)
			}

			tree.cache[string(op.Key)] = op.Value
			delete(tree.deleted, string(op.Key))

			if updated {
				batchMetrics.updates++
			} else {
				batchMetrics.newNodes++
			}

		case Remove:
			if tree.root == nil {
				continue
			}

			delete(tree.cache, string(op.Key))
			newRoot, _, _, removed, err := tree.recursiveRemove(tree.root, op.Key)
			if err != nil {
				return fmt.Errorf("operation %d (remove): %w", i, err)
			}

			if removed {
				tree.deleted[string(op.Key)] = true
				tree.root = newRoot
				batchMetrics.deletions++
			}

		default:
			return fmt.Errorf("operation %d: invalid operation type '%d'", i, op.Type)
		}
	}

	tree.modificationCount += int64(len(operations))

	if batchMetrics.updates > 0 {
		tree.metrics.IncrCounter(float32(batchMetrics.updates), metricsNamespace, "tree_update")
	}
	if batchMetrics.newNodes > 0 {
		tree.metrics.IncrCounter(float32(batchMetrics.newNodes), metricsNamespace, "tree_new_node")
	}
	if batchMetrics.deletions > 0 {
		tree.metrics.IncrCounter(float32(batchMetrics.deletions), metricsNamespace, "tree_delete")
	}

	return nil
}
