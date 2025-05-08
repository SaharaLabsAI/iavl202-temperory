package iavl

import (
	"bytes"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/eatonphil/gosqlite"

	"github.com/cosmos/iavl/v2/metrics"
)

type sqliteBatch struct {
	tree    *Tree
	sql     *SqliteDb
	size    int64
	logger  Logger
	metrics metrics.Proxy

	treeCount int64
	treeSince time.Time
	leafCount int64
	leafSince time.Time

	leafInsert *gosqlite.Stmt
	treeInsert *gosqlite.Stmt
	leafOrphan *gosqlite.Stmt
	treeOrphan *gosqlite.Stmt
}

func (b *sqliteBatch) newChangeLogBatch() (err error) {
	if err = b.sql.leafWrite.Begin(); err != nil {
		return err
	}
	b.leafInsert, err = b.sql.leafWrite.Prepare("INSERT OR REPLACE INTO leaf (version, sequence, key, bytes) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	b.leafOrphan, err = b.sql.leafWrite.Prepare("INSERT INTO leaf_orphan (version, sequence, at) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	b.leafSince = time.Now()
	return nil
}

func (b *sqliteBatch) changelogMaybeCommit() (err error) {
	if b.leafCount%b.size == 0 {
		if err = b.changelogBatchCommit(); err != nil {
			return err
		}
		if err = b.newChangeLogBatch(); err != nil {
			return err
		}
	}
	return nil
}

func (b *sqliteBatch) changelogBatchCommit() error {
	if err := b.sql.leafWrite.Commit(); err != nil {
		return err
	}
	if err := b.leafInsert.Close(); err != nil {
		return err
	}
	if err := b.leafOrphan.Close(); err != nil {
		return err
	}

	return nil
}

func (b *sqliteBatch) execBranchOrphan(nodeKey NodeKey) error {
	return b.treeOrphan.Exec(nodeKey.Version(), int(nodeKey.Sequence()), b.tree.version)
}

func (b *sqliteBatch) newTreeBatch(shardID int64) (err error) {
	if err = b.sql.treeWrite.Begin(); err != nil {
		return err
	}
	b.treeInsert, err = b.sql.treeWrite.Prepare(fmt.Sprintf(
		"INSERT INTO tree_%d (version, sequence, bytes) VALUES (?, ?, ?)", shardID))
	if err != nil {
		return err
	}
	b.treeOrphan, err = b.sql.treeWrite.Prepare("INSERT INTO orphan (version, sequence, at) VALUES (?, ?, ?)")
	b.treeSince = time.Now()
	return err
}

func (b *sqliteBatch) treeBatchCommit() error {
	if err := b.sql.treeWrite.Commit(); err != nil {
		return err
	}
	if err := b.treeInsert.Close(); err != nil {
		return err
	}
	if err := b.treeOrphan.Close(); err != nil {
		return err
	}

	if b.treeCount >= b.size {
		batchSize := b.treeCount % b.size
		if batchSize == 0 {
			batchSize = b.size
		}
		b.logger.Debug(fmt.Sprintf("db=tree count=%s dur=%s batch=%d rate=%s",
			humanize.Comma(b.treeCount),
			time.Since(b.treeSince).Round(time.Millisecond),
			batchSize,
			humanize.Comma(int64(float64(batchSize)/time.Since(b.treeSince).Seconds()))))
	}
	return nil
}

func (b *sqliteBatch) treeMaybeCommit(shardID int64) (err error) {
	if b.treeCount%b.size == 0 {
		if err = b.treeBatchCommit(); err != nil {
			return err
		}
		if err = b.newTreeBatch(shardID); err != nil {
			return err
		}
	}
	return nil
}

func (b *sqliteBatch) saveLeaves() (int64, error) {
	b.leafCount = 0
	tree := b.tree

	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)

	err := b.newChangeLogBatch()
	if err != nil {
		return 0, err
	}

	for i, leaf := range tree.leaves {
		b.leafCount++

		buf.Reset()
		if err := leaf.BytesWithBuffer(buf); err != nil {
			return 0, err
		}

		if err = b.leafInsert.Exec(leaf.nodeKey.Version(), int(leaf.nodeKey.Sequence()), leaf.key, buf.Bytes()); err != nil {
			return 0, err
		}

		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}

		if tree.heightFilter > 0 {
			if i != 0 {
				// evict leaf
				tree.returnNode(leaf)
			} else if leaf.nodeKey != tree.root.nodeKey {
				// never evict the root if it's a leaf
				tree.returnNode(leaf)
			}
		}
	}

	for _, leafDelete := range tree.deletes {
		b.leafCount++
		if err = b.leafInsert.Exec(leafDelete.deleteKey.Version(), int(leafDelete.deleteKey.Sequence()), leafDelete.leafKey, nil); err != nil {
			return 0, err
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	for _, orphan := range tree.leafOrphans {
		b.leafCount++
		if err = b.leafOrphan.Exec(orphan.Version(), int(orphan.Sequence()), b.tree.version); err != nil {
			return 0, err
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	if err = b.changelogBatchCommit(); err != nil {
		return 0, err
	}

	err = tree.sql.leafWrite.Exec("CREATE UNIQUE INDEX IF NOT EXISTS leaf_idx ON leaf (version DESC, sequence)")
	if err != nil {
		return b.leafCount, err
	}

	err = tree.sql.leafWrite.Exec("CREATE UNIQUE INDEX IF NOT EXISTS leaf_key_idx ON leaf (key, version DESC);")
	if err != nil {
		return b.leafCount, err
	}

	return b.leafCount, nil
}

func (b *sqliteBatch) saveBranches() (n int64, err error) {
	tree := b.tree
	b.treeCount = 0

	shardID, err := tree.sql.nextShard(tree.version)
	if err != nil {
		return 0, err
	}
	b.logger.Debug(fmt.Sprintf("save branches db=tree version=%d shard=%d orphans=%s",
		tree.version, shardID, humanize.Comma(int64(len(tree.branchOrphans)))))

	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)

	if err = b.newTreeBatch(shardID); err != nil {
		return 0, err
	}

	for _, node := range tree.branches {
		b.treeCount++

		buf.Reset()

		if err := node.BytesWithBuffer(buf); err != nil {
			return 0, err
		}

		if err = b.treeInsert.Exec(node.nodeKey.Version(), int(node.nodeKey.Sequence()), buf.Bytes()); err != nil {
			return 0, err
		}

		if err = b.treeMaybeCommit(shardID); err != nil {
			return 0, err
		}

		if node.evict {
			tree.returnNode(node)
		}
	}

	for _, orphan := range tree.branchOrphans {
		b.treeCount++
		err = b.execBranchOrphan(orphan)
		if err != nil {
			return 0, err
		}
		if err = b.treeMaybeCommit(shardID); err != nil {
			return 0, err
		}
	}

	if err = b.treeBatchCommit(); err != nil {
		return 0, err
	}

	err = b.sql.treeWrite.Exec(fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS tree_idx_%d ON tree_%d (version, sequence);", shardID, shardID))
	if err != nil {
		return 0, err
	}

	return b.treeCount, nil
}
