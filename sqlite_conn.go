package iavl

import (
	"fmt"
	"sync"

	"github.com/eatonphil/gosqlite"
)

type SqliteReadConn struct {
	conn *gosqlite.Conn

	queryLeaf *gosqlite.Stmt
	queryKV   *gosqlite.Stmt

	shards            *VersionRange
	shardQueries      map[int64]*gosqlite.Stmt
	pendingResetShard bool

	opts *SqliteDbOptions

	inUse  bool
	logger Logger

	mu sync.Mutex
}

func NewSqliteReadConn(conn *gosqlite.Conn, opts *SqliteDbOptions, logger Logger) *SqliteReadConn {
	return &SqliteReadConn{
		conn:              conn,
		shards:            &VersionRange{},
		shardQueries:      make(map[int64]*gosqlite.Stmt),
		opts:              opts,
		inUse:             false,
		pendingResetShard: false,
		logger:            logger,
	}
}

func (c *SqliteReadConn) Prepare(statement string, args ...interface{}) (*gosqlite.Stmt, error) {
	return c.conn.Prepare(statement, args...)
}

func (c *SqliteReadConn) getVersioned(version int64, key []byte) ([]byte, error) {
	defer c.MarkIdle()

	if len(key) == 0 {
		return nil, fmt.Errorf("get value with key length 0")
	}

	var err error
	if c.queryKV == nil {
		c.queryKV, err = c.conn.Prepare("SELECT bytes FROM changelog.leaf WHERE key = ? AND version <= ? ORDER BY version DESC LIMIT 1")
		if err != nil {
			return nil, err
		}
	}
	defer c.queryKV.Reset()

	if err = c.queryKV.Bind(key, version); err != nil {
		return nil, err
	}

	hasRow, err := c.queryKV.Step()
	if err != nil {
		return nil, err
	}
	if !hasRow {
		return nil, nil
	}

	var nodeBz gosqlite.RawBytes
	err = c.queryKV.Scan(&nodeBz)
	if err != nil {
		return nil, err
	}

	if nodeBz == nil {
		return nil, nil
	}

	return extractValue(nodeBz)
}

func (c *SqliteReadConn) getLeaf(pool *NodePool, nodeKey NodeKey) (*Node, error) {
	defer c.MarkIdle()

	var err error
	if c.queryLeaf == nil {
		c.queryLeaf, err = c.conn.Prepare("SELECT bytes FROM changelog.leaf WHERE version = ? AND sequence = ? LIMIT 1")
		if err != nil {
			return nil, err
		}
	}
	defer c.queryLeaf.Reset()

	if err = c.queryLeaf.Bind(nodeKey.Version(), int(nodeKey.Sequence())); err != nil {
		return nil, err
	}

	hasRow, err := c.queryLeaf.Step()
	if !hasRow {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var nodeBz gosqlite.RawBytes
	err = c.queryLeaf.Scan(&nodeBz)
	if err != nil {
		return nil, err
	}

	node, err := MakeNode(pool, nodeKey, nodeBz)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (c *SqliteReadConn) getNode(pool *NodePool, nodeKey NodeKey) (*Node, error) {
	defer c.MarkIdle()

	c.mu.Lock()
	shouldResetShard := c.pendingResetShard
	c.mu.Unlock()

	if shouldResetShard {
		err := c.ResetShardQueries()
		if err != nil {
			return nil, err
		}
	}

	q, err := c.getShardQuery(nodeKey.Version())
	if err != nil {
		return nil, err
	}
	defer q.Reset()

	if err := q.Bind(nodeKey.Version(), int(nodeKey.Sequence())); err != nil {
		return nil, err
	}

	hasRow, err := q.Step()
	if !hasRow {
		return nil, fmt.Errorf("node not found: %v; shard=%d; path=%s",
			nodeKey, c.shards.Find(nodeKey.Version()), c.opts.Path)
	}
	if err != nil {
		return nil, err
	}

	var nodeBz gosqlite.RawBytes
	err = q.Scan(&nodeBz)
	if err != nil {
		return nil, err
	}

	node, err := MakeNode(pool, nodeKey, nodeBz)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (c *SqliteReadConn) getShard(_ int64) (int64, error) {
	// Disable shard and always return 1
	return defaultShardID, nil
}

func (c *SqliteReadConn) getShardQuery(version int64) (*gosqlite.Stmt, error) {
	v, err := c.getShard(version)
	if err != nil {
		return nil, err
	}

	if q, ok := c.shardQueries[v]; ok {
		return q, nil
	}

	sqlQuery := fmt.Sprintf("SELECT bytes FROM tree_%d WHERE version = ? AND sequence = ? LIMIT 1", v)
	q, err := c.conn.Prepare(sqlQuery)
	if err != nil {
		return nil, err
	}

	c.shardQueries[v] = q
	c.logger.Debug(fmt.Sprintf("added shard query: %s", sqlQuery))

	return q, nil
}

func (c *SqliteReadConn) ResetShardQueries() error {
	// disable now because we don't enable sharding
	return nil

	// c.mu.Lock()
	// c.pendingResetShard = false
	// c.mu.Unlock()
	//
	// treeWrite, err := gosqlite.Open(c.opts.treeConnectionString(), c.opts.Mode)
	// if err != nil {
	// 	return err
	// }
	// defer treeWrite.Close()
	//
	// for k, q := range c.shardQueries {
	// 	err := q.Close()
	// 	if err != nil {
	// 		return err
	// 	}
	// 	delete(c.shardQueries, k)
	// }
	//
	// c.shards = &VersionRange{}
	//
	// q, err := treeWrite.Prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%'")
	// if err != nil {
	// 	return err
	// }
	// defer q.Close()
	//
	// for {
	// 	hasRow, err := q.Step()
	// 	if err != nil {
	// 		return err
	// 	}
	//
	// 	if !hasRow {
	// 		break
	// 	}
	//
	// 	var shard string
	// 	err = q.Scan(&shard)
	// 	if err != nil {
	// 		return err
	// 	}
	//
	// 	shardVersion, err := strconv.Atoi(shard[5:])
	// 	if err != nil {
	// 		return err
	// 	}
	//
	// 	if err = c.shards.Add(int64(shardVersion)); err != nil {
	// 		return fmt.Errorf("failed to add shard path=%s: %w", c.opts.Path, err)
	// 	}
	// }
	//
	// return nil
}

func (c *SqliteReadConn) SetPendingResetShard() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pendingResetShard = true
}

func (c *SqliteReadConn) MarkIdle() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.inUse = false
}

func (c *SqliteReadConn) Close() error {
	if c.queryLeaf != nil {
		if err := c.queryLeaf.Close(); err != nil {
			return err
		}
	}
	if c.queryKV != nil {
		if err := c.queryKV.Close(); err != nil {
			return err
		}
	}

	for _, q := range c.shardQueries {
		err := q.Close()
		if err != nil {
			return err
		}
	}

	return c.conn.Close()
}
