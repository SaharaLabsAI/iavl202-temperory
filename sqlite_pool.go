package iavl

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eatonphil/gosqlite"

	"github.com/sahara/iavl/metrics"
)

type SqliteReadonlyConnPool struct {
	opts *SqliteDbOptions

	treeVersion *atomic.Int64
	savingTree  atomic.Bool

	conns *ConnPool
	iters *IterPool

	metrics metrics.Proxy
	logger  Logger

	mu sync.RWMutex
}

func NewSqliteReadonlyConnPool(opts *SqliteDbOptions, MaxPoolSize int) (*SqliteReadonlyConnPool, error) {
	if MaxPoolSize <= 0 {
		MaxPoolSize = defaultMaxPoolSize
	}

	pool := &SqliteReadonlyConnPool{
		opts:    opts,
		conns:   NewConnPool(opts, MaxPoolSize, opts.Logger),
		iters:   NewIterPool(opts.Logger),
		metrics: opts.Metrics,
		logger:  opts.Logger,
	}

	// pool.logger.Info(fmt.Sprintf("Created readonly connection pool with max size %d", MaxPoolSize))

	return pool, nil
}

func (pool *SqliteReadonlyConnPool) LinkTreeVersion(version *atomic.Int64) {
	pool.treeVersion = version
}

func (pool *SqliteReadonlyConnPool) SetSavingTree() {
	pool.savingTree.Store(true)
	pool.mu.Lock()
}

func (pool *SqliteReadonlyConnPool) UnsetSavingTree() {
	pool.savingTree.Store(false)
	pool.mu.Unlock()
}

func (pool *SqliteReadonlyConnPool) GetConn() (*SqliteReadConn, error) {
	pool.mu.RLock()

	if pool.savingTree.Load() {
		pool.mu.RUnlock()

		ctx := context.Background()
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
				// Check if checkpoint finished
				pool.mu.RLock()
				if pool.savingTree.Load() {
					pool.mu.RUnlock()
					continue
				}

				// Checkpoint done, proceed with connection
				defer pool.mu.RUnlock()
				// Continue with existing GetConn logic
				return pool.conns.getConn(pool.treeVersion.Load())
			}
		}
	}

	defer pool.mu.RUnlock()
	return pool.conns.getConn(pool.treeVersion.Load())
}

// Close closes all connections in the pool
func (pool *SqliteReadonlyConnPool) Close() error {
	if err := pool.iters.closeHangingIterators(); err != nil {
		return err
	}

	return pool.conns.close()
}

func (pool *SqliteReadonlyConnPool) ResetShardQueries() {
	// disable now because we don't enable sharding

	// pool.mu.Lock()
	// defer pool.mu.Unlock()
	//
	// for _, conn := range pool.conns {
	// 	conn.SetPendingResetShard()
	// }
}

func (pool *SqliteReadonlyConnPool) CloseKVIterstor(idx int) error {
	return pool.iters.closeKVIterstor(idx)
}

func (pool *SqliteReadonlyConnPool) CloseHangingIterators() error {
	return pool.iters.closeHangingIterators()
}

func (pool *SqliteReadonlyConnPool) GetKVIteratorQuery(version int64, start, end []byte, ascending, inclusive bool) (stmt *gosqlite.Stmt, idx int, err error) {
	conn, err := pool.GetConn()
	if err != nil {
		return nil, 0, err
	}

	var suffix string
	if ascending {
		suffix = "ASC"
	} else {
		suffix = "DESC"
	}

	endKey := "key < ?"
	if inclusive {
		endKey = "key <= ?"
	}

	idx = pool.iters.nextIdx()

	switch {
	case start == nil && end == nil:
		stmt, err = conn.Prepare(
			fmt.Sprintf(`SELECT l.key, l.bytes
FROM changelog.leaf l
INNER JOIN (
    SELECT key, MAX(version) as max_version
    FROM changelog.leaf
    WHERE bytes IS NOT NULL AND version <= ?
    GROUP BY key
) m ON l.key = m.key AND l.version = m.max_version
ORDER BY l.key %s;`, suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(version); err != nil {
			return nil, idx, err
		}
	case start == nil:
		stmt, err = conn.Prepare(
			fmt.Sprintf(`SELECT l.key, l.bytes
FROM changelog.leaf l
INNER JOIN (
    SELECT key, MAX(version) as max_version
    FROM changelog.leaf
    WHERE bytes IS NOT NULL AND version <= ? AND %s
    GROUP BY key
) m ON l.key = m.key AND l.version = m.max_version
ORDER BY l.key %s;`, endKey, suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(version, end); err != nil {
			return nil, idx, err
		}
	case end == nil:
		stmt, err = conn.Prepare(
			fmt.Sprintf(`SELECT l.key, l.bytes
FROM changelog.leaf l
INNER JOIN (
    SELECT key, MAX(version) as max_version
    FROM changelog.leaf
    WHERE bytes IS NOT NULL AND version <= ? AND key >= ?
    GROUP BY key
) m ON l.key = m.key AND l.version = m.max_version
ORDER BY l.key %s;`, suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(version, start); err != nil {
			return nil, idx, err
		}
	default:
		stmt, err = conn.Prepare(
			fmt.Sprintf(`SELECT l.key, l.bytes
FROM changelog.leaf l
INNER JOIN (
    SELECT key, MAX(version) as max_version
    FROM changelog.leaf
    WHERE bytes IS NOT NULL AND version <= ? AND key >= ? AND %s
    GROUP BY key
) m ON l.key = m.key AND l.version = m.max_version
ORDER BY l.key %s;`, endKey, suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(version, start, end); err != nil {
			return nil, idx, err
		}
	}

	pool.iters.setIterator(idx, stmt, conn)

	return stmt, idx, nil
}

// GetHeightOneBranchesIterator prepares and returns a statement for height one branches iterator queries
func (pool *SqliteReadonlyConnPool) GetHeightOneBranchesIterator(conn *SqliteReadConn, start, end int64) (*gosqlite.Stmt, error) {
	stmt, err := conn.conn.Prepare(
		fmt.Sprintf("SELECT version, sequence, bytes FROM tree_%d WHERE version >= ? AND version <= ? ORDER BY version ASC", defaultShardID))
	if err != nil {
		return nil, err
	}
	if err = stmt.Bind(start, end); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (pool *SqliteReadonlyConnPool) GetVersionDescLeafIterator(version int64, limit int) (stmt *gosqlite.Stmt, idx int, err error) {
	conn, err := pool.GetConn()
	if err != nil {
		return nil, 0, err
	}

	idx = pool.iters.nextIdx()

	stmt, err = conn.Prepare(`
		SELECT l.key, l.bytes, l.version
		FROM changelog.leaf l
		INNER JOIN (
			SELECT key, MAX(version) as max_version
			FROM changelog.leaf
			WHERE bytes IS NOT NULL AND version <= ?
			GROUP BY key
		) m ON l.key = m.key AND l.version = m.max_version
		LIMIT ?;
	`)
	if err != nil {
		return nil, idx, err
	}

	if err = stmt.Bind(version, limit); err != nil {
		return nil, idx, err
	}

	pool.iters.setIterator(idx, stmt, conn)

	return stmt, idx, nil
}

type ConnPool struct {
	opts *SqliteDbOptions

	conns []*SqliteReadConn

	logger Logger

	mu sync.Mutex
}

func NewConnPool(opts *SqliteDbOptions, MaxPoolSize int, logger Logger) *ConnPool {
	return &ConnPool{
		opts:   opts,
		conns:  make([]*SqliteReadConn, 0, MaxPoolSize),
		logger: logger,
	}
}

func (c *ConnPool) getConn(version int64) (*SqliteReadConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, conn := range c.conns {
		if !conn.IsInUse() {
			conn.inUse = true
			conn.ResetToTreeVersion(version)
			return conn, nil
		}
	}

	if len(c.conns) > c.opts.MaxPoolSize {
		return nil, fmt.Errorf("service busy, try again later")
	}

	conn := NewSqliteImmutableReadConn(version, c.opts, c.logger)
	conn.inUse = true
	conn.ResetToTreeVersion(conn.treeVersion + 1) // Force reset on first connect
	c.conns = append(c.conns, conn)

	c.logger.Debug(fmt.Sprintf("Created new connection, pool size now: %d", len(c.conns)))

	return conn, nil
}

func (c *ConnPool) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var lastErr error
	for _, conn := range c.conns {
		lastErr = conn.conn.Close()
	}

	c.conns = nil

	return lastErr
}

type IterPool struct {
	kvItrIdx    int
	kvIterators map[int]*gosqlite.Stmt
	kvItrConns  map[int]*SqliteReadConn

	logger Logger

	mu sync.Mutex
}

func NewIterPool(logger Logger) *IterPool {
	return &IterPool{
		kvIterators: make(map[int]*gosqlite.Stmt),
		kvItrConns:  make(map[int]*SqliteReadConn),
		logger:      logger,
	}
}

func (i *IterPool) nextIdx() int {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.kvItrIdx++

	return i.kvItrIdx
}

func (i *IterPool) setIterator(idx int, stmt *gosqlite.Stmt, conn *SqliteReadConn) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.kvIterators[idx] = stmt
	i.kvItrConns[idx] = conn
}

func (i *IterPool) closeKVIterstor(idx int) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	var err error
	stmt, exists := i.kvIterators[idx]
	if exists {
		err = stmt.Close()
		delete(i.kvIterators, idx)
	}

	conn, exists := i.kvItrConns[idx]
	if exists {
		conn.MarkIdle()
		delete(i.kvItrConns, idx)
	}

	return err
}

func (i *IterPool) closeHangingIterators() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	for idx, stmt := range i.kvIterators {
		i.logger.Info(fmt.Sprintf("closing hanging iterator idx=%d", idx))

		if err := stmt.Close(); err != nil {
			return err
		}

		if i.kvItrConns[idx] != nil {
			i.kvItrConns[idx].MarkIdle()
			delete(i.kvItrConns, idx)
		}

		delete(i.kvIterators, idx)
	}

	i.kvItrIdx = 0

	return nil
}
