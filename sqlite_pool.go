package iavl

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/eatonphil/gosqlite"

	"github.com/cosmos/iavl/v2/metrics"
)

type SqliteReadonlyConnPool struct {
	opts *SqliteDbOptions

	treeVersion *atomic.Int64

	mu    sync.Mutex
	conns []*SqliteReadConn

	iters *IterPool

	metrics metrics.Proxy
	logger  Logger
}

func NewSqliteReadonlyConnPool(opts *SqliteDbOptions, MaxPoolSize int) (*SqliteReadonlyConnPool, error) {
	if MaxPoolSize <= 0 {
		MaxPoolSize = defaultMaxPoolSize
	}

	pool := &SqliteReadonlyConnPool{
		opts:    opts,
		conns:   make([]*SqliteReadConn, 0, MaxPoolSize),
		iters:   NewIterPool(opts.Logger),
		metrics: opts.Metrics,
		logger:  opts.Logger,
	}

	pool.logger.Info(fmt.Sprintf("Created readonly connection pool with max size %d", MaxPoolSize))

	return pool, nil
}

func (pool *SqliteReadonlyConnPool) LinkTreeVersion(version *atomic.Int64) {
	pool.treeVersion = version
}

func (pool *SqliteReadonlyConnPool) GetConn() (*SqliteReadConn, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	for _, conn := range pool.conns {
		if !conn.inUse {
			conn.inUse = true
			conn.ResetToTreeVersion(pool.treeVersion.Load())
			return conn, nil
		}
	}

	if len(pool.conns) > pool.opts.MaxPoolSize {
		return nil, fmt.Errorf("service busy, try again later")
	}

	conn := NewSqliteImmutableReadConn(pool.treeVersion.Load(), pool.opts, pool.logger)
	conn.inUse = true
	conn.ResetToTreeVersion(conn.treeVersion + 1) // Force reset on first connect
	pool.conns = append(pool.conns, conn)

	pool.logger.Debug(fmt.Sprintf("Created new connection, pool size now: %d", len(pool.conns)))

	return conn, nil
}

// ReleaseConn returns a connection to the pool
func (pool *SqliteReadonlyConnPool) ReleaseConn(conn *SqliteReadConn) {
	conn.MarkIdle()
}

// Close closes all connections in the pool
func (pool *SqliteReadonlyConnPool) Close() error {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	var lastErr error
	for _, conn := range pool.conns {
		lastErr = conn.conn.Close()
	}

	// Clear the pool
	pool.conns = nil

	return lastErr
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
