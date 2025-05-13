package iavl

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/eatonphil/gosqlite"

	"github.com/cosmos/iavl/v2/metrics"
)

type SqliteReadonlyConnPool struct {
	opts *SqliteDbOptions

	mu    sync.Mutex
	conns []*SqliteReadConn

	kvItrIdx    int
	kvIterators map[int]*gosqlite.Stmt
	kvItrConns  map[int]*SqliteReadConn

	metrics metrics.Proxy
	logger  Logger
}

func NewSqliteReadonlyConnPool(opts *SqliteDbOptions, MaxPoolSize int) (*SqliteReadonlyConnPool, error) {
	if MaxPoolSize <= 0 {
		MaxPoolSize = defaultMaxPoolSize
	}

	pool := &SqliteReadonlyConnPool{
		opts:        opts,
		conns:       make([]*SqliteReadConn, 0, MaxPoolSize),
		kvIterators: make(map[int]*gosqlite.Stmt),
		kvItrConns:  make(map[int]*SqliteReadConn),
		metrics:     opts.Metrics,
		logger:      opts.Logger,
	}

	pool.logger.Info(fmt.Sprintf("Created readonly connection pool with max size %d", MaxPoolSize))

	return pool, nil
}

func (pool *SqliteReadonlyConnPool) createReadConn() (*SqliteReadConn, error) {
	connArgs := pool.opts.ConnArgs
	if !strings.Contains(pool.opts.ConnArgs, "mode=memory&cache=shared") {
		pool.opts.ConnArgs = "mode=ro"
	}

	conn, err := gosqlite.Open(pool.opts.treeConnectionString(), pool.opts.Mode)
	pool.opts.ConnArgs = connArgs

	if err != nil {
		return nil, err
	}

	// Configure connection
	err = conn.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS changelog;", pool.opts.leafConnectionString()))
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = conn.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		conn.Close()
		return nil, err
	}

	pageSize := max(os.Getpagesize(), defaultPageSize)
	err = conn.Exec(fmt.Sprintf("PRAGMA page_size=%d;", pageSize))
	if err != nil {
		return nil, err
	}

	err = conn.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 0))
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", 100))
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d;", pool.opts.BusyTimeout))
	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA threads=%d;", pool.opts.ThreadsCount))
	if err != nil {
		return nil, err
	}

	err = conn.Exec("PRAGMA read_uncommitted=ON;")
	if err != nil {
		return nil, err
	}

	err = conn.Exec("PRAGMA query_only=ON;")
	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA sqlite_stmt_cache=%d;", pool.opts.StatementCache))
	if err != nil {
		return nil, err
	}

	readConn := NewSqliteReadConn(conn, pool.opts, pool.logger)

	return readConn, nil
}

func (pool *SqliteReadonlyConnPool) GetConn() (*SqliteReadConn, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	for _, conn := range pool.conns {
		if !conn.inUse {
			conn.inUse = true
			return conn, nil
		}
	}
	if len(pool.conns) > pool.opts.MaxPoolSize {
		return nil, fmt.Errorf("service busy, no resource available")
	}

	conn, err := pool.createReadConn()
	if err != nil {
		return nil, fmt.Errorf("failed to create new connection: %w", err)
	}

	conn.inUse = true
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
	pool.mu.Lock()
	defer pool.mu.Unlock()

	for _, conn := range pool.conns {
		conn.SetPendingResetShard()
	}
}

func (pool *SqliteReadonlyConnPool) CloseKVIterstor(idx int) error {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	var err error
	stmt, exists := pool.kvIterators[idx]
	if exists {
		err = stmt.Close()
		delete(pool.kvIterators, idx)
	}

	conn, exists := pool.kvItrConns[idx]
	if exists {
		pool.ReleaseConn(conn)
		delete(pool.kvItrConns, idx)
	}

	return err
}

func (pool *SqliteReadonlyConnPool) CloseHangingIterators() error {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	for idx, stmt := range pool.kvIterators {
		pool.logger.Warn(fmt.Sprintf("closing hanging iterator idx=%d", idx))
		if err := stmt.Close(); err != nil {
			return err
		}

		if pool.kvItrConns[idx] != nil {
			pool.ReleaseConn(pool.kvItrConns[idx])
			delete(pool.kvItrConns, idx)
		}

		delete(pool.kvIterators, idx)
	}

	pool.kvItrIdx = 0

	return nil
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

	pool.mu.Lock()
	defer pool.mu.Unlock()

	pool.kvItrIdx++
	idx = pool.kvItrIdx

	endKey := "key < ?"
	if inclusive {
		endKey = "key <= ?"
	}

	switch {
	case start == nil && end == nil:
		stmt, err = conn.Prepare(
			fmt.Sprintf(`SELECT key, bytes FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY version DESC) AS rn FROM changelog.leaf WHERE bytes IS NOT NULL AND version <= ? ORDER BY key %s) WHERE rn = 1;`, suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(version); err != nil {
			return nil, idx, err
		}
	case start == nil:
		stmt, err = conn.Prepare(
			fmt.Sprintf(`SELECT key, bytes FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY version DESC) AS rn FROM changelog.leaf WHERE bytes IS NOT NULL AND version <= ? AND %s ORDER BY key %s) WHERE rn = 1;`, endKey, suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(version, end); err != nil {
			return nil, idx, err
		}
	case end == nil:
		stmt, err = conn.Prepare(
			fmt.Sprintf(`SELECT key, bytes FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY version DESC) AS rn FROM changelog.leaf WHERE bytes IS NOT NULL AND version <= ? AND key >= ? ORDER BY key %s) WHERE rn = 1;`, suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(version, start); err != nil {
			return nil, idx, err
		}
	default:
		stmt, err = conn.Prepare(
			fmt.Sprintf(`SELECT key, bytes FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY version DESC) AS rn FROM changelog.leaf WHERE bytes IS NOT NULL AND version <= ? AND key >= ? AND %s ORDER BY key %s) WHERE rn = 1;`, endKey, suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(version, start, end); err != nil {
			return nil, idx, err
		}
	}

	pool.kvIterators[idx] = stmt
	pool.kvItrConns[idx] = conn

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
