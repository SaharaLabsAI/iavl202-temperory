package iavl

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/eatonphil/gosqlite"
	api "github.com/kocubinski/costor-api"
	"golang.org/x/sync/errgroup"

	"github.com/cosmos/iavl/v2/metrics"
)

const defaultSQLitePath = "/tmp/iavl2"
const defaultShardID = 1
const defaultMaxPoolSize = 200
const defaultPageSize = 4096 * 8 // 32K
const defaultThreadsCount = 8
const defaultAnalysisLimit = 2000
const PageSize8K = 8192

var (
	Force8KPageSize   = "0"
	isForce8KPageSize = false
)

func init() {
	if Force8KPageSize == "1" {
		isForce8KPageSize = true
	}
}

type SqliteDbOptions struct {
	Path          string
	Mode          int
	MmapSize      uint64
	WalSize       int
	CacheSize     int
	ConnArgs      string
	TempStoreSize int
	ShardTrees    bool
	MaxPoolSize   int

	BusyTimeout    int
	ThreadsCount   int
	StatementCache int

	Logger  Logger
	Metrics metrics.Proxy

	walPages int

	OptimizeOnStart bool
}

type SqliteDb struct {
	opts SqliteDbOptions

	pool *NodePool

	// 2 separate databases and 2 separate connections.  the underlying databases have different WAL policies
	// therefore separation is required.
	leafWrite *gosqlite.Conn
	treeWrite *gosqlite.Conn

	// Used by block producer or syncer
	read *SqliteReadConn

	// Separate read conn configuration from main read, typical used by rpc query
	readPool *SqliteReadonlyConnPool

	metrics metrics.Proxy
	logger  Logger

	useReadPool bool
}

func defaultSqliteDbOptions(opts SqliteDbOptions) SqliteDbOptions {
	if opts.Path == "" {
		opts.Path = defaultSQLitePath
	}
	if opts.Mode == 0 {
		opts.Mode = gosqlite.OPEN_READWRITE | gosqlite.OPEN_CREATE | gosqlite.OPEN_NOMUTEX
	}
	// Disable mmap size completely
	if opts.MmapSize == 0 {
		// 256M
		// opts.MmapSize = 256 * 1024 * 1024
	}
	if opts.WalSize == 0 {
		opts.WalSize = 1024 * 1024 * 100
	}
	if opts.CacheSize == 0 {
		// 512M
		opts.CacheSize = -512 * 1024
	}
	if opts.TempStoreSize == 0 {
		// 200M
		opts.TempStoreSize = 200 * 1024 * 1024
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.NilMetrics{}
	}
	opts.walPages = opts.WalSize / os.Getpagesize()

	opts.ShardTrees = false

	if opts.MaxPoolSize == 0 {
		opts.MaxPoolSize = defaultMaxPoolSize
	}

	if opts.BusyTimeout == 0 {
		opts.BusyTimeout = 15000
	}

	if opts.ThreadsCount == 0 {
		opts.ThreadsCount = defaultThreadsCount
	}

	if opts.StatementCache == 0 {
		opts.StatementCache = 100
	}

	if opts.Logger == nil {
		opts.Logger = NewNopLogger()
	}

	return opts
}

func (opts SqliteDbOptions) connArgs() string {
	if opts.ConnArgs == "" {
		return ""
	}
	return fmt.Sprintf("?%s", opts.ConnArgs)
}

func (opts SqliteDbOptions) leafConnectionString() string {
	return fmt.Sprintf("file:%s/changelog.sqlite%s", opts.Path, opts.connArgs())
}

func (opts SqliteDbOptions) treeConnectionString() string {
	return fmt.Sprintf("file:%s/tree.sqlite%s", opts.Path, opts.connArgs())
}

func (opts SqliteDbOptions) kvConnectionString() string {
	return fmt.Sprintf("file:%s/kv.sqlite%s", opts.Path, opts.connArgs())
}

func (opts SqliteDbOptions) EstimateMmapSize() (uint64, error) {
	opts.Logger.Info("calculate mmap size")
	opts.Logger.Info(fmt.Sprintf("leaf connection string: %s", opts.leafConnectionString()))
	conn, err := gosqlite.Open(opts.leafConnectionString(), opts.Mode)
	if err != nil {
		return 0, err
	}
	q, err := conn.Prepare("SELECT SUM(pgsize) FROM dbstat WHERE name = 'leaf'")
	if err != nil {
		return 0, err
	}
	hasRow, err := q.Step()
	if err != nil {
		return 0, err
	}
	if !hasRow {
		return 0, errors.New("no row")
	}
	var leafSize int64
	err = q.Scan(&leafSize)
	if err != nil {
		return 0, err
	}
	if err = q.Close(); err != nil {
		return 0, err
	}
	if err = conn.Close(); err != nil {
		return 0, err
	}
	mmapSize := uint64(float64(leafSize) * 1.3)
	opts.Logger.Info(fmt.Sprintf("leaf mmap size: %s", humanize.Bytes(mmapSize)))

	return mmapSize, nil
}

func NewInMemorySqliteDb(pool *NodePool) (*SqliteDb, error) {
	opts := defaultSqliteDbOptions(SqliteDbOptions{ConnArgs: "mode=memory&cache=shared"})
	return NewSqliteDb(pool, opts)
}

func NewSqliteDb(pool *NodePool, opts SqliteDbOptions) (*SqliteDb, error) {
	var err error
	opts = defaultSqliteDbOptions(opts)

	sql := &SqliteDb{
		opts:    opts,
		pool:    pool,
		metrics: opts.Metrics,
		logger:  opts.Logger,
	}

	if !api.IsFileExistent(opts.Path) {
		err = os.MkdirAll(opts.Path, 0755)
		if err != nil {
			return nil, err
		}
	}

	if err = sql.resetWriteConn(); err != nil {
		return nil, err
	}

	if err = sql.init(); err != nil {
		return nil, err
	}

	if sql.opts.OptimizeOnStart {
		if err = sql.runAnalyze(); err != nil {
			return nil, err
		}

		if err = sql.runOptimize(); err != nil {
			return nil, err
		}
	}

	sql.readPool, err = NewSqliteReadonlyConnPool(&opts, opts.MaxPoolSize)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize read connection pool: %w", err)
	}

	return sql, nil
}

func (sql *SqliteDb) init() error {
	q, err := sql.treeWrite.Prepare("SELECT name from sqlite_master WHERE type='table' AND name='root'")
	if err != nil {
		return err
	}
	hasRow, err := q.Step()
	if err != nil {
		return err
	}
	if !hasRow {
		err = sql.treeWrite.Exec(`
CREATE TABLE orphan (version int, sequence int, at int);
CREATE INDEX orphan_idx ON orphan (at DESC);
CREATE TABLE root (
	version int, 
	node_version int, 
	node_sequence int, 
	bytes blob, 
	PRIMARY KEY (version))`)
		if err != nil {
			return err
		}

		pageSize := max(os.Getpagesize(), defaultPageSize)
		if isForce8KPageSize {
			pageSize = PageSize8K
		}
		sql.logger.Info(fmt.Sprintf("setting page size to %s", humanize.Bytes(uint64(pageSize))))
		err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
		if err != nil {
			return err
		}
		err = sql.treeWrite.Exec("PRAGMA synchronous=OFF;")
		if err != nil {
			return err
		}
		err = sql.treeWrite.Exec("PRAGMA journal_mode=WAL;")
		if err != nil {
			return err
		}
		err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA analysis_limit=%d;", defaultAnalysisLimit))
		if err != nil {
			return err
		}
		err = sql.treeWrite.Exec("PRAGMA temp_store=MEMORY;")
		if err != nil {
			return err
		}
		err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA temp_store_size=%d;", sql.opts.TempStoreSize))
		if err != nil {
			return err
		}

		sql.logger.Info(fmt.Sprintf("creating shard %d", defaultShardID))
		err := sql.treeWrite.Exec(fmt.Sprintf("CREATE TABLE tree_%d (version int, sequence int, bytes blob, orphaned bool);", defaultShardID))
		if err != nil {
			return err
		}
		err = sql.treeWrite.Exec(fmt.Sprintf("CREATE INDEX tree_idx_%d ON tree_%d (version, sequence);", defaultShardID, defaultShardID))
		if err != nil {
			return err
		}
	}
	if err = q.Close(); err != nil {
		return err
	}

	q, err = sql.leafWrite.Prepare("SELECT name from sqlite_master WHERE type='table' AND name='leaf'")
	if err != nil {
		return err
	}
	if !hasRow {
		err = sql.leafWrite.Exec(`
CREATE TABLE leaf (version int, sequence int, key blob, bytes blob, orphaned bool);
CREATE UNIQUE INDEX leaf_idx ON leaf (version DESC, sequence);
CREATE UNIQUE INDEX leaf_key_idx ON leaf (key, version DESC);
CREATE TABLE leaf_orphan (version int, sequence int, at int);
CREATE INDEX leaf_orphan_idx ON leaf_orphan (at DESC);`)
		if err != nil {
			return err
		}

		pageSize := max(os.Getpagesize(), defaultPageSize)
		if isForce8KPageSize {
			pageSize = PageSize8K
		}
		sql.logger.Info(fmt.Sprintf("setting page size to %s", humanize.Bytes(uint64(pageSize))))
		err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
		if err != nil {
			return err
		}
		err = sql.leafWrite.Exec("PRAGMA synchronous=OFF;")
		if err != nil {
			return err
		}
		err = sql.leafWrite.Exec("PRAGMA journal_mode=WAL;")
		if err != nil {
			return err
		}
		err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA analysis_limit=%d;", defaultAnalysisLimit))
		if err != nil {
			return err
		}
		err = sql.leafWrite.Exec("PRAGMA temp_store=MEMORY;")
		if err != nil {
			return err
		}
		err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA temp_store_size=%d;", sql.opts.TempStoreSize))
		if err != nil {
			return err
		}
	}
	if err = q.Close(); err != nil {
		return err
	}

	return nil
}

func (sql *SqliteDb) resetWriteConn() (err error) {
	if sql.treeWrite != nil {
		err = sql.treeWrite.Close()
		if err != nil {
			return err
		}
	}
	sql.treeWrite, err = gosqlite.Open(sql.opts.treeConnectionString(), sql.opts.Mode)
	if err != nil {
		return err
	}

	err = sql.treeWrite.Exec("PRAGMA synchronous=OFF;")
	// err = sql.treeWrite.Exec("PRAGMA synchronous=NORMAL;")
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA analysis_limit=%d;", defaultAnalysisLimit))
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec("PRAGMA temp_store=MEMORY;")
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA temp_store_size=%d;", sql.opts.TempStoreSize))
	if err != nil {
		return err
	}

	if err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", sql.opts.walPages)); err != nil {
		return err
	}

	sql.leafWrite, err = gosqlite.Open(sql.opts.leafConnectionString(), sql.opts.Mode)
	if err != nil {
		return err
	}

	err = sql.leafWrite.Exec("PRAGMA synchronous=OFF;")
	// err = sql.leafWrite.Exec("PRAGMA synchronous=NORMAL;")
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA analysis_limit=%d;", defaultAnalysisLimit))
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec("PRAGMA temp_store=MEMORY;")
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA temp_store_size=%d;", sql.opts.TempStoreSize))
	if err != nil {
		return err
	}

	if err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", sql.opts.walPages)); err != nil {
		return err
	}

	return err
}

func (sql *SqliteDb) newReadConn() (*SqliteReadConn, error) {
	var (
		conn *gosqlite.Conn
		err  error
	)

	connArgs := sql.opts.ConnArgs
	if !strings.Contains(sql.opts.ConnArgs, "mode=memory&cache=shared") {
		sql.opts.ConnArgs = "mode=ro"
	}

	openMode := gosqlite.OPEN_READONLY | gosqlite.OPEN_NOMUTEX
	conn, err = gosqlite.Open(sql.opts.treeConnectionString(), openMode)
	sql.opts.ConnArgs = connArgs

	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS changelog;", sql.opts.leafConnectionString()))
	if err != nil {
		return nil, err
	}
	err = conn.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return nil, err
	}
	err = conn.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return nil, err
	}
	pageSize := max(os.Getpagesize(), defaultPageSize)
	if isForce8KPageSize {
		pageSize = PageSize8K
	}
	err = conn.Exec(fmt.Sprintf("PRAGMA page_size=%d;", pageSize))
	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", sql.opts.MmapSize))
	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", sql.opts.CacheSize))
	if err != nil {
		return nil, err
	}
	err = conn.Exec("PRAGMA temp_store=MEMORY;")
	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("PRAGMA temp_store_size=%d;", sql.opts.TempStoreSize))
	if err != nil {
		return nil, err
	}
	busyTimeout := sql.opts.BusyTimeout * 3
	err = conn.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d;", busyTimeout))
	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("PRAGMA threads=%d;", sql.opts.ThreadsCount))
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
	err = conn.Exec(fmt.Sprintf("PRAGMA sqlite_stmt_cache=%d;", sql.opts.StatementCache))
	if err != nil {
		return nil, err
	}

	c := NewSqliteReadConn(conn, &sql.opts, sql.logger)

	return c, nil
}

func (sql *SqliteDb) resetReadConn() (err error) {
	if sql.read != nil {
		err = sql.read.Close()
		if err != nil {
			return err
		}
	}
	sql.read, err = sql.newReadConn()
	return err
}

func (sql *SqliteDb) getReadConn() (*SqliteReadConn, error) {
	if sql.useReadPool {
		return sql.readPool.GetConn()
	}

	var err error
	if sql.read == nil {
		sql.read, err = sql.newReadConn()
	}

	return sql.read, err
}

func (sql *SqliteDb) getLeaf(nodeKey NodeKey) (*Node, error) {
	// Fallback to old method for backward compatibility
	start := time.Now()
	defer func() {
		sql.metrics.MeasureSince(start, metricsNamespace, "db_get")
		sql.metrics.IncrCounter(1, metricsNamespace, "db_get_leaf")
	}()

	conn, err := sql.getReadConn()
	if err != nil {
		return nil, err
	}

	return conn.getLeaf(sql.pool, nodeKey)
}

func (sql *SqliteDb) getNode(nodeKey NodeKey) (*Node, error) {
	start := time.Now()
	defer func() {
		sql.metrics.MeasureSince(start, metricsNamespace, "db_get")
		sql.metrics.IncrCounter(1, metricsNamespace, "db_get_branch")
	}()

	conn, err := sql.getReadConn()
	if err != nil {
		return nil, err
	}

	return conn.getNode(sql.pool, nodeKey)
}

func (sql *SqliteDb) Close() error {
	if err := sql.closeHangingIterators(); err != nil {
		return err
	}

	if sql.readPool != nil {
		if err := sql.readPool.Close(); err != nil {
			return err
		}
	}

	if sql.read != nil {
		if err := sql.read.Close(); err != nil {
			return err
		}
	}

	if err := sql.leafWrite.Close(); err != nil {
		return err
	}

	if err := sql.treeWrite.Close(); err != nil {
		return err
	}

	return nil
}

func (sql *SqliteDb) nextShard(_ int64) (int64, error) {
	return defaultShardID, nil

	// if !sql.opts.ShardTrees {
	// 	switch sql.shards.Len() {
	// 	case 0:
	// 		break
	// 	case 1:
	// 		return sql.shards.Last(), nil
	// 	default:
	// 		return -1, fmt.Errorf("sharding is disabled but found shards; shards=%v", sql.shards.versions)
	// 	}
	// }
	//
	// sql.logger.Info(fmt.Sprintf("creating shard %d", version))
	// err := sql.treeWrite.Exec(fmt.Sprintf("CREATE TABLE tree_%d (version int, sequence int, bytes blob, orphaned bool);", version))
	// if err != nil {
	// 	return version, err
	// }
	// return version, sql.shards.Add(version)
}

func (sql *SqliteDb) SaveRoot(version int64, node *Node) error {
	if node != nil {
		buf := bufPool.Get().(*bytes.Buffer)
		err := node.BytesWithBuffer(buf)
		if err != nil {
			return err
		}
		bz := buf.Bytes()
		err = sql.treeWrite.Exec("INSERT OR REPLACE INTO root(version, node_version, node_sequence, bytes) VALUES (?, ?, ?, ?)",
			version, node.nodeKey.Version(), int(node.nodeKey.Sequence()), bz)
		if err != nil {
			return err
		}
		bufPool.Put(buf)
		return nil
	}
	// for an empty root a sentinel is saved
	return sql.treeWrite.Exec("INSERT OR REPLACE INTO root(version) VALUES (?)", version)
}

func (sql *SqliteDb) LoadRoot(version int64) (*Node, error) {
	conn, err := gosqlite.Open(sql.opts.treeConnectionString(), sql.opts.Mode)
	if err != nil {
		return nil, err
	}
	rootQuery, err := conn.Prepare("SELECT node_version, node_sequence, bytes FROM root WHERE version = ?", version)
	if err != nil {
		return nil, err
	}

	hasRow, err := rootQuery.Step()
	if !hasRow {
		return nil, fmt.Errorf("root not found for version %d", version)
	}
	if err != nil {
		return nil, err
	}
	var (
		nodeSeq     int
		nodeVersion int64
		nodeBz      []byte
	)
	err = rootQuery.Scan(&nodeVersion, &nodeSeq, &nodeBz)
	if err != nil {
		return nil, err
	}

	// if nodeBz is nil then a (valid) empty tree was saved, which a nil root represents
	var root *Node
	if nodeBz != nil {
		rootKey := NewNodeKey(nodeVersion, uint32(nodeSeq))
		root, err = MakeNode(sql.pool, rootKey, nodeBz)
		if err != nil {
			return nil, err
		}
	}

	if err := rootQuery.Close(); err != nil {
		return nil, err
	}
	if err := sql.ResetShardQueries(); err != nil {
		return nil, err
	}
	if err := conn.Close(); err != nil {
		return nil, err
	}
	return root, nil
}

func (sql *SqliteDb) getShard(_ int64) (int64, error) {
	// Disable shard and always return 1
	return defaultShardID, nil

	// if !sql.opts.ShardTrees {
	// 	if sql.shards.Len() != 1 {
	// 		return -1, fmt.Errorf("expected a single shard; path=%s", sql.opts.Path)
	// 	}
	// 	return sql.shards.Last(), nil
	// }
	// v := sql.shards.FindMemoized(version)
	// if v == -1 {
	// 	return -1, fmt.Errorf("version %d is after the first shard; shards=%v", version, sql.shards.versions)
	// }
	// return v, nil
}

func (sql *SqliteDb) ResetShardQueries() error {
	if sql.read != nil {
		if err := sql.read.ResetShardQueries(); err != nil {
			return err
		}
	}

	go func() {
		if sql.readPool != nil {
			sql.readPool.ResetShardQueries()
		}
	}()

	return nil
}

func (sql *SqliteDb) WarmLeaves() error {
	start := time.Now()

	var stmt *gosqlite.Stmt
	var err error

	// Use the connection pool if available
	if sql.readPool != nil {
		conn, err := sql.readPool.GetConn()
		if err != nil {
			return err
		}
		defer sql.readPool.ReleaseConn(conn)

		stmt, err = conn.conn.Prepare("SELECT version, sequence, key, bytes FROM changelog.leaf")
	} else {
		read, err := sql.getReadConn()
		if err != nil {
			return err
		}

		stmt, err = read.Prepare("SELECT version, sequence, key, bytes FROM leaf")
	}

	if err != nil {
		return err
	}

	var (
		cnt, version, seq int64
		kz, vz            []byte
	)
	for {
		ok, err := stmt.Step()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		cnt++
		err = stmt.Scan(&version, &seq, &kz, &vz)
		if err != nil {
			return err
		}
		if cnt%5_000_000 == 0 {
			sql.logger.Info(fmt.Sprintf("warmed %s leaves", humanize.Comma(cnt)))
		}
	}

	sql.logger.Info(fmt.Sprintf("warmed %s leaves in %s", humanize.Comma(cnt), time.Since(start)))

	return stmt.Close()
}

func isLeafSeq(seq uint32) bool {
	return seq&(1<<31) != 0
}

func (sql *SqliteDb) getRightNode(node *Node) (*Node, error) {
	if node.isLeaf() {
		return nil, errors.New("leaf node has no children")
	}
	var err error
	if isLeafSeq(node.rightNodeKey.Sequence()) {
		node.rightNode, err = sql.getLeaf(node.rightNodeKey)
	} else {
		node.rightNode, err = sql.getNode(node.rightNodeKey)
	}
	if node.rightNode == nil {
		err = errors.New("not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get right node node_key=%s height=%d path=%s: %w",
			node.rightNodeKey, node.subtreeHeight, sql.opts.Path, err)
	}
	return node.rightNode, nil
}

func (sql *SqliteDb) getLeftNode(node *Node) (*Node, error) {
	if node.isLeaf() {
		return nil, errors.New("leaf node has no children")
	}
	var err error
	if isLeafSeq(node.leftNodeKey.Sequence()) {
		node.leftNode, err = sql.getLeaf(node.leftNodeKey)
	} else {
		node.leftNode, err = sql.getNode(node.leftNodeKey)
	}
	if node.leftNode == nil {
		err = errors.New("not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get left node node_key=%s height=%d path=%s: %w",
			node.leftNodeKey, node.subtreeHeight, sql.opts.Path, err)
	}
	return node.leftNode, nil
}

func (sql *SqliteDb) isSharded() (bool, error) {
	q, err := sql.treeWrite.Prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%'")
	if err != nil {
		return false, err
	}
	var cnt int
	for {
		hasRow, err := q.Step()
		if err != nil {
			return false, err
		}
		if !hasRow {
			break
		}
		cnt++
		if cnt > 1 {
			break
		}
	}
	return cnt > 1, q.Close()
}

func (sql *SqliteDb) Revert(version int64) error {
	if err := sql.leafWrite.Exec("DELETE FROM leaf WHERE version > ?", version); err != nil {
		return err
	}
	if err := sql.leafWrite.Exec("DELETE FROM leaf_orphan WHERE at > ?", version); err != nil {
		return err
	}
	if err := sql.treeWrite.Exec("DELETE FROM root WHERE version > ?", version); err != nil {
		return err
	}
	if err := sql.treeWrite.Exec("DELETE FROM orphan WHERE at > ?", version); err != nil {
		return err
	}
	if err := sql.treeWrite.Exec(fmt.Sprintf("DELETE FROM tree_%d WHERE version > ?", defaultShardID), version); err != nil {
		return err
	}

	return nil

	// hasShards, err := sql.isSharded()
	// if err != nil {
	// 	return err
	// }
	// if hasShards {
	// 	q, err := sql.treeWrite.Prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%'")
	// 	if err != nil {
	// 		return err
	// 	}
	// 	var shards []string
	// 	for {
	// 		hasRow, err := q.Step()
	// 		if err != nil {
	// 			return err
	// 		}
	// 		if !hasRow {
	// 			break
	// 		}
	// 		var shard string
	// 		err = q.Scan(&shard)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		shardVersion, err := strconv.Atoi(shard[5:])
	// 		if err != nil {
	// 			return err
	// 		}
	// 		if shardVersion > version {
	// 			shards = append(shards, shard)
	// 		}
	// 	}
	// 	if err = q.Close(); err != nil {
	// 		return err
	// 	}
	// 	for _, shard := range shards {
	// 		if err = sql.treeWrite.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", shard)); err != nil {
	// 			return err
	// 		}
	// 	}
	// } else {
	//
	// }
	// return nil
}

func (sql *SqliteDb) closeHangingIterators() error {
	return sql.readPool.CloseHangingIterators()
}

func (sql *SqliteDb) replayChangelog(tree *Tree, toVersion int64, targetHash []byte) error {
	var (
		version     int
		lastVersion int
		sequence    int
		bz          []byte
		key         []byte
		count       int64
		start       = time.Now()
		since       = time.Now()
		logPath     = []interface{}{"path", sql.opts.Path}
	)
	tree.isReplaying = true
	defer func() {
		tree.isReplaying = false
	}()

	sql.opts.Logger.Info(fmt.Sprintf("replaying changelog from=%d to=%d", tree.version, toVersion), logPath...)

	var q *gosqlite.Stmt
	var conn *SqliteReadConn
	var poolConn *SqliteReadConn
	var err error

	// Use the connection pool if available
	if sql.readPool != nil {
		poolConn, err = sql.readPool.GetConn()
		if err != nil {
			return err
		}
		defer sql.readPool.ReleaseConn(poolConn)

		q, err = poolConn.conn.Prepare(`SELECT * FROM (
			SELECT version, sequence, key, bytes
		FROM changelog.leaf WHERE version > ? AND version <= ?
		) as ops
		ORDER BY version, sequence`)
	} else {
		conn, err = sql.getReadConn()
		if err != nil {
			return err
		}

		q, err = conn.Prepare(`SELECT * FROM (
			SELECT version, sequence, key, bytes
		FROM leaf WHERE version > ? AND version <= ?
		) as ops
		ORDER BY version, sequence`)
	}

	if err != nil {
		return err
	}

	if err = q.Bind(tree.version, toVersion); err != nil {
		return err
	}

	for {
		ok, err := q.Step()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		count++
		if err = q.Scan(&version, &sequence, &key, &bz); err != nil {
			return err
		}
		if version-1 != lastVersion {
			tree.leaves, tree.branches, tree.leafOrphans, tree.deletes = nil, nil, nil, nil
			tree.version = int64(version - 1)
			tree.resetSequences()
			lastVersion = version - 1
		}
		if bz != nil {
			nk := NewNodeKey(0, 0)
			node, err := MakeNode(tree.pool, nk, bz)
			if err != nil {
				return err
			}
			if _, err = tree.Set(node.key, node.hash); err != nil {
				return err
			}
			if sequence != int(tree.leafSequence) {
				return fmt.Errorf("sequence mismatch version=%d; expected %d got %d; path=%s",
					version, sequence, tree.leafSequence, sql.opts.Path)
			}
		} else {
			if _, _, err = tree.Remove(key); err != nil {
				return err
			}
			deleteSequence := tree.deletes[len(tree.deletes)-1].deleteKey.Sequence()
			if sequence != int(deleteSequence) {
				return fmt.Errorf("sequence delete mismatch; version=%d expected %d got %d; path=%s",
					version, sequence, tree.leafSequence, sql.opts.Path)
			}
		}
		if count%250_000 == 0 {
			sql.opts.Logger.Info(fmt.Sprintf("replayed changelog to version=%d count=%s node/s=%s",
				version, humanize.Comma(count), humanize.Comma(int64(250_000/time.Since(since).Seconds()))), logPath)
			since = time.Now()
		}
	}
	rootHash := tree.computeHash()
	if !bytes.Equal(targetHash, rootHash) {
		return fmt.Errorf("root hash mismatch; expected %x got %x", targetHash, rootHash)
	}
	tree.leaves, tree.branches, tree.leafOrphans, tree.deletes = nil, nil, nil, nil
	tree.resetSequences()
	tree.version = toVersion
	sql.opts.Logger.Info(fmt.Sprintf("replayed changelog to version=%d count=%s dur=%s root=%v",
		tree.version, humanize.Comma(count), time.Since(start).Round(time.Millisecond), tree.root), logPath)
	return q.Close()
}

func (sql *SqliteDb) Logger() Logger {
	return sql.logger
}

func DefaultSqliteDbOptions(opts SqliteDbOptions) SqliteDbOptions {
	return defaultSqliteDbOptions(opts)
}

func (sql *SqliteDb) GetAt(version int64, key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("get value with key length 0")
	}

	conn, err := sql.getReadConn()
	if err != nil {
		return nil, err
	}

	return conn.getVersioned(version, key)
}

func (sql *SqliteDb) HasRoot(version int64) (bool, error) {
	conn, err := gosqlite.Open(sql.opts.treeConnectionString(), sql.opts.Mode)
	if err != nil {
		return false, err
	}

	rootQuery, err := conn.Prepare("SELECT node_version FROM root WHERE version = ?", version)
	if err != nil {
		return false, err
	}

	hasRow, err := rootQuery.Step()
	if !hasRow {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if err := rootQuery.Close(); err != nil {
		return false, err
	}

	if err := conn.Close(); err != nil {
		return false, err
	}

	return true, nil
}

func (sql *SqliteDb) latestRoot() (version int64, err error) {
	conn, err := gosqlite.Open(sql.opts.treeConnectionString(), sql.opts.Mode)
	if err != nil {
		return 0, err
	}

	rootQuery, err := conn.Prepare("SELECT MAX(version) FROM root LIMIT 1")
	if err != nil {
		return 0, err
	}

	hasRow, err := rootQuery.Step()
	if !hasRow {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	err = rootQuery.Scan(&version)
	if err != nil {
		return 0, err
	}

	if err := rootQuery.Close(); err != nil {
		return 0, err
	}

	if err := conn.Close(); err != nil {
		return 0, err
	}

	return version, nil
}

func (sql *SqliteDb) getKVIteratorQuery(version int64, start, end []byte, ascending, inclusive bool) (stmt *gosqlite.Stmt, idx int, err error) {
	return sql.readPool.GetKVIteratorQuery(version, start, end, ascending, inclusive)
}

func (sql *SqliteDb) getHeightOneBranchesIteratorQuery(start, end int64) (stmt *gosqlite.Stmt, err error) {
	conn, err := sql.getReadConn()
	if err != nil {
		return nil, err
	}

	stmt, err = conn.Prepare(
		fmt.Sprintf("SELECT version, sequence, bytes FROM tree_%d WHERE version >= ? AND version <= ? ORDER BY version ASC", defaultShardID))
	if err != nil {
		return nil, err
	}

	if err = stmt.Bind(start, end); err != nil {
		return nil, err
	}

	return stmt, err
}

func (sql *SqliteDb) runAnalyze() error {
	start := time.Now()
	defer func() {
		sql.metrics.MeasureSince(start, metricsNamespace, "db_analyze")
		// sql.logger.Info(fmt.Sprintf("tree %s indexes analyzed, duration %d", sql.opts.Path, time.Since(start).Milliseconds()))
		fmt.Sprintf("tree %s indexes analyzed, duration %d\n", sql.opts.Path, time.Since(start).Milliseconds())
	}()

	eg := errgroup.Group{}
	eg.SetLimit(2)

	eg.Go(func() error {
		if err := sql.treeWrite.Exec(fmt.Sprintf("ANALYZE tree_idx_%d;", defaultShardID)); err != nil {
			return fmt.Errorf("failed to analyze tree %s: %w", sql.opts.Path, err)
		}
		return nil
	})

	eg.Go(func() error {
		if err := sql.leafWrite.Exec("ANALYZE leaf_idx;"); err != nil {
			return fmt.Errorf("failed to analyze tree leaf %s: %w", sql.opts.Path, err)
		}
		if err := sql.leafWrite.Exec("ANALYZE leaf_key_idx;"); err != nil {
			return fmt.Errorf("failed to analyze tree leaf %s: %w", sql.opts.Path, err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	sql.logger.Info(fmt.Sprintf("tree %s indexes analyzed", sql.opts.Path))

	return nil
}

func (sql *SqliteDb) runOptimize() error {
	start := time.Now()
	defer func() {
		sql.metrics.MeasureSince(start, metricsNamespace, "db_optimize")
		sql.logger.Info(fmt.Sprintf("tree %s indexes optimized, duration %d", sql.opts.Path, time.Since(start).Milliseconds()))
		fmt.Printf("tree %s indexes optimized, duration %d\n", sql.opts.Path, time.Since(start).Milliseconds())
	}()

	eg := errgroup.Group{}
	eg.SetLimit(2)

	eg.Go(func() error {
		if err := sql.treeWrite.Exec(fmt.Sprintf("PRAGMA optimize('tree_idx_%d');", defaultShardID)); err != nil {
			return fmt.Errorf("failed to optimize tree %s: %w", sql.opts.Path, err)
		}
		return nil
	})

	eg.Go(func() error {
		if err := sql.leafWrite.Exec("PRAGMA optimize('leaf_idx');"); err != nil {
			return fmt.Errorf("failed to optimize tree leaf %s: %w", sql.opts.Path, err)
		}
		if err := sql.leafWrite.Exec("PRAGMA optimize('leaf_key_idx');"); err != nil {
			return fmt.Errorf("failed to optimize tree leaf %s: %w", sql.opts.Path, err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	sql.logger.Info(fmt.Sprintf("tree %s indexes optimized", sql.opts.Path))

	return nil
}
