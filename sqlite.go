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
	"github.com/klauspost/compress/s2"
	api "github.com/kocubinski/costor-api"
	"golang.org/x/sync/errgroup"

	"github.com/sahara/iavl/metrics"
)

const defaultSQLitePath = "/tmp/iavl2"
const defaultShardID = 1
const defaultMaxPoolSize = 1000
const defaultPageSize = 4096 * 8 // 32K
const defaultThreadsCount = 8
const defaultAnalysisLimit = 2000
const defaultIncrementalVacuum = 50
const defaultWriteCacheSize = -256 * 1024 // 256M

// journal mode is database wide, only need to set once on write connections
const defaultJournalMode = "WAL"

// We cannot guarantee that read connection are not shared between different go routine
const openReadOnlyMode = gosqlite.OPEN_READONLY | gosqlite.OPEN_FULLMUTEX

type ConnectionType int

const (
	UseOption ConnectionType = iota
	Immutable
	ReadOnly
)

var (
	DisableS2Compression   = "1"
	isDisableS2Compression = true
)

func init() {
	if DisableS2Compression == "1" {
		isDisableS2Compression = true
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
	hashPool []*SqliteReadConn

	metrics metrics.Proxy
	logger  Logger

	useReadPool bool

	leafInsert *gosqlite.Stmt
	leafOrphan *gosqlite.Stmt
	treeInsert *gosqlite.Stmt
	treeOrphan *gosqlite.Stmt
}

func getPageSize() int {
	pageSize := os.Getpagesize()

	for pageSize < defaultPageSize {
		pageSize = pageSize * 2
	}

	return pageSize
}

func defaultSqliteDbOptions(opts SqliteDbOptions) SqliteDbOptions {
	if opts.Path == "" {
		opts.Path = defaultSQLitePath
	}
	// NOTE: mutex mode is set on open func call not here
	if opts.Mode == 0 {
		opts.Mode = gosqlite.OPEN_READWRITE | gosqlite.OPEN_CREATE
	}
	if opts.MmapSize == 0 {
		// opts.MmapSize = 512 * 1024 * 1024
		opts.MmapSize = 0 // disable mmap, it only map the first N bytes of data into memory
	}
	if opts.WalSize == 0 {
		opts.WalSize = 1024 * 1024 * 100
	}
	if opts.CacheSize == 0 {
		// 1G
		opts.CacheSize = -1 * 1024 * 1024
	}
	if opts.TempStoreSize == 0 {
		// 200M
		opts.TempStoreSize = 200 * 1024 * 1024
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.NilMetrics{}
	}

	opts.walPages = opts.WalSize / getPageSize()

	opts.ShardTrees = false

	if opts.MaxPoolSize == 0 {
		opts.MaxPoolSize = defaultMaxPoolSize
	}

	if opts.BusyTimeout == 0 {
		opts.BusyTimeout = 2000
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

func (opts SqliteDbOptions) connArgs(ty ConnectionType) string {
	// Short circuit for unit tests
	if strings.Contains(opts.ConnArgs, "mode=memory&cache=shared") {
		return opts.ConnArgs
	}

	var args string

	switch ty {
	case UseOption:
		if opts.ConnArgs == "" {
			return ""
		}
		args = opts.ConnArgs
	case ReadOnly:
		args = "mode=ro"
	// NOTE: immutable freeze database view on connection creation, it may not see latest changes compare to
	// first readonly then pragma immutable=1 later.
	case Immutable:
		args = "mode=ro&immutable=1"
	}

	return fmt.Sprintf("?%s", args)
}

func (opts SqliteDbOptions) leafConnectionString(ty ConnectionType) string {
	return fmt.Sprintf("file:%s/changelog.sqlite%s", opts.Path, opts.connArgs(ty))
}

func (opts SqliteDbOptions) treeConnectionString(ty ConnectionType) string {
	return fmt.Sprintf("file:%s/tree.sqlite%s", opts.Path, opts.connArgs(ty))
}

func (opts SqliteDbOptions) EstimateMmapSize() (uint64, error) {
	opts.Logger.Info("calculate mmap size")
	opts.Logger.Info(fmt.Sprintf("leaf connection string: %s", opts.leafConnectionString(ReadOnly)))
	conn, err := gosqlite.Open(opts.leafConnectionString(ReadOnly), openReadOnlyMode)
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

	if err = sql.prepareInsertStatements(); err != nil {
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

	// if err = sql.runQuickCheck(); err != nil {
	// 	return nil, err
	// }

	sql.hashPool = make([]*SqliteReadConn, 0)

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
		pageSize := getPageSize()
		sql.logger.Info(fmt.Sprintf("setting page size to %s", humanize.Bytes(uint64(pageSize))))
		err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
		if err != nil {
			return err
		}
		err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA journal_mode=%s;", defaultJournalMode))
		if err != nil {
			return err
		}
		err = sql.treeWrite.Exec("PRAGMA auto_vacuum=INCREMENTAL;")
		if err != nil {
			return err
		}

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
		pageSize := getPageSize()
		err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
		if err != nil {
			return err
		}
		err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA journal_mode=%s;", defaultJournalMode))
		if err != nil {
			return err
		}
		err = sql.leafWrite.Exec("PRAGMA auto_vacuum=INCREMENTAL;")
		if err != nil {
			return err
		}

		err = sql.leafWrite.Exec(`
CREATE TABLE leaf (version int, sequence int, key blob, bytes blob, orphaned bool);
CREATE UNIQUE INDEX IF NOT EXISTS leaf_idx ON leaf (version DESC, sequence);
CREATE UNIQUE INDEX IF NOT EXISTS leaf_key_idx ON leaf (key, version DESC);
CREATE TABLE leaf_orphan (version int, sequence int, at int);
CREATE INDEX leaf_orphan_idx ON leaf_orphan (at DESC);`)
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
	sql.treeWrite, err = gosqlite.Open(sql.opts.treeConnectionString(UseOption), sql.opts.Mode|gosqlite.OPEN_FULLMUTEX)
	if err != nil {
		return err
	}

	err = sql.treeWrite.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec("PRAGMA lock_mode=NORMAL;")
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec("PRAGMA automatic_index=OFF;")
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", defaultWriteCacheSize/2))
	if err != nil {
		return err
	}
	err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA journal_mode=%s;", defaultJournalMode))
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

	err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d;", sql.opts.BusyTimeout))
	if err != nil {
		return err
	}

	sql.leafWrite, err = gosqlite.Open(sql.opts.leafConnectionString(UseOption), sql.opts.Mode|gosqlite.OPEN_FULLMUTEX)
	if err != nil {
		return err
	}

	err = sql.leafWrite.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec("PRAGMA lock_mode=NORMAL;")
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec("PRAGMA automatic_index=OFF;")
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA cache_size=%d;", defaultWriteCacheSize/2))
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA journal_mode=%s;", defaultJournalMode))
	if err != nil {
		return err
	}
	err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA analysis_limit=%d;", defaultAnalysisLimit))
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

	err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d;", sql.opts.BusyTimeout))
	if err != nil {
		return err
	}

	return err
}

func (sql *SqliteDb) prepareInsertStatements() (err error) {
	if sql.leafInsert != nil {
		if err = sql.leafInsert.Close(); err != nil {
			return err
		}
	}
	sql.leafInsert, err = sql.leafWrite.Prepare("INSERT OR REPLACE INTO leaf (version, sequence, key, bytes) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}

	if sql.leafOrphan != nil {
		if err = sql.leafOrphan.Close(); err != nil {
			return err
		}
	}
	sql.leafOrphan, err = sql.leafWrite.Prepare("INSERT INTO leaf_orphan (version, sequence, at) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}

	if sql.treeOrphan != nil {
		if err = sql.treeOrphan.Close(); err != nil {
			return err
		}
	}
	sql.treeOrphan, err = sql.treeWrite.Prepare("INSERT INTO orphan (version, sequence, at) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}

	if sql.treeInsert != nil {
		if err = sql.treeInsert.Close(); err != nil {
			return err
		}
	}
	sql.treeInsert, err = sql.treeWrite.Prepare(fmt.Sprintf(
		"INSERT INTO tree_%d (version, sequence, bytes) VALUES (?, ?, ?)", defaultShardID))
	if err != nil {
		return err
	}

	return err
}

func (sql *SqliteDb) newReadConn() (*SqliteReadConn, error) {
	var (
		conn *gosqlite.Conn
		err  error
	)

	conn, err = gosqlite.Open(sql.opts.treeConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS changelog;", sql.opts.leafConnectionString(ReadOnly)))
	if err != nil {
		return nil, err
	}
	err = conn.Exec("PRAGMA automatic_index=OFF;")
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
	err = conn.Exec("PRAGMA query_only=ON;")
	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d;", sql.opts.BusyTimeout))
	if err != nil {
		return nil, err
	}

	// Below configuration may cause issues
	// err = conn.Exec(fmt.Sprintf("PRAGMA threads=%d;", sql.opts.ThreadsCount))
	// if err != nil {
	// 	return nil, err
	// }
	// err = conn.Exec(fmt.Sprintf("PRAGMA sqlite_stmt_cache=%d;", sql.opts.StatementCache))
	// if err != nil {
	// 	return nil, err
	// }
	// err = conn.Exec("PRAGMA read_uncommitted=ON;")
	// if err != nil {
	// 	return nil, err
	// }

	c := NewSqliteReadConn(conn, &sql.opts, sql.logger)

	return c, nil
}

func (sql *SqliteDb) resetReadConn() (err error) {
	// if sql.read != nil {
	// 	err = sql.read.Close()
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	// sql.read, err = sql.newReadConn()

	if sql.read == nil {
		sql.read, err = sql.newReadConn()
		return err
	}

	err = sql.read.conn.Exec("PRAGMA query_only=OFF;")
	if err != nil {
		return err
	}

	err = sql.read.conn.Exec("PRAGMA query_only=ON;")
	if err != nil {
		return err
	}

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

func (sql *SqliteDb) newHashConnection() (*SqliteReadConn, error) {
	conn, err := gosqlite.Open(sql.opts.treeConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS changelog;", sql.opts.leafConnectionString(ReadOnly)))
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = conn.Exec("PRAGMA automatic_index=OFF;")
	if err != nil {
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

	err = conn.Exec("PRAGMA read_uncommitted=OFF;")
	if err != nil {
		return nil, err
	}

	err = conn.Exec("PRAGMA query_only=ON;")
	if err != nil {
		return nil, err
	}

	return &SqliteReadConn{
		conn:         conn,
		treeVersion:  0,
		shards:       &VersionRange{},
		shardQueries: make(map[int64]*gosqlite.Stmt),
		opts:         &sql.opts,
		inUse:        false,
		logger:       sql.logger,
	}, nil
}

func (sql *SqliteDb) getHashConn() (*SqliteReadConn, error) {
	for _, conn := range sql.hashPool {
		if conn.IsInUse() {
			continue
		}

		conn.MarkInUse()
		return conn, nil
	}

	conn, err := sql.newHashConnection()
	if err != nil {
		return nil, err
	}

	conn.MarkInUse()
	sql.hashPool = append(sql.hashPool, conn)

	return conn, nil
}

func (sql *SqliteDb) returnHashConns(conns []*SqliteReadConn) {
	for _, conn := range conns {
		conn.MarkIdle()
	}
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
	if sql.leafInsert != nil {
		if err := sql.leafInsert.Close(); err != nil {
			sql.logger.Warn("failed to close leaf insert statement", "err", err)
		}
	}

	if sql.leafOrphan != nil {
		if err := sql.leafOrphan.Close(); err != nil {
			sql.logger.Warn("failed to close leaf orphan statement", "err", err)
		}
	}

	if sql.treeInsert != nil {
		if err := sql.treeInsert.Close(); err != nil {
			sql.logger.Warn("failed to close tree insert statement", "err", err)
		}
	}

	if sql.treeOrphan != nil {
		if err := sql.treeOrphan.Close(); err != nil {
			sql.logger.Warn("failed to close tree orphan statement", "err", err)
		}
	}

	if err := sql.closeHangingIterators(); err != nil {
		return err
	}

	if sql.readPool != nil {
		if err := sql.readPool.Close(); err != nil {
			return err
		}
	}

	if sql.hashPool != nil {
		for _, conn := range sql.hashPool {
			if err := conn.Close(); err != nil {
				return err
			}
		}
	}

	if sql.read != nil {
		if err := sql.read.Close(); err != nil {
			return err
		}
	}

	if sql.leafWrite != nil {
		if err := sql.leafWrite.Close(); err != nil {
			return err
		}
	}

	if sql.treeWrite != nil {
		if err := sql.treeWrite.Close(); err != nil {
			return err
		}
	}

	if sql.pool != nil {
		sql.pool = nil
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
		buf.Reset()
		defer bufPool.Put(buf)

		err := node.BytesWithBuffer(buf)
		if err != nil {
			return err
		}

		bz := buf.Bytes()
		if !isDisableS2Compression {
			compressBuf := bufPool.Get().(*bytes.Buffer)
			compressBuf.Reset()
			defer bufPool.Put(compressBuf)

			bz = s2.Encode(compressBuf.Bytes(), buf.Bytes())
		}

		err = sql.treeWrite.Exec("INSERT OR REPLACE INTO root(version, node_version, node_sequence, bytes) VALUES (?, ?, ?, ?)",
			version, node.nodeKey.Version(), int(node.nodeKey.Sequence()), bz)
		if err != nil {
			return err
		}

		return nil
	}
	// for an empty root a sentinel is saved
	return sql.treeWrite.Exec("INSERT OR REPLACE INTO root(version) VALUES (?)", version)
}

func (sql *SqliteDb) LoadRoot(version int64) (*Node, error) {
	conn, err := gosqlite.Open(sql.opts.treeConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rootQuery, err := conn.Prepare("SELECT node_version, node_sequence, bytes FROM root WHERE version = ? LIMIT 1", version)
	if err != nil {
		return nil, err
	}
	defer rootQuery.Close()

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

	if err := sql.ResetShardQueries(); err != nil {
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
		defer conn.MarkIdle()

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
	if sql.readPool != nil {
		return sql.readPool.CloseHangingIterators()
	}

	return nil
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

	sql.opts.Logger.Info(fmt.Sprintf("replaying changelog from=%d to=%d", tree.version.Load(), toVersion), logPath...)

	var q *gosqlite.Stmt
	var conn *SqliteReadConn
	var err error

	conn, err = sql.getReadConn()
	if err != nil {
		return err
	}
	defer conn.MarkIdle()

	q, err = conn.Prepare(`SELECT * FROM (
			SELECT version, sequence, key, bytes
		FROM leaf WHERE version > ? AND version <= ?
		) as ops
		ORDER BY version, sequence`)
	if err != nil {
		return err
	}
	defer q.Reset()

	if err = q.Bind(tree.version.Load(), toVersion); err != nil {
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
			tree.version.Store(int64(version - 1))
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
	tree.version.Store(toVersion)
	sql.opts.Logger.Info(fmt.Sprintf("replayed changelog to version=%d count=%s dur=%s root=%v",
		tree.version.Load(), humanize.Comma(count), time.Since(start).Round(time.Millisecond), tree.root), logPath)
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
	conn, err := gosqlite.Open(sql.opts.treeConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	rootQuery, err := conn.Prepare("SELECT node_version FROM root WHERE version = ? LIMIT 1", version)
	if err != nil {
		return false, err
	}
	defer rootQuery.Close()

	hasRow, err := rootQuery.Step()
	if !hasRow {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

func (sql *SqliteDb) LatestVersion() (int64, error) {
	return sql.latestRoot()
}

func (sql *SqliteDb) latestRoot() (version int64, err error) {
	conn, err := gosqlite.Open(sql.opts.treeConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	err = conn.Exec("PRAGMA immutable=1;")
	if err != nil {
		return 0, err
	}

	rootQuery, err := conn.Prepare("SELECT MAX(version) FROM root LIMIT 1")
	if err != nil {
		return 0, err
	}
	defer rootQuery.Close()

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
		sql.logger.Warn(fmt.Sprintf("tree %s indexes analyzed, duration %d", sql.opts.Path, time.Since(start).Milliseconds()))
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
		sql.logger.Warn(fmt.Sprintf("tree %s indexes optimized, duration %d", sql.opts.Path, time.Since(start).Milliseconds()))
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

func (sql *SqliteDb) runQuickCheck() error {
	start := time.Now()
	defer func() {
		sql.metrics.MeasureSince(start, metricsNamespace, "db_quick_check")
		sql.logger.Warn(fmt.Sprintf("tree %s quick check, duration %d", sql.opts.Path, time.Since(start).Milliseconds()))
	}()

	eg := errgroup.Group{}
	eg.SetLimit(2)

	eg.Go(func() error {
		if err := sql.treeWrite.Exec("PRAGMA quick_check"); err != nil {
			return fmt.Errorf("failed to quick check tree %s: %w", sql.opts.Path, err)
		}
		return nil
	})

	eg.Go(func() error {
		if err := sql.leafWrite.Exec("PRAGMA quick_check;"); err != nil {
			return fmt.Errorf("failed to quick check tree leaf %s: %w", sql.opts.Path, err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	sql.logger.Info(fmt.Sprintf("tree %s quick checked", sql.opts.Path))

	return nil
}
