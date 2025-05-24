package iavl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eatonphil/gosqlite"

	"github.com/cosmos/iavl/v2/metrics"
)

const (
	metricsNamespace  = "iavl2"
	leafSequenceStart = uint32(1 << 31)
)

var bufPool = &sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

type nodeDelete struct {
	// the sequence in which this deletion was processed
	deleteKey NodeKey
	// the leaf key to delete in `latest` table (if maintained)
	leafKey []byte
}

type nodeLoadTask struct {
	parentNode *Node
	isLeft     bool // true for left child, false for right child
	nodeKey    NodeKey
}

type Tree struct {
	version      atomic.Int64
	root         *Node
	metrics      metrics.Proxy
	sql          *SqliteDb
	sqlWriter    *sqlWriter
	writerCancel context.CancelFunc
	pool         *NodePool

	// options
	maxWorkingSize uint64
	workingBytes   uint64
	workingSize    int64
	heightFilter   int8
	metricsProxy   metrics.Proxy

	// state
	branches       []*Node
	leaves         []*Node
	branchOrphans  []NodeKey
	leafOrphans    []NodeKey
	deletes        []*nodeDelete
	leafSequence   uint32
	branchSequence uint32
	isReplaying    bool

	immutable         bool
	hashedVersion     int64
	modificationCount int64
	cache             map[string][]byte
	deleted           map[string]bool

	rw sync.RWMutex
}

type TreeOptions struct {
	StateStorage  bool
	HeightFilter  int8
	EvictionDepth int8
	MetricsProxy  metrics.Proxy
}

func DefaultTreeOptions() TreeOptions {
	return TreeOptions{
		StateStorage:  true,
		HeightFilter:  1,
		EvictionDepth: 18,
		MetricsProxy:  &metrics.NilMetrics{},
	}
}

// loadTaskPool reduces allocation for nodeLoadTask structs
var loadTaskPool = &sync.Pool{
	New: func() any {
		return make([]nodeLoadTask, 0, 64) // Pre-allocate with reasonable capacity
	},
}

// connectionPool for reusing database connections
var hashConnectionPool = &sync.Pool{
	New: func() any {
		return make([]*SqliteReadConn, 0, 4)
	},
}

// heightMapPool for reusing height maps
var heightMapPool = &sync.Pool{
	New: func() any {
		return make(map[int8][]*Node, 16) // Pre-allocate with reasonable capacity
	},
}

// nodeSlicePool for reusing node slices
var nodeSlicePool = &sync.Pool{
	New: func() any {
		return make([]*Node, 0, 1024)
	},
}

// AtomicNodeCounter for lock-free progress tracking
type AtomicNodeCounter struct {
	completed int64
	total     int64
}

func (c *AtomicNodeCounter) increment() {
	atomic.AddInt64(&c.completed, 1)
}

func (c *AtomicNodeCounter) setTotal(total int64) {
	atomic.StoreInt64(&c.total, total)
}

func (c *AtomicNodeCounter) progress() float64 {
	completed := atomic.LoadInt64(&c.completed)
	total := atomic.LoadInt64(&c.total)
	if total == 0 {
		return 0
	}
	return float64(completed) / float64(total)
}

// CompactNodeBatch for cache-friendly data layout
type CompactNodeBatch struct {
	nodes     []*Node
	parentIdx []int32
	isLeft    []bool
	nodeKeys  []NodeKey
}

func (cnb *CompactNodeBatch) reset() {
	cnb.nodes = cnb.nodes[:0]
	cnb.parentIdx = cnb.parentIdx[:0]
	cnb.isLeft = cnb.isLeft[:0]
	cnb.nodeKeys = cnb.nodeKeys[:0]
}

func (cnb *CompactNodeBatch) add(node *Node, parent int32, left bool, key NodeKey) {
	cnb.nodes = append(cnb.nodes, node)
	cnb.parentIdx = append(cnb.parentIdx, parent)
	cnb.isLeft = append(cnb.isLeft, left)
	cnb.nodeKeys = append(cnb.nodeKeys, key)
}

// compactBatchPool for cache-friendly batch processing
var compactBatchPool = &sync.Pool{
	New: func() any {
		return &CompactNodeBatch{
			nodes:     make([]*Node, 0, 256),
			parentIdx: make([]int32, 0, 256),
			isLeft:    make([]bool, 0, 256),
			nodeKeys:  make([]NodeKey, 0, 256),
		}
	},
}

// estimateCapacity uses subtree height for better memory pre-allocation
func (tree *Tree) estimateCapacity(rootNode *Node) (int, int) {
	if rootNode == nil {
		return 64, 32
	}

	height := rootNode.SubTreeHeight()
	if height <= 0 {
		return 64, 32
	}

	// Estimate based on height, but cap at reasonable limits
	estimatedNodes := min(1<<height, 10000)
	estimatedLeaves := estimatedNodes / 2

	// Ensure minimums for small trees
	if estimatedNodes < 64 {
		estimatedNodes = 64
	}
	if estimatedLeaves < 32 {
		estimatedLeaves = 32
	}

	return estimatedNodes, estimatedLeaves
}

// optimizedWorkerCount dynamically sizes worker pool based on workload
func (tree *Tree) optimizedWorkerCount(workload int) int {
	cpuCount := runtime.NumCPU()

	if workload <= 1 {
		return 1
	} else if workload <= 5 {
		return min(2, cpuCount)
	} else if workload <= 20 {
		return min(3, cpuCount)
	} else if workload <= 100 {
		return min(4, cpuCount)
	} else if workload <= 1000 {
		return min(6, cpuCount)
	}
	return min(8, cpuCount*2) // For large workloads
}

func NewTree(sql *SqliteDb, pool *NodePool, opts TreeOptions) *Tree {
	ctx, cancel := context.WithCancel(context.Background())
	if sql != nil {
		sql.useReadPool = false
	}

	tree := &Tree{
		sql:            sql,
		sqlWriter:      sql.newSQLWriter(),
		writerCancel:   cancel,
		pool:           pool,
		metrics:        opts.MetricsProxy,
		maxWorkingSize: 1.5 * 1024 * 1024 * 1024,
		heightFilter:   opts.HeightFilter,
		metricsProxy:   opts.MetricsProxy,
		leafSequence:   leafSequenceStart,
		immutable:      false,
		hashedVersion:  -1,
		cache:          make(map[string][]byte),
		deleted:        make(map[string]bool),
	}

	tree.version.Store(0)
	if tree.sql != nil {
		tree.sql.readPool.LinkTreeVersion(&tree.version)
	}

	tree.sqlWriter.start(ctx)

	return tree
}

func (tree *Tree) LoadVersion(version int64) (err error) {
	if tree.sql == nil {
		return errors.New("sql is nil")
	}

	if version == 0 {
		return nil
	}

	tree.version.Store(version)
	if tree.immutable {
		exists, err := tree.sql.HasRoot(version)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("root not found for version %d", version)
		}

		return nil
	}

	tree.rw.Lock()
	defer tree.rw.Unlock()

	tree.workingBytes = 0
	tree.workingSize = 0

	tree.root, err = tree.sql.LoadRoot(version)
	if err != nil {
		return err
	}

	tree.hashedVersion = version
	tree.cache = make(map[string][]byte)
	tree.deleted = make(map[string]bool)

	return nil
}

func (tree *Tree) LoadSnapshot(version int64, traverseOrder TraverseOrderType) (err error) {
	tree.rw.Lock()
	defer tree.rw.Unlock()

	var v int64
	tree.root, v, err = tree.sql.ImportMostRecentSnapshot(version, traverseOrder, true)
	if err != nil {
		return err
	}
	if v < version {
		return fmt.Errorf("requested %d found snapshot %d, replay not yet supported", version, v)
	}
	tree.version.Store(v)
	tree.hashedVersion = v
	tree.cache = make(map[string][]byte)
	tree.deleted = make(map[string]bool)
	return nil
}

func (tree *Tree) SaveSnapshot() (err error) {
	tree.rw.Lock()
	defer tree.rw.Unlock()

	ctx := context.Background()
	return tree.sql.Snapshot(ctx, tree)
}

func (tree *Tree) SaveVersion() ([]byte, int64, error) {
	tree.rw.Lock()
	defer tree.rw.Unlock()

	// TODO: fix query_trace_tx/query_trace_block panic (use after free)
	// if err := tree.sql.closeHangingIterators(); err != nil {
	// 	return nil, 0, err
	// }

	dirtyTreeVersion := tree.version.Load()
	savedTreeVersion := dirtyTreeVersion + 1

	tree.sql.readPool.SetSavingTree()
	defer tree.sql.readPool.UnsetSavingTree()

	rootHash := tree.computeHash()

	tree.version.Add(1)
	tree.resetSequences()

	err := tree.sqlWriter.saveTree(tree)
	if err != nil {
		return nil, dirtyTreeVersion, err
	}

	tree.resetSequences()

	tree.branchOrphans = nil
	tree.deleted = make(map[string]bool)
	tree.cache = make(map[string][]byte)
	tree.modificationCount = 0

	if err := tree.sql.resetReadConn(); err != nil {
		return nil, savedTreeVersion, err
	}

	if err := tree.sql.ResetShardQueries(); err != nil {
		return nil, savedTreeVersion, err
	}

	tree.leafOrphans = nil
	tree.leaves = nil
	tree.branches = nil
	tree.deletes = nil

	return rootHash, savedTreeVersion, nil
}

// ComputeHash the node and its descendants recursively. This usually mutates all
// descendant nodes. Returns the tree root node hash.
// If the tree is empty (i.e. the node is nil), returns the hash of an empty input,
// to conform with RFC-6962.
func (tree *Tree) computeHash() []byte {
	if tree.root == nil {
		return sha256.New().Sum(nil)
	}

	currentVersion := tree.version.Load()
	if tree.hashedVersion == currentVersion && tree.root.Hash() != nil {
		if tree.modificationCount != 0 {
			panic("unexpected tree modification, hash root twice")
		}
		return tree.root.Hash()
	}

	tree.deepHashParallel(tree.root, 0)

	tree.hashedVersion = currentVersion
	tree.modificationCount = 0
	return tree.root.Hash()
}

func (tree *Tree) newHashConnection() (*SqliteReadConn, error) {
	conn, err := gosqlite.Open(tree.sql.opts.treeConnectionString(ReadOnly), openReadOnlyMode)
	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS changelog;", tree.sql.opts.leafConnectionString(ReadOnly)))
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
		treeVersion:  tree.version.Load(),
		shards:       &VersionRange{},
		shardQueries: make(map[int64]*gosqlite.Stmt),
		opts:         &tree.sql.opts,
		inUse:        false,
		logger:       tree.sql.logger,
	}, nil
}

func (tree *Tree) deepHashParallel(node *Node, depth int8) {
	if node == nil {
		return
	}

	// Only use sequential for extremely small trees (under 10 nodes)
	estimatedNodes, _ := tree.estimateCapacity(node)
	if estimatedNodes < 10 {
		tree.deepHash(node, depth)
		return
	}

	// Get pooled resources to reduce allocations
	allBranches := nodeSlicePool.Get().([]*Node)
	allLeaves := nodeSlicePool.Get().([]*Node)
	nodesToLoad := loadTaskPool.Get().([]nodeLoadTask)

	defer func() {
		// Return resources to pools
		allBranches = allBranches[:0]
		allLeaves = allLeaves[:0]
		nodesToLoad = nodesToLoad[:0]

		nodeSlicePool.Put(allBranches)
		nodeSlicePool.Put(allLeaves)
		loadTaskPool.Put(nodesToLoad)
	}()

	// Initialize with estimated capacity
	tree.branches = make([]*Node, 0, estimatedNodes)
	tree.leaves = make([]*Node, 0, estimatedNodes/2)

	type nodeWithDepth struct {
		node  *Node
		depth int8
	}

	var toProcess = []nodeWithDepth{{node: node, depth: depth}}
	nextTreeVersion := tree.version.Load() + 1

	// Phase 1: Collect nodes and identify what needs loading (match original logic)
	for len(toProcess) > 0 {
		current := toProcess[0]
		toProcess = toProcess[1:]

		if current.node == nil {
			continue
		}

		// Handle leaf nodes first
		if current.node.isLeaf() {
			if current.node.nodeKey.Version() == nextTreeVersion {
				allLeaves = append(allLeaves, current.node)
			}
			continue
		}

		// Skip non-dirty or non-current version branch nodes
		if !current.node.dirty || current.node.nodeKey.Version() != nextTreeVersion {
			continue
		}

		// This is a dirty branch node that needs processing
		allBranches = append(allBranches, current.node)

		// Check if children need loading
		needsLeftLoad := current.node.leftNode == nil && !current.node.leftNodeKey.IsEmpty()
		needsRightLoad := current.node.rightNode == nil && !current.node.rightNodeKey.IsEmpty()

		if needsLeftLoad {
			nodesToLoad = append(nodesToLoad, nodeLoadTask{
				parentNode: current.node,
				isLeft:     true,
				nodeKey:    current.node.leftNodeKey,
			})
		}

		if needsRightLoad {
			nodesToLoad = append(nodesToLoad, nodeLoadTask{
				parentNode: current.node,
				isLeft:     false,
				nodeKey:    current.node.rightNodeKey,
			})
		}

		// Add children to processing queue if they're already loaded
		if current.node.leftNode != nil {
			toProcess = append(toProcess, nodeWithDepth{node: current.node.leftNode, depth: current.depth + 1})
		}
		if current.node.rightNode != nil {
			toProcess = append(toProcess, nodeWithDepth{node: current.node.rightNode, depth: current.depth + 1})
		}
	}

	// Phase 2: Always attempt parallel loading if there are nodes to load
	if len(nodesToLoad) > 0 {
		// Use fewer connections for smaller workloads to reduce overhead
		var maxConnections int
		if len(nodesToLoad) < 50 {
			maxConnections = 2
		} else if len(nodesToLoad) < 200 {
			maxConnections = 3
		} else {
			maxConnections = 4
		}

		actualConnections := min(maxConnections, tree.optimizedWorkerCount(len(nodesToLoad)))
		actualConnections = max(1, actualConnections) // Ensure at least 1

		connPool := make([]*SqliteReadConn, 0, actualConnections)
		defer func() {
			for _, conn := range connPool {
				conn.Close()
			}
		}()

		// Create connections - always try parallel, fallback on error
		parallelLoadSuccessful := false
		if actualConnections > 1 {
			for i := 0; i < actualConnections; i++ {
				conn, err := tree.newHashConnection()
				if err != nil {
					// Clean up any connections we've already created
					for _, existingConn := range connPool {
						existingConn.Close()
					}
					connPool = connPool[:0]
					break
				}
				connPool = append(connPool, conn)
			}

			if len(connPool) > 1 {
				tree.loadNodesParallel(nodesToLoad, connPool)
				parallelLoadSuccessful = true
			}
		}

		// Fallback to sequential loading if parallel failed or not attempted
		if !parallelLoadSuccessful {
			tree.loadNodesSequentially(nodesToLoad)
		}
	}

	// Phase 3: Continue tree traversal for newly loaded nodes
	toProcess = []nodeWithDepth{{node: node, depth: depth}}
	seen := make(map[*Node]bool, len(allBranches)+len(allLeaves))

	for len(toProcess) > 0 {
		current := toProcess[0]
		toProcess = toProcess[1:]

		if current.node == nil || seen[current.node] {
			continue
		}
		seen[current.node] = true

		if current.node.isLeaf() {
			// Already handled in Phase 1
			continue
		}

		// Skip nodes that don't meet the processing criteria
		if !current.node.dirty || current.node.nodeKey.Version() != nextTreeVersion {
			continue
		}

		// Add children to process queue (they should be loaded now)
		if current.node.leftNode != nil {
			toProcess = append(toProcess, nodeWithDepth{
				node:  current.node.leftNode,
				depth: current.depth + 1,
			})
		}
		if current.node.rightNode != nil {
			toProcess = append(toProcess, nodeWithDepth{
				node:  current.node.rightNode,
				depth: current.depth + 1,
			})
		}
	}

	// Phase 4: Always parallel process leaf nodes (if any exist)
	if len(allLeaves) > 0 {
		leafWorkers := tree.optimizedWorkerCount(len(allLeaves))
		leafWorkers = max(1, min(leafWorkers, 6)) // Cap at 6 workers, min 1

		if leafWorkers > 1 && len(allLeaves) > 2 { // Much lower threshold
			leafChan := make(chan *Node, min(len(allLeaves), 50))

			go func() {
				defer close(leafChan)
				for _, leaf := range allLeaves {
					leafChan <- leaf
				}
			}()

			var leafWg sync.WaitGroup
			for i := 0; i < leafWorkers; i++ {
				leafWg.Add(1)
				go func() {
					defer leafWg.Done()
					for leaf := range leafChan {
						leaf._hash()
					}
				}()
			}
			leafWg.Wait()
		} else {
			// Sequential for very small workloads
			for _, leaf := range allLeaves {
				leaf._hash()
			}
		}
	}

	// Phase 5: Always parallel process branch nodes by height (if any exist)
	if len(allBranches) > 0 {
		heightMap := make(map[int8][]*Node)
		var heights []int8

		// Group by height
		for _, branch := range allBranches {
			h := branch.subtreeHeight
			if _, exists := heightMap[h]; !exists {
				heights = append(heights, h)
			}
			heightMap[h] = append(heightMap[h], branch)
		}

		// Sort heights (process from lowest to highest)
		sort.Slice(heights, func(i, j int) bool {
			return heights[i] < heights[j]
		})

		// Process by height with optimized parallelism
		for _, height := range heights {
			branches := heightMap[height]
			branchWorkers := tree.optimizedWorkerCount(len(branches))
			branchWorkers = max(1, min(branchWorkers, 4)) // Cap at 4 workers, min 1

			if branchWorkers > 1 && len(branches) > 1 { // Much lower threshold - parallel for 2+ nodes
				branchChan := make(chan *Node, min(len(branches), 20))

				go func() {
					defer close(branchChan)
					for _, branch := range branches {
						branchChan <- branch
					}
				}()

				var branchWg sync.WaitGroup
				for i := 0; i < branchWorkers; i++ {
					branchWg.Add(1)
					go func() {
						defer branchWg.Done()
						for branch := range branchChan {
							// Clear hash and ensure children are hashed
							branch.SetHash(nil)
							if branch.leftNode != nil && branch.leftNode.hash == nil {
								branch.leftNode._hash()
							}
							if branch.rightNode != nil && branch.rightNode.hash == nil {
								branch.rightNode._hash()
							}
							branch._hash()
						}
					}()
				}
				branchWg.Wait()
			} else {
				// Sequential for single nodes
				for _, branch := range branches {
					branch.SetHash(nil)
					if branch.leftNode != nil && branch.leftNode.hash == nil {
						branch.leftNode._hash()
					}
					if branch.rightNode != nil && branch.rightNode.hash == nil {
						branch.rightNode._hash()
					}
					branch._hash()
				}
			}
		}
	}

	// Copy results to tree's collections
	tree.branches = make([]*Node, len(allBranches))
	copy(tree.branches, allBranches)
	tree.leaves = make([]*Node, len(allLeaves))
	copy(tree.leaves, allLeaves)

	// Phase 6: Post-processing (same as original)
	nodesToEvict := make(map[*Node]bool)
	nodesToReturn := make(map[*Node]bool)

	for _, branch := range tree.branches {
		if tree.heightFilter > 0 {
			leftNode := branch.leftNode
			rightNode := branch.rightNode

			if leftNode != nil && leftNode.isLeaf() {
				if !leftNode.dirty {
					nodesToReturn[leftNode] = true
				}
				branch.leftNode = nil
			}

			if rightNode != nil && rightNode.isLeaf() {
				if !rightNode.dirty {
					nodesToReturn[rightNode] = true
				}
				branch.rightNode = nil
			}
		}

		if branch.subtreeHeight < 2 {
			nodesToEvict[branch] = true
		}
	}

	for node := range nodesToEvict {
		node.evictChildren()
	}

	for node := range nodesToReturn {
		tree.returnNode(node)
	}
}

// loadNodesParallel loads nodes in parallel using multiple database connections
func (tree *Tree) loadNodesParallel(tasks []nodeLoadTask, connPool []*SqliteReadConn) {
	if len(tasks) == 0 {
		return
	}

	type loadResult struct {
		task *nodeLoadTask
		node *Node
		err  error
	}

	taskChan := make(chan *nodeLoadTask, len(tasks))
	resultChan := make(chan loadResult, len(tasks))

	// Send all tasks to channel
	for i := range tasks {
		taskChan <- &tasks[i]
	}
	close(taskChan)

	// Start worker goroutines
	var wg sync.WaitGroup
	maxWorkers := min(len(connPool), len(tasks))

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(connIdx int) {
			defer wg.Done()
			conn := connPool[connIdx]

			for task := range taskChan {
				var loadedNode *Node
				var err error

				// Load node from database using the dedicated connection
				if isLeafSeq(task.nodeKey.Sequence()) {
					loadedNode, err = conn.getLeaf(tree.pool, task.nodeKey)
				} else {
					loadedNode, err = conn.getNode(tree.pool, task.nodeKey)
				}

				resultChan <- loadResult{
					task: task,
					node: loadedNode,
					err:  err,
				}
			}
		}(i)
	}

	// Collect results
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Process results and update parent nodes
	for result := range resultChan {
		if result.err != nil {
			panic(fmt.Sprintf("Error loading node %s: %v", result.task.nodeKey, result.err))
		}

		if result.node == nil {
			panic(fmt.Sprintf("Node not found: %s", result.task.nodeKey))
		}

		// Update parent node with loaded child
		if result.task.isLeft {
			result.task.parentNode.leftNode = result.node
		} else {
			result.task.parentNode.rightNode = result.node
		}
	}
}

// loadNodesSequentially is a fallback method for sequential loading
func (tree *Tree) loadNodesSequentially(tasks []nodeLoadTask) {
	for _, task := range tasks {
		var err error

		if task.isLeft {
			_, err = task.parentNode.getLeftNode(tree)
		} else {
			_, err = task.parentNode.getRightNode(tree)
		}

		if err != nil {
			panic(fmt.Sprintf("Error loading node sequentially: %v", err))
		}
	}
}

// Original deepHash function kept for reference
func (tree *Tree) deepHash(node *Node, depth int8) {
	type nodeWithDepth struct {
		node    *Node
		depth   int8
		visited bool // Flag to track if children have been visited
	}

	nextTreeVersion := tree.version.Load() + 1

	// Estimate stack capacity based on tree height to avoid reallocations
	// 2^height is roughly the maximum number of nodes
	estimatedCapacity := min(1<<(node.SubTreeHeight()+1), 1024)
	stack := make([]nodeWithDepth, 0, estimatedCapacity)
	stack = append(stack, nodeWithDepth{node: node, depth: depth, visited: false})

	// Pre-allocate slices for append operations to avoid reallocations
	tree.branches = make([]*Node, 0, estimatedCapacity/2)
	tree.leaves = make([]*Node, 0, estimatedCapacity/2)

	// Track nodes that should be evicted but not returned to pool yet
	nodesToEvict := make(map[*Node]bool)
	// Track nodes that should be returned to pool after hash calculation
	nodesToReturn := make(map[*Node]bool)

	// Process nodes in a depth-first manner using a stack
	for len(stack) > 0 {
		// Pop from stack instead of peeking - reduces slice operations
		lastIdx := len(stack) - 1
		current := stack[lastIdx]
		stack = stack[:lastIdx]

		if current.node == nil {
			panic(fmt.Sprintf("node is nil; sql.path=%s", tree.sql.opts.Path))
		}

		// Check if this is a leaf node
		if current.node.isLeaf() {
			// new leaves are written every version
			if current.node.nodeKey.Version() == nextTreeVersion {
				tree.leaves = append(tree.leaves, current.node)
			}
			continue // No further processing for leaf nodes
		}

		// Skip non-dirty or non-current version branch nodes
		if !current.node.dirty || current.node.nodeKey.Version() != nextTreeVersion {
			continue
		}

		// If it's the first visit, process children
		if !current.visited {
			// Clear cached hash
			current.node.SetHash(nil)

			// Push back the current node with visited flag set
			current.visited = true
			stack = append(stack, current)

			// Fetch both children at once to reduce function calls
			leftNode := current.node.left(tree)
			rightNode := current.node.right(tree)

			// Push children to process first (post-order traversal)
			// Add right then left, so left is processed first due to LIFO stack
			stack = append(stack, nodeWithDepth{node: rightNode, depth: current.depth + 1, visited: false})
			stack = append(stack, nodeWithDepth{node: leftNode, depth: current.depth + 1, visited: false})
			continue
		}

		// Process the node after its children have been visited
		tree.branches = append(tree.branches, current.node)
		current.node._hash()

		// Apply height filter if enabled - combined conditional checks
		if tree.heightFilter > 0 {
			leftNode := current.node.leftNode
			rightNode := current.node.rightNode

			if leftNode != nil && leftNode.isLeaf() {
				if !leftNode.dirty {
					nodesToReturn[leftNode] = true
				}
				current.node.leftNode = nil
			}

			if rightNode != nil && rightNode.isLeaf() {
				if !rightNode.dirty {
					nodesToReturn[rightNode] = true
				}
				current.node.rightNode = nil
			}
		}

		// Apply eviction if at or beyond the eviction depth
		if current.node.SubTreeHeight() < 2 {
			nodesToEvict[current.node] = true
		}
	}

	for node := range nodesToEvict {
		node.evictChildren()
	}

	for node := range nodesToReturn {
		tree.returnNode(node)
	}
}

func (tree *Tree) Get(key []byte) ([]byte, error) {
	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), metricsNamespace, "tree_get")
	}

	tree.rw.RLock()
	defer tree.rw.RUnlock()

	treeVersion := tree.version.Load()

	if tree.immutable {
		return tree.sql.GetAt(treeVersion, key)
	}

	if val, exists := tree.cache[string(key)]; exists {
		return val, nil
	}
	if _, exists := tree.deleted[string(key)]; exists {
		return nil, nil
	}

	return tree.sql.GetAt(treeVersion, key)

	// var (
	// 	res []byte
	// 	err error
	// )
	//
	// 	if tree.root == nil {
	// 		return nil, nil
	// 	}
	// 	_, res, err = tree.root.get(tree, key)
	//
	// return res, err
}

func (tree *Tree) Has(key []byte) (bool, error) {
	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), metricsNamespace, "tree_has")
	}

	tree.rw.RLock()
	defer tree.rw.RUnlock()

	treeVersion := tree.version.Load()

	if tree.immutable {
		val, err := tree.sql.GetAt(treeVersion, key)
		if err != nil {
			return false, err
		}
		return val != nil, nil
	}

	if val, exists := tree.cache[string(key)]; exists {
		return val != nil, nil
	}
	if _, exists := tree.deleted[string(key)]; exists {
		return false, nil
	}

	val, err := tree.sql.GetAt(treeVersion, key)
	if err != nil {
		return false, err
	}

	return val != nil, nil

	// var (
	// 	err error
	// 	val []byte
	// )
	// 	if tree.root == nil {
	// 		return false, nil
	// 	}
	// 	_, val, err = tree.root.get(tree, key)
	// if err != nil {
	// 	return false, err
	// }
	// return val != nil, nil
}

// Set sets a key in the working tree. Nil values are invalid. The given
// key/value byte slices must not be modified after this call, since they point
// to slices stored within IAVL. It returns true when an existing value was
// updated, while false means it was a new key.
func (tree *Tree) Set(key, value []byte) (updated bool, err error) {
	if tree.immutable {
		panic("set on immutable tree")
	}

	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), metricsNamespace, "tree_set")
	}

	tree.rw.Lock()
	defer tree.rw.Unlock()

	updated, err = tree.set(key, value)
	if err != nil {
		return false, err
	}
	if updated {
		tree.metrics.IncrCounter(1, metricsNamespace, "tree_update")
	} else {
		tree.metrics.IncrCounter(1, metricsNamespace, "tree_new_node")
	}

	tree.modificationCount++
	tree.cache[string(key)] = value
	delete(tree.deleted, string(key))

	return updated, nil
}

func (tree *Tree) set(key []byte, value []byte) (updated bool, err error) {
	if value == nil {
		return updated, fmt.Errorf("attempt to store nil value at key '%s'", key)
	}

	if tree.root == nil {
		tree.root = tree.NewLeafNode(key, value)
		return updated, nil
	}

	tree.root, updated, err = tree.recursiveSet(tree.root, key, value)
	return updated, err
}

func (tree *Tree) recursiveSet(node *Node, key []byte, value []byte) (
	newSelf *Node, updated bool, err error,
) {
	// Define a struct to track our traversal state
	type setFrame struct {
		node    *Node
		key     []byte
		value   []byte
		goLeft  bool // Whether we should go left or right from this node
		visited bool // Whether this node's children have been processed
	}

	// Create stack of frames to process
	stack := make([]setFrame, 0, 32) // Initial capacity to reduce allocations
	stack = append(stack, setFrame{
		node:    node,
		key:     key,
		value:   value,
		goLeft:  bytes.Compare(key, node.Key()) < 0,
		visited: false,
	})

	// Process frames in a loop until stack is empty
	var currentNode *Node
	childMap := make(map[*Node]*Node) // Maps parent nodes to their processed children

	for len(stack) > 0 {
		// Get current frame from the top of stack
		currentIndex := len(stack) - 1
		currentFrame := &stack[currentIndex]
		currentNode = currentFrame.node

		// Handle nil node (should never happen)
		if currentNode == nil {
			panic("node is nil")
		}

		// Handle leaf nodes directly - no recursion needed
		if currentNode.isLeaf() {
			// Pop the frame
			stack = stack[:currentIndex]

			switch bytes.Compare(currentFrame.key, currentNode.Key()) {
			case -1: // setKey < leafKey
				tree.metrics.IncrCounter(2, metricsNamespace, "pool_get")
				parent := tree.pool.Get()
				parent.nodeKey = tree.nextNodeKey()
				parent.key = currentNode.Key()
				parent.subtreeHeight = 1
				parent.size = 2
				parent.dirty = true
				parent.setLeft(tree.NewLeafNode(currentFrame.key, currentFrame.value))
				parent.setRight(currentNode)

				tree.workingBytes += parent.sizeBytes()
				tree.workingSize++

				// Store result for parent frame
				if len(stack) > 0 {
					parentFrame := &stack[len(stack)-1]
					childMap[parentFrame.node] = parent
				} else {
					return parent, false, nil
				}
			case 1: // setKey > leafKey
				tree.metrics.IncrCounter(2, metricsNamespace, "pool_get")
				parent := tree.pool.Get()
				parent.nodeKey = tree.nextNodeKey()
				parent.key = currentFrame.key
				parent.subtreeHeight = 1
				parent.size = 2
				parent.dirty = true
				parent.setLeft(currentNode)
				parent.setRight(tree.NewLeafNode(currentFrame.key, currentFrame.value))

				tree.workingBytes += parent.sizeBytes()
				tree.workingSize++

				// Store result for parent frame
				if len(stack) > 0 {
					parentFrame := &stack[len(stack)-1]
					childMap[parentFrame.node] = parent
				} else {
					return parent, false, nil
				}
			default: // Equal keys - update the value
				tree.addOrphan(currentNode)
				wasDirty := currentNode.dirty
				tree.mutateNode(currentNode)
				if tree.isReplaying {
					currentNode.SetHash(currentFrame.value)
				} else {
					if wasDirty {
						tree.workingBytes -= currentNode.sizeBytes()
					}
					currentNode.SetValue(currentFrame.value)
					// currentNode._hash() // parallel hash in deepHashParallel
					tree.workingBytes += currentNode.sizeBytes()
				}

				// Store result for parent frame
				if len(stack) > 0 {
					parentFrame := &stack[len(stack)-1]
					childMap[parentFrame.node] = currentNode
					updated = true
				} else {
					return currentNode, true, nil
				}
			}
		} else {
			// Handle internal nodes
			if !currentFrame.visited {
				// Mark as visited to avoid repeated work
				currentFrame.visited = true
				stack[currentIndex] = *currentFrame

				// First visit: add orphan and mutate node
				tree.addOrphan(currentNode)
				tree.mutateNode(currentNode)

				// Add frame for child node traversal
				var childNode *Node
				if currentFrame.goLeft {
					childNode = currentNode.left(tree)
				} else {
					childNode = currentNode.right(tree)
				}

				// Push child onto stack
				stack = append(stack, setFrame{
					node:    childNode,
					key:     currentFrame.key,
					value:   currentFrame.value,
					goLeft:  bytes.Compare(currentFrame.key, childNode.Key()) < 0,
					visited: false,
				})
			} else {
				// Pop frame from stack
				stack = stack[:currentIndex]

				// Get processed child node
				childNode, exists := childMap[currentNode]
				if !exists {
					return nil, false, fmt.Errorf("internal error: child not found for node")
				}

				// Delete from map to prevent memory leaks
				delete(childMap, currentNode)

				// Apply the child node to the current node
				if currentFrame.goLeft {
					currentNode.setLeft(childNode)
				} else {
					currentNode.setRight(childNode)
				}

				// If the child was updated, no need for balancing
				if updated {
					// Store result for parent frame if not at root
					if len(stack) > 0 {
						parentFrame := &stack[len(stack)-1]
						childMap[parentFrame.node] = currentNode
					} else {
						return currentNode, updated, nil
					}
				} else {
					// Perform height/size calculation and balancing
					err = currentNode.calcHeightAndSize(tree)
					if err != nil {
						return nil, false, err
					}

					newNode, err := tree.balance(currentNode)
					if err != nil {
						return nil, false, err
					}

					// Store result for parent frame if not at root
					if len(stack) > 0 {
						parentFrame := &stack[len(stack)-1]
						childMap[parentFrame.node] = newNode
					} else {
						return newNode, updated, nil
					}
				}
			}
		}
	}

	// We should never reach here if the algorithm is implemented correctly
	return node, false, fmt.Errorf("unexpected exit from recursiveSet")
}

// Remove removes a key from the working tree. The given key byte slice should not be modified
// after this call, since it may point to data stored inside IAVL.
func (tree *Tree) Remove(key []byte) ([]byte, bool, error) {
	if tree.immutable {
		panic("Remove on immutable tree")
	}

	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), metricsNamespace, "tree_remove")
	}

	tree.rw.Lock()
	defer tree.rw.Unlock()

	if tree.root == nil {
		return nil, false, nil
	}

	delete(tree.cache, string(key))

	newRoot, _, value, removed, err := tree.recursiveRemove(tree.root, key)
	if err != nil {
		return nil, false, err
	}
	if !removed {
		return nil, false, nil
	}

	tree.deleted[string(key)] = true
	tree.modificationCount++

	tree.metrics.IncrCounter(1, metricsNamespace, "tree_delete")

	tree.root = newRoot
	return value, true, nil
}

// removes the node corresponding to the passed key and balances the tree.
// It returns:
// - the hash of the new node (or nil if the node is the one removed)
// - the node that replaces the orig. node after remove
// - new leftmost leaf key for tree after successfully removing 'key' if changed.
// - the removed value
func (tree *Tree) recursiveRemove(node *Node, key []byte) (newSelf *Node, newKey []byte, newValue []byte, removed bool, err error) {
	// Define a struct to track our traversal state
	type removeFrame struct {
		node    *Node
		key     []byte
		visited bool // Whether this node's children have been processed
		goLeft  bool // Whether we go left or right from this node
	}

	// Create stack of frames to process
	stack := make([]removeFrame, 0, 32) // Initial capacity to reduce allocations
	stack = append(stack, removeFrame{
		node:    node,
		key:     key,
		visited: false,
		goLeft:  bytes.Compare(key, node.Key()) < 0,
	})

	// Maps parent nodes to their processed children and results
	type resultInfo struct {
		node       *Node  // The new node replacing the old one
		newKey     []byte // New key (if any)
		value      []byte // Value removed (if any)
		wasRemoved bool   // Whether a removal occurred in this subtree
	}
	resultMap := make(map[*Node]resultInfo)

	// Track nodes that should be returned to the pool after the operation
	nodesToReturn := make(map[*Node]bool)

	// Process frames until the stack is empty
	for len(stack) > 0 {
		// Get current frame from the top of stack
		currentIndex := len(stack) - 1
		currentFrame := &stack[currentIndex]
		currentNode := currentFrame.node

		// Handle leaf nodes directly
		if currentNode.isLeaf() {
			// Pop the frame
			stack = stack[:currentIndex]

			var result resultInfo

			// Check if this is the leaf we're looking for
			if bytes.Equal(currentFrame.key, currentNode.Key()) {
				// Found the node to remove
				tree.addDelete(currentNode)
				nodesToReturn[currentNode] = true

				result = resultInfo{
					node:       nil,
					newKey:     nil,
					value:      currentNode.Value(),
					wasRemoved: true,
				}
			} else {
				// This leaf doesn't match the key, keep it
				result = resultInfo{
					node:       currentNode,
					newKey:     nil,
					value:      nil,
					wasRemoved: false,
				}
			}

			// Store result for parent frame
			if len(stack) > 0 {
				parentFrame := &stack[len(stack)-1]
				resultMap[parentFrame.node] = result
			} else {
				// We're at the root
				// Return nodes to pool now that we're done
				for n := range nodesToReturn {
					tree.returnNode(n)
				}
				return result.node, result.newKey, result.value, result.wasRemoved, nil
			}
			continue
		}

		// Handle internal nodes
		if !currentFrame.visited {
			// Mark as visited for the next iteration
			currentFrame.visited = true
			stack[currentIndex] = *currentFrame

			// Visit the appropriate child node based on key comparison
			var childNode *Node
			if currentFrame.goLeft {
				childNode = currentNode.left(tree)
			} else {
				childNode = currentNode.right(tree)
			}

			// Add child to the stack
			stack = append(stack, removeFrame{
				node:    childNode,
				key:     currentFrame.key,
				visited: false,
				goLeft:  bytes.Compare(currentFrame.key, childNode.Key()) < 0,
			})
		} else {
			// Pop the frame since we've processed its children
			stack = stack[:currentIndex]

			// Get the result from the processed child
			childResult, exists := resultMap[currentNode]
			if !exists {
				return nil, nil, nil, false, fmt.Errorf("internal error: child result not found")
			}

			// Clean up the result map to prevent memory leaks
			delete(resultMap, currentNode)

			// If nothing was removed in the subtree, just pass it up
			if !childResult.wasRemoved {
				if len(stack) > 0 {
					parentFrame := &stack[len(stack)-1]
					resultMap[parentFrame.node] = childResult
				} else {
					// We're at the root
					// Return nodes to pool now that we're done
					for n := range nodesToReturn {
						tree.returnNode(n)
					}
					return childResult.node, childResult.newKey, childResult.value, childResult.wasRemoved, nil
				}
				continue
			}

			// We need to update the current node based on the removal result
			tree.addOrphan(currentNode)

			var resultNode *Node
			var resultKey []byte

			if currentFrame.goLeft {
				// Left child was affected
				if childResult.node == nil {
					// Left node held value, was removed
					// Collapse `node.rightNode` into `node`
					rightNode := currentNode.right(tree)
					resultKey = currentNode.Key() // Important: pass the current node's key up
					nodesToReturn[currentNode] = true
					resultNode = rightNode
				} else {
					// Left subtree changed but node wasn't removed
					tree.mutateNode(currentNode)
					currentNode.setLeft(childResult.node)

					// Update node's height and size
					err := currentNode.calcHeightAndSize(tree)
					if err != nil {
						return nil, nil, nil, false, err
					}

					// Balance the node
					resultNode, err = tree.balance(currentNode)
					if err != nil {
						return nil, nil, nil, false, err
					}

					// Important: propagate the new key if there is one from child
					resultKey = childResult.newKey
				}
			} else {
				// Right child was affected
				if childResult.node == nil {
					// Right node held value, was removed
					// Collapse `node.leftNode` into `node`
					leftNode := currentNode.left(tree)
					nodesToReturn[currentNode] = true
					resultNode = leftNode
					// No new key when right node is removed and replaced with left node
				} else {
					// Right subtree changed but node wasn't removed
					tree.mutateNode(currentNode)
					currentNode.setRight(childResult.node)

					// Update key if needed (this is crucial for correct hash calculation)
					if childResult.newKey != nil {
						currentNode.SetKey(childResult.newKey)
					}

					// Update node's height and size
					err := currentNode.calcHeightAndSize(tree)
					if err != nil {
						return nil, nil, nil, false, err
					}

					// Balance the node
					resultNode, err = tree.balance(currentNode)
					if err != nil {
						return nil, nil, nil, false, err
					}
				}
			}

			// Create result for this node
			result := resultInfo{
				node:       resultNode,
				newKey:     resultKey,
				value:      childResult.value,
				wasRemoved: true,
			}

			// Store result for parent frame or return final result
			if len(stack) > 0 {
				parentFrame := &stack[len(stack)-1]
				resultMap[parentFrame.node] = result
			} else {
				// We're at the root
				// Return nodes to pool now that we're done
				for n := range nodesToReturn {
					n.leftNode = nil
					n.rightNode = nil
					tree.returnNode(n)
				}
				return result.node, result.newKey, result.value, result.wasRemoved, nil
			}
		}
	}

	// We should never reach here
	// If we somehow do, make sure to clean up
	for n := range nodesToReturn {
		tree.returnNode(n)
	}
	return nil, nil, nil, false, fmt.Errorf("unexpected exit from recursiveRemove")
}

func (tree *Tree) Size() int64 {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	return tree.root.size
}

func (tree *Tree) Height() int8 {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	return tree.root.SubTreeHeight()
}

func (tree *Tree) nextNodeKey() NodeKey {
	tree.branchSequence++
	nk := NewNodeKey(tree.version.Load()+1, tree.branchSequence)
	return nk
}

func (tree *Tree) nextLeafNodeKey() NodeKey {
	tree.leafSequence++
	if tree.leafSequence < leafSequenceStart {
		panic("leaf sequence underflow")
	}
	nk := NewNodeKey(tree.version.Load()+1, tree.leafSequence)
	return nk
}

func (tree *Tree) resetSequences() {
	tree.leafSequence = leafSequenceStart
	tree.branchSequence = 0
}

func (tree *Tree) mutateNode(node *Node) {
	// this seems to be true only in certain cases
	// we should investigate if we can remove this check
	alreadyMutatedForNextVersion := node.Hash() == nil && node.nodeKey.Version() == tree.version.Load()+1
	if alreadyMutatedForNextVersion {
		// This node has already been mutated for the next version, so we can skip
		// This can happen when we have already processed this node during a recursive operation
		return
	}

	// Always mark the hash as nil to ensure recomputation
	node.SetHash(nil)

	// Create a new nodeKey with the next version
	if node.isLeaf() {
		node.nodeKey = tree.nextLeafNodeKey()
	} else {
		node.nodeKey = tree.nextNodeKey()
	}

	// Mark the node as dirty for tracking
	node.dirty = true

	// Mark the tree hash as dirty
	tree.markHashDirty()
}

func (tree *Tree) addOrphan(node *Node) {
	if node.Hash() == nil {
		return
	}
	if !node.isLeaf() {
		tree.branchOrphans = append(tree.branchOrphans, node.nodeKey)
	} else if node.isLeaf() && !node.dirty {
		tree.leafOrphans = append(tree.leafOrphans, node.nodeKey)
	}
}

func (tree *Tree) addDelete(node *Node) {
	// added and removed in the same version; no op.
	if node.nodeKey.Version() == tree.version.Load()+1 {
		return
	}
	del := &nodeDelete{
		deleteKey: tree.nextLeafNodeKey(),
		leafKey:   node.Key(),
	}
	tree.deletes = append(tree.deletes, del)
}

// NewLeafNode returns a new node from a key, value and version.
func (tree *Tree) NewLeafNode(key []byte, value []byte) *Node {
	node := tree.pool.Get()

	node.nodeKey = tree.nextLeafNodeKey()

	node.key = key
	node.subtreeHeight = 0
	node.size = 1

	if tree.isReplaying {
		node.hash = value
	} else {
		node.value = value
		// node._hash() // parallel hash in deepHashParallel
	}

	node.dirty = true
	tree.workingBytes += node.sizeBytes()
	tree.workingSize++
	return node
}

func (tree *Tree) returnNode(node *Node) {
	if node == nil {
		return
	}

	node.checkValid()

	// Make sure node is not the tree's root before recycling
	if node == tree.root {
		return
	}

	// Check for lingering references before returning to pool
	if node.leftNode != nil || node.rightNode != nil {
		panic("Attempted to return node with active child references")
	}

	if node.dirty {
		tree.workingBytes -= node.sizeBytes()
		tree.workingSize--
	}

	// Clear ALL fields to prevent carrying stale data
	node.hash = nil
	node.key = nil
	node.value = nil
	node.dirty = false
	node.evict = false
	node.nodeKey = emptyNodeKey
	node.leftNodeKey = emptyNodeKey
	node.rightNodeKey = emptyNodeKey
	node.size = 0
	node.subtreeHeight = -1

	// Return node to the pool for reuse
	tree.pool.Put(node)
}

func (tree *Tree) Close() error {
	if tree.writerCancel != nil {
		tree.writerCancel()
		tree.sqlWriter.awaitStop()
	}
	return tree.sql.Close()
}

func (tree *Tree) Hash() []byte {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	if tree.root == nil {
		return emptyHash
	}
	return tree.root.Hash()
}

func (tree *Tree) Version() int64 {
	return tree.version.Load()
}

func (tree *Tree) replayChangelog(toVersion int64, targetHash []byte) error {
	return tree.sql.replayChangelog(tree, toVersion, targetHash)
}

func (tree *Tree) PausePruning(pause bool) {
	tree.sqlWriter.pausePruning.Store(pause)
}

func (tree *Tree) DeleteVersionsTo(toVersion int64) error {
	tree.sqlWriter.treePruneCh <- &pruneSignal{pruneVersion: toVersion}
	tree.sqlWriter.leafPruneCh <- &pruneSignal{pruneVersion: toVersion}
	return nil
}

func (tree *Tree) DeleteVersionsToSync(toVersion int64) error {
	tree.sqlWriter.awaitTreePruned = make(chan struct{})
	tree.sqlWriter.treePruneCh <- &pruneSignal{pruneVersion: toVersion}
	tree.sqlWriter.leafPruneCh <- &pruneSignal{pruneVersion: toVersion}
	<-tree.sqlWriter.awaitTreePruned
	return nil
}

func (tree *Tree) WorkingBytes() uint64 {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	return tree.workingBytes
}

func (tree *Tree) GetWithIndex(key []byte) (int64, []byte, error) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	if tree.root == nil {
		return 0, nil, nil
	}

	return tree.root.get(tree, key)
}

func (tree *Tree) GetByIndex(index int64) (key []byte, value []byte, err error) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	if tree.root == nil {
		return nil, nil, nil
	}

	return tree.getByIndex(tree.root, index)
}

func (tree *Tree) getByIndex(node *Node, index int64) (key []byte, value []byte, err error) {
	if node.isLeaf() {
		if index == 0 {
			return node.Key(), node.Value(), nil
		}
		return nil, nil, nil
	}

	leftNode, err := node.getLeftNode(tree)
	if err != nil {
		return nil, nil, err
	}

	if index < leftNode.size {
		return tree.getByIndex(leftNode, index)
	}

	rightNode, err := node.getRightNode(tree)
	if err != nil {
		return nil, nil, err
	}

	return tree.getByIndex(rightNode, index-leftNode.size)
}

func (tree *Tree) SetInitialVersion(version int64) error {
	if tree.immutable {
		panic("set initial version on immutable tree")
	}

	var err error

	tree.version.Store(version - 1)

	return err
}

func (tree *Tree) GetRecent(version int64, key []byte) (bool, []byte, error) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	got, root := tree.getRecentRoot(version)
	if !got {
		return false, nil, nil
	}
	if root == nil {
		return true, nil, nil
	}

	_, val, err := root.get(tree, key)
	return true, val, err
}

func (tree *Tree) getRecentRoot(version int64) (bool, *Node) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	if version != tree.version.Load() {
		return false, nil
	}

	// Version == 0
	if tree.root == nil {
		return false, nil
	}

	root := *tree.root

	return true, &root
}

func (tree *Tree) IterateRecent(version int64, start, end []byte, ascending bool) (bool, Iterator) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	got, root := tree.getRecentRoot(version)
	if !got {
		return false, nil
	}
	itr := &TreeIterator{
		tree:      tree,
		start:     start,
		end:       end,
		ascending: ascending,
		inclusive: false,
		stack:     []*Node{root},
		valid:     true,
		metrics:   tree.metricsProxy,
	}
	itr.Next()
	return true, itr
}

func (tree *Tree) Import(version int64) (*Importer, error) {
	return newImporter(tree, version)
}

func (tree *Tree) WorkingHash() []byte {
	tree.rw.Lock()
	defer tree.rw.Unlock()

	if tree.root == nil {
		return emptyHash
	}

	if tree.root.Hash() != nil {
		return tree.root.Hash()
	}

	// TODO: fix query_trace_tx/query_trace_block panic (use after free)
	// if err := tree.sql.closeHangingIterators(); err != nil {
	// 	panic(err)
	// }

	tree.sql.readPool.SetSavingTree()
	defer tree.sql.readPool.UnsetSavingTree()

	hash := tree.computeHash()

	return hash
}

func (tree *Tree) GetImmutable(version int64) (*Tree, error) {
	// We can discard whole pool after usage
	pool := NewNodePool()

	sql := &SqliteDb{
		opts:        tree.sql.opts,
		pool:        pool,
		readPool:    tree.sql.readPool,
		metrics:     tree.sql.metrics,
		logger:      tree.sql.logger,
		useReadPool: true,
	}

	imTree := &Tree{
		sql:            sql,
		sqlWriter:      nil,
		writerCancel:   nil,
		pool:           sql.pool,
		metrics:        tree.metrics,
		maxWorkingSize: tree.maxWorkingSize,
		heightFilter:   tree.heightFilter,
		metricsProxy:   tree.metricsProxy,
		leafSequence:   leafSequenceStart,
		hashedVersion:  version,
		cache:          make(map[string][]byte),
		deleted:        make(map[string]bool),
	}

	if err := imTree.LoadVersion(version); err != nil {
		return nil, err
	}

	imTree.immutable = true

	return imTree, nil
}

func (tree *Tree) GetImmutableProvable(version int64) (*Tree, error) {
	// We can discard whole pool after usage
	pool := NewNodePool()

	sql := &SqliteDb{
		opts:        tree.sql.opts,
		pool:        pool,
		readPool:    tree.sql.readPool,
		metrics:     tree.sql.metrics,
		logger:      tree.sql.logger,
		useReadPool: true,
	}

	imTree := &Tree{
		sql:            sql,
		sqlWriter:      nil,
		writerCancel:   nil,
		pool:           sql.pool,
		metrics:        tree.metrics,
		maxWorkingSize: tree.maxWorkingSize,
		heightFilter:   tree.heightFilter,
		metricsProxy:   tree.metricsProxy,
		leafSequence:   leafSequenceStart,
		hashedVersion:  version,
		cache:          make(map[string][]byte),
		deleted:        make(map[string]bool),
	}

	if err := imTree.LoadVersion(version); err != nil {
		return nil, err
	}

	imTree.immutable = true

	return imTree, nil
}

func (tree *Tree) DiscardImmutableTree() error {
	tree.sql.leafWrite = nil
	tree.sql.treeWrite = nil
	tree.sql.read = nil
	tree.sql.readPool = nil
	tree.sql.pool = nil
	return tree.Close()
}

func (tree *Tree) VersionExists(version int64) (bool, error) {
	exists, err := tree.sql.HasRoot(version)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (tree *Tree) GetFromRoot(key []byte) ([]byte, error) {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	_, val, err := tree.root.get(tree, key)
	return val, err
}

func (tree *Tree) IteratorLeavesAt(version int64) (Iterator, error) {
	return tree.IteratorAt(version, nil, nil, true)
}

func (tree *Tree) Path() string {
	return tree.sql.opts.Path
}

func (tree *Tree) Revert(version int64) error {
	return tree.sql.Revert(version)
}

// markHashDirty marks the tree's hash as needing recalculation
// Call this function whenever the tree structure changes
func (tree *Tree) markHashDirty() {
	tree.hashedVersion = -1 // Invalidate hash by setting to impossible version
}

// adaptiveBatching groups tasks for better database locality and balanced load
func (tree *Tree) adaptiveBatching(tasks []nodeLoadTask, numWorkers int) [][]nodeLoadTask {
	if len(tasks) == 0 {
		return nil
	}

	// Sort tasks by version first, then sequence for better cache locality
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].nodeKey.Version() != tasks[j].nodeKey.Version() {
			return tasks[i].nodeKey.Version() < tasks[j].nodeKey.Version()
		}
		return tasks[i].nodeKey.Sequence() < tasks[j].nodeKey.Sequence()
	})

	// Calculate optimal batch size
	batchSize := len(tasks) / numWorkers
	if batchSize < 10 {
		batchSize = 10
	}

	// Create balanced batches
	batches := make([][]nodeLoadTask, 0, numWorkers)
	for i := 0; i < len(tasks); i += batchSize {
		end := min(i+batchSize, len(tasks))
		batch := make([]nodeLoadTask, end-i)
		copy(batch, tasks[i:end])
		batches = append(batches, batch)
	}

	return batches
}

// loadNodesBatched performs optimized batch loading with prepared statements
func (tree *Tree) loadNodesBatched(batches [][]nodeLoadTask, connPool []*SqliteReadConn) error {
	if len(batches) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errorChan := make(chan error, len(batches))
	numWorkers := min(len(connPool), len(batches))

	// Counter for progress tracking
	counter := &AtomicNodeCounter{}
	totalTasks := 0
	for _, batch := range batches {
		totalTasks += len(batch)
	}
	counter.setTotal(int64(totalTasks))

	// Process batches in parallel
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerIdx int, conn *SqliteReadConn) {
			defer wg.Done()

			// Process assigned batches
			for batchIdx := workerIdx; batchIdx < len(batches); batchIdx += numWorkers {
				batch := batches[batchIdx]
				if err := tree.processBatch(batch, conn, counter); err != nil {
					errorChan <- err
					return
				}
			}
		}(i, connPool[i])
	}

	wg.Wait()
	close(errorChan)

	// Check for errors
	for err := range errorChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// processBatch handles a single batch of loading tasks
func (tree *Tree) processBatch(batch []nodeLoadTask, conn *SqliteReadConn, counter *AtomicNodeCounter) error {
	// Group by node type for better query optimization
	leafTasks := make([]nodeLoadTask, 0, len(batch))
	branchTasks := make([]nodeLoadTask, 0, len(batch))

	for _, task := range batch {
		if isLeafSeq(task.nodeKey.Sequence()) {
			leafTasks = append(leafTasks, task)
		} else {
			branchTasks = append(branchTasks, task)
		}
	}

	// Process leaf nodes
	if len(leafTasks) > 0 {
		if err := tree.batchLoadNodes(leafTasks, conn, true); err != nil {
			return err
		}
	}

	// Process branch nodes
	if len(branchTasks) > 0 {
		if err := tree.batchLoadNodes(branchTasks, conn, false); err != nil {
			return err
		}
	}

	// Update progress counter
	counter.completed += int64(len(batch))

	return nil
}

// batchLoadNodes performs the actual batched database queries
func (tree *Tree) batchLoadNodes(tasks []nodeLoadTask, conn *SqliteReadConn, isLeaf bool) error {
	const maxBatchSize = 100 // SQLite parameter limit consideration

	for i := 0; i < len(tasks); i += maxBatchSize {
		end := min(i+maxBatchSize, len(tasks))
		batch := tasks[i:end]

		if err := tree.executeBatchQuery(batch, conn, isLeaf); err != nil {
			return err
		}
	}

	return nil
}

// executeBatchQuery runs optimized batch queries
func (tree *Tree) executeBatchQuery(tasks []nodeLoadTask, conn *SqliteReadConn, isLeaf bool) error {
	// For now, fall back to individual queries but with better batching
	// In a full implementation, we'd use prepared statements with batch parameters
	for _, task := range tasks {
		var node *Node
		var err error

		if isLeaf {
			node, err = conn.getLeaf(tree.pool, task.nodeKey)
		} else {
			node, err = conn.getNode(tree.pool, task.nodeKey)
		}

		if err != nil {
			return fmt.Errorf("error loading node %s: %w", task.nodeKey, err)
		}

		if node == nil {
			return fmt.Errorf("node not found: %s", task.nodeKey)
		}

		// Update parent node with loaded child
		if task.isLeft {
			task.parentNode.leftNode = node
		} else {
			task.parentNode.rightNode = node
		}
	}

	return nil
}

// Pipeline stage functions for pipeline processing
type pipelineStage struct {
	input  chan interface{}
	output chan interface{}
	worker func(interface{}) interface{}
}

func (ps *pipelineStage) run() {
	defer close(ps.output)
	for item := range ps.input {
		if result := ps.worker(item); result != nil {
			ps.output <- result
		}
	}
}
