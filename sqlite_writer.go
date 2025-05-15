package iavl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/eatonphil/gosqlite"
	"golang.org/x/sys/unix"
)

const pruneBatchSize = 10

type pruneSignal struct {
	pruneVersion int64
}

type saveSignal struct {
	batch   *sqliteBatch
	root    *Node
	version int64
}

type saveResult struct {
	n   int64
	err error
}

type sqlWriter struct {
	sql    *SqliteDb
	logger Logger

	treePruneCh chan *pruneSignal
	treeCh      chan *saveSignal
	treeResult  chan *saveResult

	leafPruneCh chan *pruneSignal
	leafCh      chan *saveSignal
	leafResult  chan *saveResult

	treeStopCh chan struct{}
	leafStopCh chan struct{}
}

func (sql *SqliteDb) newSQLWriter() *sqlWriter {
	writer := &sqlWriter{
		sql:         sql,
		leafPruneCh: make(chan *pruneSignal),
		treePruneCh: make(chan *pruneSignal),
		leafCh:      make(chan *saveSignal),
		treeCh:      make(chan *saveSignal),
		leafResult:  make(chan *saveResult),
		treeResult:  make(chan *saveResult),
		leafStopCh:  make(chan struct{}),
		treeStopCh:  make(chan struct{}),
	}
	if sql != nil {
		writer.logger = sql.logger
	}
	return writer
}

func (w *sqlWriter) start(ctx context.Context) {
	treeStarted := make(chan struct{})
	leafStarted := make(chan struct{})

	go func() {
		runtime.LockOSThread()
		unix.Setpriority(unix.PRIO_PROCESS, 0, -20)

		close(treeStarted)

		err := w.treeLoop(ctx)
		if err != nil {
			w.logger.Error("tree loop failed", "error", err)
			os.Exit(1)
		}
	}()
	go func() {
		runtime.LockOSThread()
		unix.Setpriority(unix.PRIO_PROCESS, 0, -20)

		close(leafStarted)

		err := w.leafLoop(ctx)
		if err != nil {
			w.logger.Error("leaf loop failed", "error", err)
			os.Exit(1)
		}
	}()

	<-treeStarted
	<-leafStarted
}

func (w *sqlWriter) awaitStop() {
	<-w.leafStopCh
	<-w.treeStopCh
}

func (w *sqlWriter) leafLoop(ctx context.Context) error {
	var (
		pruneVersion     int64
		nextPruneVersion int64
		orphanQuery      *gosqlite.Stmt
		deleteOrphan     *gosqlite.Stmt
		deleteLeaf       *gosqlite.Stmt
		pruneCount       int64
		pruneStartTime   time.Time
		err              error
	)

	defer func() {
		if orphanQuery != nil {
			if err := orphanQuery.Close(); err != nil {
				w.logger.Warn("stop sql writer, leaf orphan query", "err", err)
			}
		}

		if deleteOrphan != nil {
			if err := deleteOrphan.Close(); err != nil {
				w.logger.Warn("stop sql writer, leaf delete orphan", "err", err)
			}
		}

		if deleteLeaf != nil {
			if err := deleteLeaf.Close(); err != nil {
				w.logger.Warn("stop sql writer, leaf delete leaf", "err", err)
			}
		}

		close(w.leafStopCh)
	}()

	beginPruneBatch := func(pruneTo int64) error {
		if orphanQuery == nil {
			orphanQuery, err = w.sql.leafWrite.Prepare(`SELECT version, sequence, ROWID FROM leaf_orphan WHERE at <= ?`)
			if err != nil {
				return fmt.Errorf("failed to prepare leaf orphan query; %w", err)
			}
		}

		if deleteOrphan == nil {
			deleteOrphan, err = w.sql.leafWrite.Prepare("DELETE FROM leaf_orphan WHERE ROWID = ?")
			if err != nil {
				return fmt.Errorf("failed to prepare leaf orphan delete; %w", err)
			}
		}

		if deleteLeaf == nil {
			deleteLeaf, err = w.sql.leafWrite.Prepare("DELETE FROM leaf WHERE version = ? and sequence = ? LIMIT 1")
			if err != nil {
				return fmt.Errorf("failed to prepare leaf delete; %w", err)
			}
		}

		if err = w.sql.leafWrite.Begin(); err != nil {
			return fmt.Errorf("failed to begin leaf prune tx; %w", err)
		}
		if err = orphanQuery.Bind(pruneTo, pruneBatchSize); err != nil {
			return err
		}

		return nil
	}
	startPrune := func(startPruningVersion int64) error {
		pruneVersion = startPruningVersion
		pruneCount = 0
		pruneStartTime = time.Now()

		w.logger.Debug(fmt.Sprintf("leaf prune starting pruneTo=%d", pruneVersion))
		if err = beginPruneBatch(pruneVersion); err != nil {
			return err
		}
		return nil
	}
	commitPrune := func() error {
		if err = orphanQuery.Reset(); err != nil {
			return err
		}

		if err = w.sql.leafWrite.Commit(); err != nil {
			return err
		}

		w.logger.Debug(fmt.Sprintf("commit leaf prune count=%s", humanize.Comma(pruneCount)))
		if err = w.sql.leafWrite.Exec("PRAGMA wal_checkpoint(RESTART)"); err != nil {
			return fmt.Errorf("failed to checkpoint; %w", err)
		}

		if err = deleteLeaf.Reset(); err != nil {
			return err
		}

		if err = deleteOrphan.Reset(); err != nil {
			return err
		}

		return nil
	}
	stepPruning := func() error {
		hasRow, err := orphanQuery.Step()
		if err != nil {
			return fmt.Errorf("failed to step leaf orphan query; %w", err)
		}
		if hasRow {
			pruneCount++
			var (
				version  int64
				sequence int
				rowID    int64
			)
			err = orphanQuery.Scan(&version, &sequence, &rowID)
			if err != nil {
				return err
			}
			if err = deleteLeaf.Exec(version, sequence); err != nil {
				return err
			}
			if err = deleteOrphan.Exec(rowID); err != nil {
				return err
			}
			if pruneCount%pruneBatchSize == 0 {
				if err = commitPrune(); err != nil {
					return err
				}
				if err = beginPruneBatch(pruneVersion); err != nil {
					return err
				}
			}
		} else {
			if err = commitPrune(); err != nil {
				return err
			}
			w.logger.Debug(fmt.Sprintf("done leaf prune count=%s dur=%s to=%d",
				humanize.Comma(pruneCount),
				time.Since(pruneStartTime).Round(time.Millisecond),
				pruneVersion,
			))
			if nextPruneVersion != 0 {
				if err = startPrune(nextPruneVersion); err != nil {
					return err
				}
				nextPruneVersion = 0
			} else {
				pruneVersion = 0
			}
		}

		return nil
	}
	saveLeaves := func(sig *saveSignal) {
		res := &saveResult{}
		res.n, res.err = sig.batch.saveLeaves()
		if err = w.sql.leafWrite.Exec("PRAGMA wal_checkpoint(RESTART)"); err != nil {
			w.logger.Error("failed leaf wal_checkpoint", "error", err)
		}
		w.leafResult <- res
	}
	for {
		select {
		case sig := <-w.leafCh:
			if pruneVersion != 0 {
				if err = commitPrune(); err != nil {
					return fmt.Errorf("interrupt leaf prune failed in commit; %w", err)
				}
				saveLeaves(sig)
				if err = beginPruneBatch(pruneVersion); err != nil {
					return fmt.Errorf("interrupt leaf prune failed in begin; %w", err)
				}
			} else {
				saveLeaves(sig)
			}
		default:
			if pruneVersion != 0 {
				select {
				case sig := <-w.leafCh:
					if err = commitPrune(); err != nil {
						return fmt.Errorf("interrupt leaf prune failed in commit; %w", err)
					}
					saveLeaves(sig)
					if err = beginPruneBatch(pruneVersion); err != nil {
						return fmt.Errorf("interrupt leaf prune failed in begin; %w", err)
					}
				case sig := <-w.leafPruneCh:
					w.logger.Warn(fmt.Sprintf("leaf prune signal received while pruning version=%d next=%d", pruneVersion, sig.pruneVersion))
					nextPruneVersion = sig.pruneVersion
				case <-ctx.Done():
					return nil
				default:
					err = stepPruning()
					if err != nil {
						return fmt.Errorf("failed to step pruning; %w", err)
					}
				}
			} else {
				select {
				case sig := <-w.leafCh:
					saveLeaves(sig)
				case sig := <-w.leafPruneCh:
					err = startPrune(sig.pruneVersion)
					if err != nil {
						return fmt.Errorf("failed to start leaf prune; %w", err)
					}
				case <-ctx.Done():
					return nil
				}
			}
		}
	}
}

func (w *sqlWriter) treeLoop(ctx context.Context) error {
	var (
		nextPruneVersion int64
		pruneVersion     int64
		pruneCount       int64
		pruneStartTime   time.Time
		orphanQuery      *gosqlite.Stmt
		// TODO: support sharding
		// TODO: use map
		deleteBranch *gosqlite.Stmt
		deleteOrphan *gosqlite.Stmt
	)

	defer func() {
		if orphanQuery != nil {
			if err := orphanQuery.Close(); err != nil {
				w.logger.Warn("stop sql writer, tree orphan query", "err", err)
			}
		}

		if deleteBranch != nil {
			if err := deleteBranch.Close(); err != nil {
				w.logger.Warn("stop sql writer, tree delete branch", "err", err)
			}
		}

		if deleteOrphan != nil {
			if err := deleteOrphan.Close(); err != nil {
				w.logger.Warn("stop sql writer, tree delete orphan", "err", err)
			}
		}

		close(w.treeStopCh)
	}()

	beginPruneBatch := func(version int64) (err error) {
		if orphanQuery == nil {
			orphanQuery, err = w.sql.treeWrite.Prepare("SELECT version, sequence, at, ROWID FROM orphan WHERE at <= ?")
			if err != nil {
				return fmt.Errorf("failed to prepare orphan query; %w", err)
			}
		}
		if deleteBranch == nil {
			deleteBranch, err = w.sql.treeWrite.Prepare(fmt.Sprintf("DELETE FROM tree_%d WHERE version = ? AND sequence = ? LIMIT 1", defaultShardID))
			if err != nil {
				return fmt.Errorf("failed to prepare delete branch; %w", err)
			}
		}
		if deleteOrphan == nil {
			deleteOrphan, err = w.sql.treeWrite.Prepare("DELETE FROM orphan WHERE ROWID = ?")
			if err != nil {
				return fmt.Errorf("failed to prepare orphan delete; %w", err)
			}
		}

		if err = w.sql.treeWrite.Begin(); err != nil {
			return err
		}

		if err = orphanQuery.Bind(version, pruneBatchSize); err != nil {
			return err
		}

		return nil
	}
	commitPrune := func() (err error) {
		if err = orphanQuery.Reset(); err != nil {
			return err
		}

		if err = w.sql.treeWrite.Commit(); err != nil {
			return err
		}

		w.logger.Debug(fmt.Sprintf("commit tree prune count=%s", humanize.Comma(pruneCount)))
		if err = w.sql.treeWrite.Exec("PRAGMA wal_checkpoint(RESTART)"); err != nil {
			return fmt.Errorf("failed to checkpoint; %w", err)
		}

		if err = deleteBranch.Reset(); err != nil {
			return err
		}

		if err = deleteOrphan.Reset(); err != nil {
			return err
		}

		return nil
	}
	startPrune := func(startPruningVersion int64) error {
		w.logger.Debug(fmt.Sprintf("tree prune to version=%d", startPruningVersion))
		pruneStartTime = time.Now()
		pruneCount = 0
		pruneVersion = startPruningVersion
		err := beginPruneBatch(pruneVersion)
		if err != nil {
			return err
		}
		return nil
	}
	stepPruning := func() error {
		hasRow, err := orphanQuery.Step()
		if err != nil {
			return fmt.Errorf("failed to step orphan query; %w", err)
		}
		if hasRow {
			pruneCount++
			var (
				version  int64
				sequence int
				at       int
				rowID    int64
			)
			err = orphanQuery.Scan(&version, &sequence, &at, &rowID)
			if err != nil {
				return err
			}
			shard, err := w.sql.getShard(version)
			if err != nil {
				return err
			}
			if err = deleteBranch.Exec(version, sequence); err != nil {
				return fmt.Errorf("failed to delete from tree_%d count=%d; %w", shard, pruneCount, err)
			}
			if err = deleteOrphan.Exec(rowID); err != nil {
				return fmt.Errorf("failed to delete from orphan count=%d; %w", pruneCount, err)
			}
			if pruneCount%pruneBatchSize == 0 {
				if err = commitPrune(); err != nil {
					return err
				}
				if err = beginPruneBatch(pruneVersion); err != nil {
					return err
				}
			}
		} else {
			if err = commitPrune(); err != nil {
				return err
			}

			if err = w.sql.treeWrite.Exec("DELETE FROM root WHERE version < ?", pruneVersion); err != nil {
				return err
			}

			w.logger.Debug(fmt.Sprintf("done tree prune count=%s dur=%s to=%d",
				humanize.Comma(pruneCount),
				time.Since(pruneStartTime).Round(time.Millisecond),
				pruneVersion,
			))
			if nextPruneVersion != 0 {
				if err = startPrune(nextPruneVersion); err != nil {
					return err
				}
				nextPruneVersion = 0
			} else {
				pruneVersion = 0
			}
		}

		return nil
	}
	saveTree := func(sig *saveSignal) {
		res := &saveResult{}
		res.n, res.err = sig.batch.saveBranches()
		if res.err == nil {
			err := w.sql.SaveRoot(sig.version, sig.root)
			if err != nil {
				res.err = fmt.Errorf("failed to save root path=%s version=%d: %w", w.sql.opts.Path, sig.version, err)
			}
		}
		if err := w.sql.treeWrite.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
			res.err = fmt.Errorf("failed tree checkpoint; %w", err)
		}
		w.treeResult <- res
	}
	for {
		select {
		case sig := <-w.treeCh:
			if pruneVersion != 0 {
				if err := commitPrune(); err != nil {
					return err
				}
				saveTree(sig)
				if err := beginPruneBatch(pruneVersion); err != nil {
					return err
				}
			} else {
				saveTree(sig)
			}
		default:
			if pruneVersion != 0 {
				select {
				case sig := <-w.treeCh:
					if err := commitPrune(); err != nil {
						return err
					}
					saveTree(sig)
					if err := beginPruneBatch(pruneVersion); err != nil {
						return err
					}
				case sig := <-w.treePruneCh:
					w.logger.Warn(fmt.Sprintf("tree prune signal received while pruning version=%d next=%d", pruneVersion, sig.pruneVersion))
					nextPruneVersion = sig.pruneVersion
				case <-ctx.Done():
					return nil
				default:
					// continue pruning if no signal
					err := stepPruning()
					if err != nil {
						return err
					}
				}
			} else {
				select {
				case sig := <-w.treeCh:
					saveTree(sig)
				case sig := <-w.treePruneCh:
					err := startPrune(sig.pruneVersion)
					if err != nil {
						return err
					}
				case <-ctx.Done():
					return nil
				}
			}
		}
	}
}

func (w *sqlWriter) saveTree(tree *Tree) error {
	defer tree.metrics.MeasureSince(time.Now(), metricsNamespace, "db_write")
	batch := &sqliteBatch{
		sql:    tree.sql,
		tree:   tree,
		size:   200_000,
		logger: w.sql.logger,
		// logger: log.With().
		// 	Str("module", "sqlite-batch").
		// 	Str("path", tree.sql.opts.Path).Logger(),
	}
	saveSig := &saveSignal{batch: batch, root: tree.root, version: tree.version.Load()}
	w.treeCh <- saveSig
	w.leafCh <- saveSig
	treeResult := <-w.treeResult
	leafResult := <-w.leafResult
	tree.metrics.IncrCounter(float32(batch.leafCount), metricsNamespace, "db_write_leaf")
	tree.metrics.IncrCounter(float32(batch.treeCount), metricsNamespace, "db_write_branch")

	err := errors.Join(treeResult.err, leafResult.err)

	return err
}

// TODO
// unify delete approach between tree and leaf. tree uses rowid range in delete, leaf issues delete for each rowid.
// which one is faster?
//
