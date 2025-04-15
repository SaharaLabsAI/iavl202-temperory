package iavl

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/stretchr/testify/require"

	"github.com/cosmos/iavl/v2/testutil"
)

func Test_ExportImport(t *testing.T) {
	var err error

	tmpDir := t.TempDir()
	t.Logf("tmpDir: %s", tmpDir)
	require.NoError(t, err)
	opts := testutil.BigTreeOptions_100_000()
	opts.Until = 20
	opts.UntilHash = "0d4dfc4b6f6194f72da11fa254cf2910e54d330e8a4d6238af40e6b8d35ea77f"
	treeOpts := TreeOptions{CheckpointInterval: 10, HeightFilter: 1, StateStorage: true, EvictionDepth: 8}

	multiTree := NewMultiTree(NewDebugLogger(), tmpDir, treeOpts)
	itrs, ok := opts.Iterator.(*bench.ChangesetIterators)
	require.True(t, ok)
	storeKeys := itrs.StoreKeys()
	for _, sk := range storeKeys {
		require.NoError(t, multiTree.MountTree(sk))
	}
	_, err = multiTree.TestBuild(opts)
	require.NoError(t, err)
	require.Equal(t, opts.UntilHash, fmt.Sprintf("%x", multiTree.Hash()))

	exported := make(map[string][]*Node)

	// Export
	for sk, tree := range multiTree.Trees {
		exporter := tree.Export(PostOrder)
		require.NotNil(t, exporter)
		for {
			n, err := exporter.NextRawNode()
			if errors.Is(err, ErrorExportDone) {
				break
			}
			require.NoError(t, err)
			if n == nil {
				t.Errorf("nil node for %s", sk)
			}
			exported[sk] = append(exported[sk], n)
		}
	}
	for sk, nodes := range exported {
		require.Equal(t, int64(len(nodes)), (multiTree.Trees[sk].root.size*2)-1)
	}

	importDir := t.TempDir()
	multiTree = NewMultiTree(NewDebugLogger(), importDir, treeOpts)
	for _, sk := range storeKeys {
		require.NoError(t, multiTree.MountTree(sk))
	}
	for sk, tree := range multiTree.Trees {
		importer, err := newImporter(tree, opts.Until)
		require.NoError(t, err)
		for _, n := range exported[sk] {
			require.NoError(t, importer.Add(n))
		}
		require.NoError(t, importer.Commit())
	}
	require.Equal(t, opts.UntilHash, fmt.Sprintf("%x", multiTree.Hash()))
}
