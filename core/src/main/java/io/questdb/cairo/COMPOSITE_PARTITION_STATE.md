# Composite partitions: implementation state

Running record of what is BUILT, what is DECIDED, and where the work stands. Update this file in the same
commit as the change it describes.

Branch `lazy-geometry2`, worktree `questdb-wt-lazygeom2`, based on `f8cf9e468e` - the head of `master`.

## The goal

A partition can be several PIECES over one set of column files. Incoming rows are appended at the tail, the
pieces they do not touch are left exactly where they are, and the partition's geometry lives in a
`_geometry` file inside its own directory. `_txn` carries a pointer to it.

The model is parquet's, throughout: a composite partition is the parquet FILE, a piece is a ROW GROUP.
`_geometry` opens only when a query or a commit lands on THAT partition, exactly as row-group metadata is
read when a parquet partition is scanned and not at load.

## Why this is a second attempt

The first attempt (`questdb-wt-lazygeom`, branch `lazy-geometry`) built the same feature by growing it out
of an existing partition-split implementation. That left an expansion step in `TxReader` - every `_txn`
load read every composite partition's `_geometry` and spliced the results into one flat piece list - and a
piece-indexed API spread across ~160 call sites in `TableWriter` alone. Removing the expansion meant
converting all of them first, so the lazy read could never actually land.

This tree starts from clean `master` and adds only what is needed, in dependency order. **The expansion is
deliberately not ported.** `attachedPartitions` stays exactly as master has it: one 4-long record per
partition. Every index in the new code is the ordinary partition index, and the word "piece" appears only
inside `PartitionGeometry`, the planner, the executor and the two frame cursors that consume it.

## Status

No known issue, tread any new tests failure as an unknown new problem and drive the investigation to the end.
Do not track buf fixes in this state file