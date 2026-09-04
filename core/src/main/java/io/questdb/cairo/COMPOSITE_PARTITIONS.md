# Composite partitions, modelled on parquet partitions

A partition can hold several PIECES over ONE set of column files. A composite partition is the parquet
FILE; a piece is a ROW GROUP. New rows go at the tail, untouched pieces stay put, and the geometry lives
in a `_geometry` file opened only when a query or commit lands on that partition.

Behind `cairo.o3.partition.merge.append.enabled`. OFF in production, ON by default in tests.

## What problem this solves

An out-of-order commit into an existing partition rewrites that partition into a fresh directory: every
row is copied, however few rows arrived. One backdated row into a day holding ten million costs ten
million row copies. That is the write amplification.

A composite partition writes only what it touches. The rows go at the tail of the files the partition
already has, and the pieces the commit did not touch keep their bytes exactly where they are - the new
geometry record simply carries their `(rowOffset, rowCount)` forward.

### When it pays

- **Continuous late data.** Corrections, a slow source, a device catching up after a network gap. Each
  such commit lands in one narrow time stride of an otherwise cold partition.
- **A WAL block whose transactions cluster.** Applying K transactions together, `WalTxnClusterer` bins
  the partition's time range, marks the strides any incoming transaction covers, and cuts at the edges
  of the COLD gaps. The gaps become pieces that are kept for free; only the hot strides are rewritten.
  The more the incoming work clusters, the less is copied.
- **Wide partitions.** The saving is proportional to what the commit does NOT touch, so it grows with
  partition size.

### When it does not

- **Pure in-order ingest.** There is no out-of-order work to avoid, and a merge-append table gives up
  WAL lag and the in-order block fast append (`tryFastAppendInOrderBlock`) - every commit goes through
  the merge-append path instead. Different path, nothing gained.
- **Backdated rows spread evenly.** With no cold gap wide enough to cut, the plan degenerates to one
  MERGE over the whole partition: the same copy a rewrite would have done, plus a geometry record.
- **Space, until compaction runs.** A superseded piece's bytes stay in the files as dead space. The
  partition is larger on disk than its live rows until a JOIN, MAKE-PLAIN, MOVE-TAIL or REWRITE folds
  it.
- **Reads.** One page frame per piece rather than one per partition, plus one `_geometry` resolve per
  touched composite partition.

## The geometry, drawn

Take a DAY partition holding 1000 rows, written in order. It is not composite: it has no `_geometry`
record at all, and its `_txn` record already says everything - one piece, at file row 0, spanning
everything.

```
file rows   0                                                           1000
            +-----------------------------------------------------------+
            | rows 0..999          00:00 -> 23:59                       |
            +-----------------------------------------------------------+
            implicit piece: tsLo=00:00  rowOffset=0  rowCount=1000   E=1000
```

### 1. Pre-split: manufacturing the structure, for free

A WAL block arrives carrying rows around 02:00 and around 20:00, and nothing between. The clusterer
reports 03:00 and 19:01 as cut points. Cutting is two more entries over the SAME bytes - nothing moves:

```
file rows   0              300                          900          1000
            +--------------+----------------------------+------------+
            | p0  (300)    | p1  (600)                  | p2  (100)  |
            +--------------+----------------------------+------------+
             00:00-02:59    03:00-19:00                  19:01-23:59
             off=0   HOT    off=300  COLD                off=900  HOT
```

A piece's `tsLo`/`tsHi` describe THE ROWS IT HOLDS, not a range it owns. The gap between two pieces
belongs to neither, which is what lets a later batch landing in it become a piece of its own.

### 2. Merge-append: the write

The plan is one action per piece, in timestamp order: **MERGE** p0 (50 incoming rows overlap it),
**KEEP** p1 (nothing arrived for it), **MERGE** p2 (20 incoming rows). Both merges are written at the
tail; `KEEP` copies nothing at all.

```
file rows   0          300                      900      1000          1350          1470
            +----------+------------------------+--------+-------------+-------------+
            | DEAD(300)| p1 KEEP (600)          | DEAD   | p0' (350)   | p2' (120)   |
            +----------+------------------------+--------+-------------+-------------+
                        untouched bytes          (100)    MERGE         MERGE

            E was 1000, is now 1470
```

`E` (physical rows) is grow-only: it is how far the files reach, dead space included. Live rows are the
sum of the piece row counts - here 350 + 600 + 120 = 1070, against E = 1470, so 400 rows are dead.

Copied: 370 rows plus the 70 incoming. A rewrite would have copied 1070.

### 3. Row numbering: two spaces

Pieces are stored ascending by `tsLo`, and a partition's rows are numbered cumulatively over them in
that order. So the LOGICAL order a query sees is not the FILE order:

```
logical     0                     350                          950                1070
            +---------------------+----------------------------+------------------+
            | p0'  00:00-02:59    | p1   03:00-19:00           | p2'  19:01-23:59 |
            +---------------------+----------------------------+------------------+
             file 1000..1350       file  300..900               file 1350..1470

piece   tsLo     rowOffset  rowCount   cumulativeLo   shift = rowOffset - cumLo
p0'     00:00      1000        350           0             +1000
p1      03:00       300        600         350                -50
p2'     19:01      1350        120         950               +400

file_row = logical_row + shift(piece) - columnTop
```

`shift` is SIGNED. p1 sits at a LOWER file row than the piece before it in timestamp order, because a
merge relocated p0 to the tail while p1 never moved. That is why interval scans go through
`CompositeTimestampFinder` in directory row order, and why a covering sidecar has to accept unsorted
timestamps.

### 4. `_geometry`: append-only full snapshots

```
partition directory
  ts.d  sym.d  ...          the column files, shared by every piece
  _geometry.0               [rec@0][rec@72][rec@136] ...  append-only
                                            ^
_txn slot 3 for this partition:  [composite flag | generation | byte offset ]
```

Every record is a FULL snapshot (56-byte header + 32 bytes per piece), never a delta, so a reader seeks
to the one offset `_txn` publishes and is done. The writer appends and syncs the record, THEN commits
`_txn` - a crash between the two leaves an unreferenced record, which is harmless. Bytes past the
committed offset are unreachable by construction. A generation rotates when a record would push the
file past its size cap.

`PartitionGeometry` resolves lazily: a table with no composite partition never opens the file, and a
query over one partition of a thousand opens one file.

## Storage

| Item | What it does |
|---|---|
| `_geometry` file | Append-only full snapshots: header + one `(tsLo, tsHi, rowOffset, rowCount)` per piece |
| `_txn` slot 3 | Composite flag + generation + byte offset of the live geometry record |
| `PartitionGeometry` | Lazy resolver, reads `_geometry` on first touch of a partition |
| Physical rows | How far the files reach, dead space included. Live rows is the sum of piece rows |

## Write

| Item | What it does |
|---|---|
| Pre-split | Cuts a piece in two. No bytes move - two records over the same files |
| Transaction clustering | Picks where to cut from the block's cold gaps, without reading rows |
| Planner | Per piece: KEEP (nothing written), MERGE (piece + batch out at the tail), NEW_PIECE, APPEND |
| Executor | Writes at the tail only, so one writer and any number of readers share the files |
| `FrameAlgebra.merge` | Fixed and var-size columns, plus column-top-aware kernels |
| Dedup | Deduplicating merge index; an all-duplicate commit with identical values writes nothing |
| Active partition | Left closed like parquet; when mapped, positioned at physical rows so a close trims nothing live |
| Non-WAL | Never founds one, but still handles a partition that is already composite |
| WAL lag | None, and no fast append through it either (`tryFastAppendInOrderBlock`): every commit is a merge-append |
| Replace commits | Fully implemented, including the table's own last partition: a piece fully inside the declared range is dropped (KEEP) or superseded (MERGE, rewritten to NEW_PIECE) |

## Read

| Item | What it does |
|---|---|
| Page frames | Maps `[0, physicalRows)`, one frame per piece |
| `CompositeTimestampFinder` | Interval scans in DIRECTORY row order - file order is not timestamp order |
| Indexes | Build, seal and rebuild cover the directory's whole `[columnTop, physicalRows)`, not one piece |
| Covering sidecar | Accepts unsorted timestamps, which a relocated piece produces |
| Covering index growth | Every write lands at the tail, so a covering POSTING index grows by one generation per commit, appended after the write with the rows' covered values (`O3PartitionJob.publishCoveredIndexesForAppend`) - the O3 worker does it for a composite plan, the seal sweep for the last partition's in-place append. Never a rebuild of the whole sidecar; entries of rows a MERGE or DROP retired stay, unreachable, as they would after a rebuild |

## Other

| Item | What it does |
|---|---|
| `ALTER COLUMN TYPE` | Converts the file's whole extent |
| `table_partitions()` | A composite directory reports its summed rows and disk size |
| Logging | One INFO line per merge-append: pieces before/after, keep/merge/newPiece, live and physical rows, dead % |

## Compaction of dead space

All four moves are built and on by default - see `PARTITION_COMPACTION.md` for the rules and
`PARTITION_COMPACTION_JOB_DESIGN.md` for the background job that drives them off the writer thread.
Cheapest first; the writer tries one partition per commit, and each move is its own transaction.

**JOIN** - pieces that are neighbours BOTH in timestamp order and in the files fold into one. Reads
nothing, writes nothing, changes no index: the survivor occupies exactly the file rows its parts did.
This is what a pre-split that never got written to costs to undo.

```
before
file rows   0            300                      900        1000
            +------------+------------------------+----------+
            | p0 (300)   | p1 (600)               | p2 (100) |
            +------------+------------------------+----------+

after
file rows   0                                                1000
            +------------------------------------------------+
            | p0 (900)    the very same bytes, one entry     |
            +------------------------------------------------+
```

**MAKE-PLAIN** - a single piece already at file row 0, with dead space above it, drops its geometry
record entirely and TRIM-FILES cuts the files down in the same transaction. The one move gated on the
txn scoreboard: an older reader may still resolve the record this shape came from.

```
before
file rows   0                    700                        1000
            +--------------------+--------------------------+
            | p0 (700)           | DEAD (300)               |
            +--------------------+--------------------------+

after   files trimmed, no _geometry record at all
file rows   0                    700
            +--------------------+
            | rows 0..699        |
            +--------------------+
```

**MOVE-TAIL** - the front piece is clean and big enough to be worth keeping where it is, so only the
tail pieces are copied, into a brand new sibling partition. The front's directory is untouched and its
`E` is deliberately left alone for MAKE-PLAIN to reclaim later. Declines when piece 0 does not start at
file row 0.

```
before
file rows   0                      800        1000           1120
            +----------------------+----------+--------------+
            | p0 (800)             | DEAD     | p1 (120)     |
            +----------------------+----------+--------------+

after   the front's directory, untouched
file rows   0                      800                       1120
            +----------------------+-------------------------+
            | p0 (800)             | DEAD, E left alone      |
            +----------------------+-------------------------+

        plus a fresh sibling partition, plain
file rows   0              120
            +--------------+
            | p1 (120)     |
            +--------------+
```

**REWRITE** - the fallback: copy every live row into a fresh directory, in timestamp order, and put the
old one on the remove-candidate list. This is what the running example above needs, because its first
piece no longer starts at file row 0.

```
before  the running example: 1070 live rows over 1470 file rows, and the
        first piece no longer at file row 0

after   a fresh directory
file rows   0                                        1070
            +----------------------------------------+
            | every live row, in timestamp order     |
            +----------------------------------------+
```

## Not done

- A floor on very small pieces (attempted, crashes)
- Piece-wise parquet conversion. A composite partition is folded to plain first
  (`TableWriter.compactPartitionToPlain`), as DETACH and SQUASH also do

## Downgrade caveat

Bit 61 of the `_txn` slot-3 word carries the composite flag, with no `META_FORMAT_MINOR_VERSION`
bump - the same way the parquet flag was introduced. An older binary therefore opens a table with
composite partitions without complaint and reads each one as flat `[0, liveRows)`, and its
checkpoint scrub zeroes the geometry pointer permanently.

Turning the flag back OFF on a current binary is safe: a fresh writer folds every composite partition
back to plain at open, in its own transaction, before it processes a row
(`TableWriter.foldCompositePartitionsWhenMergeAppendDisabled`).
