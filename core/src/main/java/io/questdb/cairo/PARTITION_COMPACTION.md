# Compaction: reclaiming wasted space in composite partitions

Original design doc (condensed - idea and mechanism only, implementation detail and rationale
stripped). Ported from the enterprise `feat-partition-top-split` branch.

## The problem

Merge-append never overwrites. When it rewrites a hot piece, it appends the new copy at the end of
the folder's column files and abandons the old copy where it lay. The abandoned rows stay on disk
forever. A folder's size grows with how many times it has been merged, not with how many rows it
actually holds. Nothing cleans that up.

## Compaction is not squash

Squash turns a logical partition into exactly one piece, right now - a fixed target, used by DETACH,
ATTACH, parquet conversion and the partition switch. Compaction works on one physical folder at a
time, decided by waste rather than piece count, and can tidy up one folder while leaving others alone.

## Vocabulary

- **folder** - one physical partition: one directory, one `(dirTs, nameTxn)`, one set of column files,
  one geometry chain.
- **piece** - a window onto part of a folder's files. Several pieces can share one folder.
- **`E`** - how far the folder's files have ever been written, in rows. Only goes up.
- **dead rows** - rows in the files no piece points at any more: `E` minus live rows.

## When to compact

Checked every commit, per folder. A folder is a candidate only if it is composite (more than one
piece, or its one piece does not start at row 0) and is not the table's last (active) logical
partition - compacting the active partition would fight with the writer's own append state.

A folder with exactly one piece already at row 0 is never a candidate for the four rules below: every
byte alive in it is already at the front, so nothing about it needs copying. That shape goes straight
to MAKE-PLAIN instead.

| rule | fires when |
|---|---|
| **waste ratio** | dead rows exceed a ratio of live rows AND a minimum size |
| **piece count** | the folder (or its logical partition) has too many pieces - the cap is `max(piece.threshold, liveRows / avg.rows.piece.lim)`, never below the flat floor but scaled up for a large folder, since a piece's read cost is a fixed amount per piece regardless of folder size |
| **age** | idle past a timeout, and still has waste or more than one piece |
| **table pressure** | the whole table's dead-row percentage crosses a high-water mark AND the absolute dead bytes clear a minimum floor (or the absolute dead bytes alone cross a much higher ceiling); picks the oldest wasteful folder first, and keeps compacting until a lower low-water mark is reached |

## How compaction works

Four ways to reclaim a folder's waste, cheapest first:

| name | what it does | reader check needed |
|---|---|---|
| **JOIN** | merges pieces already adjacent in the files into one piece; copies nothing | no |
| **MOVE-TAIL** | copies only the messy tail pieces into a new sibling folder, leaving the clean front untouched | no |
| **MAKE-PLAIN** | lowers `E` to the row count so the folder stops being composite; no bytes move | yes |
| **TRIM-FILES** | shortens every column file down to the live size, giving the dead bytes back to the filesystem | yes, its OWN check, after MAKE-PLAIN's commit - see below |
| **REWRITE** | copies every live row into a fresh folder and deletes the old one | no (the delete has its own check) |

Two pieces can only be merged (JOIN) or copied together (MOVE-TAIL, REWRITE) if they are neighbours in
the folder's own piece list - a piece's range ends where the next piece in the list begins, not
wherever the data happens to sit in the files.

**Choosing which one runs**, once a folder qualifies:

- If pieces are adjacent in the files, JOIN them first, always - it's free, and it may already leave
  the folder simple enough that nothing else is needed.
- If a clean front survives (first piece at row 0, a real share of the live rows) and there is a messy
  tail, MOVE-TAIL splits the tail off into its own folder rather than recopying the whole thing.
- Otherwise, REWRITE copies everything live into a fresh folder.
- A folder already reduced to one piece at row 0, with real dead space above it, needs none of the
  above - only MAKE-PLAIN, which runs TRIM-FILES in the same call (see below).

### What MOVE-TAIL leaves behind

MOVE-TAIL leaves the old folder **still composite**: one piece at row 0, `E` unchanged, dead space
above it. It is cleaned up by MAKE-PLAIN, which lowers `E` to the row count and stops the folder being
composite, and by TRIM-FILES, which cuts the files down to that row count in the same call:

```
MOVE-TAIL commits at T1
      |
      |  wait: no reader below T1        <- readers below T1 still resolve the pieces MOVE-TAIL removed,
      v                                     which sit higher up the files than the one at row 0
MAKE-PLAIN commits at T2                 (E -> row count, folder stops being composite)
      |
      v
TRIM-FILES                               (files cut down to the row count - no wait of its own)
```

TRIM-FILES needs no wait of its own because **a reader maps a folder's column files only as far as its
highest live piece reaches** - `max(rowOffset + rowCount)` over the pieces of the geometry record it
resolved, never `E` (see `PartitionGeometry#getLiveFileExtent` and `TableReader#mappedRowCount`). Nothing
resolves a row outside a piece, so the dead space between the last live piece and `E` is bytes no reader
can ask for. Once MAKE-PLAIN's own check has cleared - no reader below the transaction that published the
current, one-piece record - every live and arriving reader resolves either that record or the plain folder
MAKE-PLAIN just committed, and both map exactly the live row count. That is precisely what TRIM-FILES cuts
to.

`isRangeAvailable` pushes the scoreboard's max txn to the value asked for, so a reader arriving after the
check takes that transaction or newer, and therefore resolves one of those two shapes.

An earlier version of this design had readers map `E` for any composite folder. TRIM-FILES then needed a
second wait, one transaction after MAKE-PLAIN's commit, for the readers sitting in `[T1, T2)` that still
mapped `E` rows of the files it was about to shorten - `WalWriterFuzzTest#testWalWriteEqualTimestamp` hit
that about once in twenty runs when the two were folded into one check. The cost of the second wait was
that it could not decide whether to commit: when it did not clear, the folder was recorded as plain with
its dead bytes still on disk, and `deadRows` then read 0, the folder was no longer composite, and
MAKE-PLAIN never selected it again - a leak measured at 62 KB of a 328 KB partition. Mapping to the live
extent removes the wait and the leak together.

### What a checkpoint blocks

A running CHECKPOINT is not a reader and the scoreboard check above does not stand in for it. Backup sizes
a folder's column files by the physical row extent `E` its manifest copied out of the checkpoint, then
reads those files from the LIVE db root - so TRIM-FILES must not shorten them while one is running
(`TableWriter#processPartitionRemoveCandidates0` defers partition removal for the same reason).

**A folder that cannot be trimmed must not be recorded as plain**, or its dead bytes are left with nothing
to report them and nothing to reclaim them. So MAKE-PLAIN declines outright while a checkpoint is in
progress, and the ordinary decline/backoff bookkeeping brings the folder back once the checkpoint is
released - still composite, still one piece, still reporting its dead rows.

## Reader safety, the general principle

No compaction step ever writes below `E` (dangerously, into bytes a live reader might resolve): live
rows are always moved to a fresh location first (which nothing has ever pointed at, so it is safe to
write to without asking), and only afterward is `E` itself lowered or a file shortened - both pure
bookkeeping moves, gated on a check that no reader still needs the old state. MAKE-PLAIN and TRIM-FILES
share ONE check (see above): lowering `E` and shortening a file both wait for the readers that still see
the pieces MOVE-TAIL removed, and nothing else - a reader maps only as far as its highest live piece
reaches, so the bytes TRIM-FILES cuts are already unreachable for every reader of the current shape.
