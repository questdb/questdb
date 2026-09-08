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
  above - only MAKE-PLAIN, which then runs TRIM-FILES behind a second check of its own (see below).

### What MOVE-TAIL leaves behind

MOVE-TAIL leaves the old folder **still composite**: one piece at row 0, `E` unchanged, dead space
above it. It is not cleaned up by copying again - it is cleaned up by waiting for readers:

```
MOVE-TAIL commits at T1
      |
      |  wait: no reader below T1        <- readers may still see the pieces MOVE-TAIL removed
      v
MAKE-PLAIN commits at T2                 (E -> row count, folder stops being composite)
      |
      |  wait: no reader below T2        <- readers below T2 still resolve the folder as composite,
      v                                     so they still map E rows of files this is about to cut
TRIM-FILES                               (files cut down to the row count)
```

TRIM-FILES needs its own, later wait, and it has to be taken AFTER MAKE-PLAIN's commit. A reader maps a
column up to the row count its *own resolved view* reports - `E` while the folder still resolves as
composite, the live row count once it resolves as plain (see `TableReader#mappedRowCount`) - never up to
the file's raw byte length. The folder starts resolving as plain only at T2, so a reader anywhere in
`[T1, T2)` still maps `E` rows, which is what the files still hold; cutting them to the row count leaves
that reader mapped past the end of a file, and it reads a zero offset out of the aux vector.

`isRangeAvailable` pushes the scoreboard's max txn to the value asked for, so taking the wait after the
commit is also what closes the race against a reader acquiring T2 concurrently: a reader that arrives
from there on takes T2 or newer and resolves the folder as plain.

Lowering `E` is safe on its own - a reader below T2 keeps reading the old geometry generation, which the
geometry purge retires behind its own wait - so the MAKE-PLAIN commit stands whether or not the second
wait clears. Only the byte reclaim is skipped, and the dead bytes are unreachable by then: the folder is
plain, every reader maps its live row count, and the next write to it truncates them off on close.

An earlier version of this design folded TRIM-FILES into MAKE-PLAIN's single check, on the argument that
once that check clears "no reader, old or new, can ever again resolve this folder as composite". That
step does not hold: it clears readers below T1, not readers already sitting in `[T1, T2)`.
`WalWriterFuzzTest#testWalWriteEqualTimestamp` hit it about once in twenty runs.

## Reader safety, the general principle

No compaction step ever writes below `E` (dangerously, into bytes a live reader might resolve): live
rows are always moved to a fresh location first (which nothing has ever pointed at, so it is safe to
write to without asking), and only afterward is `E` itself lowered or a file shortened - both pure
bookkeeping moves, gated on a check that no reader still needs the old state. MAKE-PLAIN and TRIM-FILES
take two separate checks, one commit apart (see above): lowering `E` waits for the readers that still see
the pieces MOVE-TAIL removed, and shortening a file waits for the readers that still resolve the folder
as composite and therefore still map `E` rows of it.
