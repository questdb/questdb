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
| **piece count** | the folder (or its logical partition) has too many pieces |
| **age** | idle past a timeout, and still has waste or more than one piece |
| **table pressure** | the whole table's dead-row percentage crosses a high-water mark AND the absolute dead bytes clear a minimum floor (or the absolute dead bytes alone cross a much higher ceiling); picks the oldest wasteful folder first, and keeps compacting until a lower low-water mark is reached |

## How compaction works

Four ways to reclaim a folder's waste, cheapest first:

| name | what it does | reader check needed |
|---|---|---|
| **JOIN** | merges pieces already adjacent in the files into one piece; copies nothing | no |
| **MOVE-TAIL** | copies only the messy tail pieces into a new sibling folder, leaving the clean front untouched | no |
| **MAKE-PLAIN** | lowers `E` to the row count so the folder stops being composite; no bytes move | yes |
| **TRIM-FILES** | shortens every column and index file down to the live size, giving the dead bytes back to the filesystem | yes, a second, later wait |
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
  above - only MAKE-PLAIN, and later TRIM-FILES.

### What MOVE-TAIL leaves behind

MOVE-TAIL leaves the old folder **still composite**: one piece at row 0, `E` unchanged, dead space
above it. It is not cleaned up by copying again - it is cleaned up by waiting for readers, in two
separate steps:

```
MOVE-TAIL commits at T1
      |
      |  wait: no reader below T1        <- readers may still see the pieces MOVE-TAIL removed
      v
MAKE-PLAIN commits at T2  (E -> row count, folder becomes a plain partition)
      |
      |  wait: no reader below T2        <- readers may have mapped the old, larger E
      v
TRIM-FILES  (cut every column and index file down to the live size)
```

The two waits cannot be collapsed into one: a reader that opened between MAKE-PLAIN and TRIM-FILES
still has the old, larger `E` mapped even though it only reads up to the row count, and shortening the
files under it would leave it mapped past the end of a file.

## Reader safety, the general principle

No compaction step ever writes below `E` (dangerously, into bytes a live reader might resolve): live
rows are always moved to a fresh location first (which nothing has ever pointed at, so it is safe to
write to without asking), and only afterward is `E` itself lowered or a file shortened - both pure
bookkeeping moves, gated on a check that no reader still needs the old state. Shortening a file is
riskier than merely leaving space marked dead, which is why it is split into its own step (TRIM-FILES)
with its own, later check.
