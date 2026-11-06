# QWP cross-table commit batching, simplified

This document describes the simplified design of the QWP cross-table commit batch of PR #8195, as implemented on
branch `jh_qwp_cross_table_commit_batch_simplified` on top of the PR head `a82a4afe95`. It started as a proposal;
section 16 lists where the code disagreed with the proposal and how the design changed, and section 15 records the
decisions on the questions the proposal left open. Methods are named rather than line numbers.

## 1. Motivation

A QWP client sends rows over one WebSocket connection as a stream of binary frames. A frame holds one block per
table it writes to. The server answers with cumulative acks: an OK ack for sequence N tells the client that every
row of the frames up to N is committed, so the client may drop those frames from its store-and-forward buffer.
After an error or a reconnect, the client replays everything above the last OK ack.

Every commit costs the same fixed work, whatever its row count: a sequencer transaction, a transaction-log record,
a step of the ack watermark and one unit of work for the WAL apply job. A frame with one row cannot amortize it.

master batches commits only within one table, so a client that cycles its rows through several tables commits once
per frame. The PR lifts the one-table rule, caches schema parses and column mappings per table, and sends acks
larger than the send buffer in pieces. This design keeps those gains and removes the PR's largest piece of
machinery: the redo, which rolled a whole batch back and decoded its frames a second time when one frame failed after
it appended rows next to the batch's.

The redo existed because `WalWriter` could roll back only to the start of its transaction. A writer that can roll
back to a savepoint inside the transaction lets the failing frame undo its own blocks. The batch then commits as it
stands, and the frame is answered the way it would be without a batch. That removes the redo, the per-frame record
the redo read, the second decode, the rule that a redefined symbol dictionary ends a batch, and a test hook that
pinned that rule.

## 2. The design in brief

- The commit batch holds plain frames of any tables from one receive pass. Each table's rows go to its own WAL
  writer, and each table commits once: when the pass ends, or at the first frame that cannot join.
- Before a frame's first block touches a table's writer, the batch marks a savepoint on that writer. A frame that
  fails or cannot join rolls every writer it touched back to its savepoint, and the batch commits as it stands.
  There is no redo.
- When that rollback fails, or the frame's failure distressed the table cache, the batch cannot commit. It is
  abandoned with nothing committed, and the NACK names the failed frame, so a failure that recurs on every replay
  keeps striking that frame and not a healthy one.
- Schema parses and column mappings are cached per table, on the table's cache entry. A table switch costs a
  lookup, not a parse.
- QuestDB has no transaction across tables: each table commits through its own sequencer. The batch checks
  everything that would refuse a commit before it commits anything, and it places the ack so that a failure in the
  middle of the batch never acknowledges an uncommitted row. Such a failure can duplicate rows, but it cannot lose
  them.
- An ack larger than the send buffer is staged in connection memory and sent in pieces.

## 3. Terms

| Term | Meaning |
|---|---|
| receive pass | One call of `processWebSocketFrames`: the bytes one `recv` delivered, at most `http.recv.buffer.size` (2 MiB by default) |
| frame | One complete binary WebSocket message: one QWP message with zero or more table blocks |
| sequence | The server's number for each binary frame of a connection; the unit of the cumulative ack |
| watermark | `highestProcessedSequence`, the sequence the next OK ack names |
| batch entry | One table of the batch (`CommitBatchEntry`): its writer, the rows it holds for the batch, the sequence of the first frame that gave it rows, the sequence of the frame that last marked its savepoint (`frameSequence`), and the rows the entry held when that frame marked it (`frameStartRows`) |
| savepoint | The state a `WalWriter` restores on `rollbackToSavepoint`: its uncommitted row count and its transaction's timestamp bounds and out-of-order flag at the time of `markSavepoint` |
| boundary | A frame that cannot join; the batch commits as it stands before the frame is answered |
| lost batch | A batch that cannot commit because the table cache is distressed; it is abandoned (4.5) |

## 4. Batch lifecycle

### 4.1 Joining

`QwpIngressUpgradeProcessor.handleBinaryMessage` decodes each frame (`QwpIngressProcessorState.prepareMessage`)
and calls `joinCommitBatch(seq)`, which returns whether the frame joined. With a batch pending, the frame tries to
join it; with none pending, it tries to open one. `joinCommitBatch` takes the prepared first block, so the block is
consumed, and walks the frame's blocks:

- A block of a table already in the batch takes no table lookup (`findCommitBatchEntry`). It joins when the table's
  structure version in its `SeqTxnTracker` still equals the writer's metadata version, the table is not dropped, and
  the table's rows stay below the cap (`isCommitBatchEntryJoinable`). Before the frame's first append to this
  writer, `joinCommitBatch` marks the savepoint: when `entry.frameSequence != seq`, it calls
  `entry.writer.markSavepoint()` and sets `entry.frameSequence = seq` and `entry.frameStartRows = entry.rows`. A
  second block of the same table in the same frame finds `frameSequence == seq` and does not mark again, so the
  savepoint stays before the frame's first block. The block is appended with column creation refused:
  `QwpWalAppender.appendToWalStreaming(..., false)` returns false when the block names a column the writer lacks,
  before any row is appended, and the frame cannot join.
- A block of a table new to the batch takes the normal lookup, `QwpTudCache.getTableUpdateDetails`, which may
  create the table and its columns. The block must stay below the cap, and the table must pass the join-time checks
  of section 5. `joinCommitBatch` asserts the writer holds no uncommitted rows, calls `markSavepoint()` on it, appends
  the block, and creates the entry through `addCommitBatchEntry`, with `firstSequence` and `frameSequence` set to
  this frame's sequence and `frameStartRows` to 0. A second block of the same table in the same frame finds the
  entry and does not mark again.
- A block without rows joins nothing. For a table outside the batch it still takes the lookup, as on the normal
  path.

The cap is `qwp.max.uncommitted.rows` (1,000,000 by default). Below it, the appender never commits a table's rows on
its own, which would break the batch's row accounting.

A frame that joins gets no ack until the batch commits. `joinCommitBatch` returns true. Nothing is recorded about
the frame beyond `lastCommitBatchSequence`.

### 4.2 What ends a batch

The batch commits:

- when the receive pass ends, after the frame loop of `processWebSocketFrames`;
- before a frame that can never join: a deferred-commit frame, a durable-ack poll, or a frame shorter than the QWP
  header (`isCommitBatchCandidate`);
- before a control frame, a fragment or an oversized frame;
- at a boundary frame (4.4).

A batch never outlives its pass. If one is still pending when the frame loop exits, the loop's finally abandons it
and logs at critical level. A disconnect without a CLOSE abandons the batch instead of committing it: no ack could
reach the client, so a commit would only turn its replay into duplicates.

While a batch is pending, every uncommitted row of the connection belongs to it. A batch and a deferred-commit
group never coexist: no batch opens while deferred rows are uncommitted or a rowless deferred frame is withheld.

### 4.3 Committing

`commitBatch` takes the engine's role-switch read lock, holds it across all commits, and then:

1. Runs the pre-commit checks of section 5 for every entry. A refusal refuses the whole batch with nothing
   committed.
2. Commits the entries one at a time, in the order they joined, through `QwpTudCache.commitBatch`. Before each
   commit, `isCommitBatchEntryIntact` checks that the table cache is healthy and that the writer holds exactly the
   entry's rows. It is a positive check that nothing else committed or discarded them, and that a rewind left
   exactly the rows the entry had before the rewound frame, not a report from each path that could have.
3. On success, moves the watermark to the batch's last sequence. Each table's seqTxn goes into the ack and the
   durable-ack bookkeeping (`recordCommittedTable`).

### 4.4 Frames that cannot join

`joinCommitBatch` returns false for a frame that failed or cannot join, and before it returns, it puts the batch
back where it was before the frame touched a writer. Every exit inside its try goes through
`leaveCommitBatch(seq)`, which runs both steps below; its catch, for a frame that threw, runs only the first:

1. `rewindFrame` walks the batch entries from the last to the first. For each entry with `frameSequence == seq` it
   calls `entry.writer.rollbackToSavepoint()` and sets `entry.rows = entry.frameStartRows`; an assertion checks that
   the writer now holds exactly that many rows, and the commit-time check of 4.3 checks it again for the entries
   that stay. An entry with `firstSequence == seq` was created by this frame; its savepoint is zero rows, and
   `rewindFrame` clears it and drops it from the batch. Entries this frame created are a contiguous tail of the list,
   because `addCommitBatchEntry` appends in join order.
2. A frame that is still OK only could not join. `leaveCommitBatch` calls
   `preparedMessageCursor.resetTableIteration()`, which moves the message cursor back to the frame's first table block
   without touching the delta symbol dictionary, and sets `preparedTableBlock` from `nextTable()` again. It runs inside
   `joinCommitBatch`'s try, so a parse failure there goes to its catch, which fails the frame and calls `rewindFrame`
   again; the second rewind finds every writer at its savepoint and changes nothing. A frame that only could not join
   stopped at a block, so it has a first block to restart at. The normal path can then take the
   frame from its first block. A frame that failed is not prepared again: the normal path does not process a failed
   frame, and a parse failure has already released the message cursor (`rejectMessageError` calls
   `QwpStreamingDecoder.releaseCachedResources`).

A rewind that throws leaves rows the batch cannot vouch for. `rewindFrame` then logs at critical level, calls
`tudCache.setDistressed()` and returns; it does not rewind the remaining entries, because the distressed cache's
next `clear()` frees every writer and rolls everything back. A frame that was still OK fails with
`INTERNAL_ERROR` ("could not rewind the frame's rows"), so that the normal path never runs on a writer the failed
rewind distressed. `rollbackToSavepoint` refuses a writer left mid columnar write by a failed cancel and a distressed
writer, and distresses the writer when its own rewind fails.

`handleBinaryMessage` treats every false return the same way:

- With a batch pending, it captures the frame's error, if any (`responseStatusOf`, `getErrorText`,
  `isRoleChangeClosePending`). When `isCommitBatchIntact()` is false, the batch is lost (4.5). Otherwise it clears
  the error with `clearFrameErrorForBatchCommit` and commits the batch. If the commit fails, the frame is not
  processed: the finally's `clear()` discards it and the client replays it behind the batch's failed frame. If the
  commit succeeds and the frame had failed, the frame gets its captured answer. If the commit succeeds and the frame
  only could not join, `handleBinaryMessage` calls `joinCommitBatch` once more, now with no batch pending, so the
  frame opens a new batch when it can.
- With no batch pending, a frame that cannot join goes the normal way: `processPreparedMessage` from the first
  block, then `commit`, or its own error. A frame whose failure distressed the cache gets its own NACK, and
  `clear()` frees the writers, as on master.

The two calls of `joinCommitBatch` keep the structure `handleBinaryMessage` had, as an unrolled loop.

**A table new to the batch whose cancel fails.** A table new to the batch gets its entry only after its append
returned. When that append throws and the appender's `cancelColumnarWrite` also fails, the writer is left mid
columnar write without an entry, and `rewindFrame` does not see it. The frame has failed (the cancel's exception,
usually a `CairoException` from a failed mapping, which does not distress the cache), and the writer holds none of
the batch's rows: its uncommitted row count is zero, because `finishColumnarWrite` never ran. The batch therefore
commits without it, and the frame gets its own NACK. The frame's `clear()` then calls the writer's `rollback()`,
which refuses a writer mid columnar write; `QwpTudCache.clear` distresses itself on that refusal and frees every
writer, and closing the writer cancels the columnar write again. The catch of `joinCommitBatch` documents this and
asserts that the writer holds no uncommitted rows.
`QwpIngressCommitBatchTest#testNewTableCancelFailureCommitsBatchWithoutIt` drives it: the batch commits once, the
frame is NACKed with the mapping failure, the writer is mid columnar write when the frame's `clear()` frees it, and
the table takes rows on a fresh connection without any of the failed frame's.

A frame whose delta symbol dictionary redefines existing ids joins like any other. The rule that made it a boundary
protected the redo, which decoded earlier frames again; rows in the writers hold table symbol keys, not client ids,
and `prepareMessage` already clears the connection's `ConnectionSymbolCache` on a redefinition. The
`onSymbolDictApplied` hook, which existed to pin that rule's order against the distressed-cache check, is gone.

### 4.5 A lost batch

The table cache distresses when a frame's failure leaves the connection's writers in doubt: `rejectMessageError`
distresses it for a `CommitFailedException` and for any throwable that is not a `QwpParseException` or a
`CairoException`, and `rewindFrame` distresses it when a rewind throws. A distressed cache commits nothing
(`isCommitBatchEntryIntact`), so the batch cannot commit.

`handleBinaryMessage` then does what master did for a lost one-table batch: it does not call `commitBatch`, logs
`WebSocket commit batch lost with a failed frame`, calls `abandonCommitBatch`, which pins the watermark below the
batch's first frame and leaves the cache distressed, and answers the failed frame with `STATUS_INTERNAL_ERROR` and
the frame's own error text. Nothing of the batch commits; the client replays it from its acked watermark. The
pending ack below the batch goes out before the error, through `rejectFrame`. A captured role-change close is kept
instead of the NACK; it does not occur together with a distressed cache, because the read-only refusals do not
distress it, but it is not dropped.

The NACK names the failed frame, not the batch's first frame, because the pinned Java client
(`CursorWebSocketSendLoop.recordHeadRejectionStrike`) keys its poison-frame strikes on the frame a NACK names and
halts with `PROTOCOL_VIOLATION`, naming that frame, after `max_frame_rejections` strikes (4 by default). A
distressing failure can recur on every replay, for example a writer acquisition that fails with an unexpected error
for one table. Naming the batch's first frame would make the client strike, and eventually halt on, a healthy frame,
while naming the failed frame strikes the frame that fails. A transient fault clears on the replay, which then
commits the batch once.

### 4.6 Partial failure and the ack

Commits stop at the first table that fails. Tables join in the order of their first frames, so the frames before
the failed table's first frame hold rows only of tables that committed. `onCommitBatchFailed` moves the watermark to
just before that frame, and the NACK names it. The client replays from there.

Rows that the replayed frames added to tables that did commit are now in the WAL twice. Frames a1, b1, a2, b2 join
one batch; a commits and b fails. The ack covers a1, the client replays b1, a2 and b2, and a2 lands twice. When
every frame carries both tables, as the Java client's combined frames do, both tables join at the first frame.
Nothing is acked then, and the committed table receives the whole pass again.

The batch trades loss for duplication, bounded by one receive pass. Section 5 narrows when it can happen. This is
unchanged from PR #8195.

## 5. Refusal checks and the role-switch lock

A failure between two tables' commits duplicates rows, so the batch refuses up front whatever it can know in
advance. The two checks below are kept as they are in PR #8195; each has a purpose the other cannot serve.

When a table joins, `TableSequencerAPI.throwIfCommitRefused` checks that the table token still matches its
sequencer, that the table is not dropped, and that it is not hard-suspended while
`cairo.wal.apply.suspended.write.denied` is on. `TableUpdateDetails.authorizeCommit` checks insert permission. A
refused table never joins: its frame ends the batch, the frames before it commit, and the frame gets the refusal.
This is what makes a deterministic refusal land on the refused table's own frame. Routed through the pre-commit
check instead, the refusal would NACK the batch's first frame, which belongs to a healthy table, and the client's
replay would meet the same refusal at the same place every time.

Before the first commit, `commitBatchEntries` checks read-only mode, each entry tracker's dropped flag, and the two
checks above for every entry. A refusal here refuses the whole batch, NACKed at the first frame of its first table,
and turns a refusal that landed inside the pass window into a clean replay instead of a partial commit. It costs
one sequencer read lock per table per batch, not per frame. The tracker's dropped check stays although the
sequencer's `checkDropped` would also throw: `commitBatchEntries` answers a dropped table with a non-critical
exception, which `cairoExceptionStatus` maps to `STATUS_WRITE_ERROR`, while `CairoException.tableDropped` is
critical and would map to `STATUS_INTERNAL_ERROR`. `QwpIngressCommitBatchTest#testDroppedTableRefusesWholeBatch`
pins the status.

`throwIfCommitRefused` repeats the refusals that `nextTxn` raises before it appends a transaction.
`WalWriterTest#testCommitRefusalCheck*` pins that the two agree for a live, dropped, renamed and suspended table.

The role-switch read lock is held across all the batch's commits, so an in-place demote cannot land between two
tables. `RoleSwitchReadWriteLock` lets a thread take the read lock again while it holds it, and
`TableUpdateDetails.commit` does so for each table. A demote waits for the whole batch, not for one table.

What remains is a storage fault, or a DROP, RENAME or SUSPEND WAL that lands after the checks and before the table's
commit.

## 6. The WalWriter savepoint

### 6.1 What a block append changes in the writer

`QwpWalAppender.appendToWalColumnar` drives one block through `WalColumnarRowAppender`: `beginColumnarWrite`
records `startRowId = segmentRowCount`, the column puts append to each column's data and aux memory and set
`rowValueIsNotNull[column]` to the last row written, and `endColumnarWrite` calls `WalWriter.finishColumnarWrite`,
which null-fills every column that was not written up to `segmentRowCount + rowCount - 1`, widens
`txnMinTimestamp` and `txnMaxTimestamp`, sets `txnOutOfOrder` when the block's minimum lies below the previous
maximum or the block itself is out of order, and advances `segmentRowCount`. On failure the appender calls
`cancelColumnarWrite`, which rewinds every column to `startRowId` through `WalWriter.setAppendPosition` and leaves
`segmentRowCount` and the timestamp bounds untouched. When the cancel itself throws, its exception replaces the
append's, and `WalColumnarRowAppender` stays in columnar write.

Symbol puts add keys to the writer's in-memory per-column maps (`symbolMaps`, `localSymbolIds`) through
`resolveSymbol`, and a null symbol sets `symbolMapNullFlags` and `symbolMapNullFlagsChanged` through
`markSymbolMapNull`. `commit` writes these into the transaction's WAL event, and `resetDataTxnProperties` clears the
maps after the commit. Neither `cancelColumnarWrite` nor `rollback0` touches them.

### 6.2 markSavepoint

`WalWriter.markSavepoint()` stores four values on the writer:

- `savepointRows = segmentRowCount - currentTxnStartRowNum`, the uncommitted row count;
- `savepointMinTimestamp = txnMinTimestamp`;
- `savepointMaxTimestamp = txnMaxTimestamp`;
- `savepointOutOfOrder = txnOutOfOrder`.

It asserts that no columnar write is in progress, performs no I/O and does not check whether the writer is
distressed (section 15). A later mark replaces the earlier one. The savepoint belongs to the transaction it is
marked in: a commit or a rollback ends it, and `rollbackToSavepoint` asserts that the rows before the mark are still
uncommitted.

The row count is relative to `currentTxnStartRowNum` rather than an absolute segment row, because the writer may
move the transaction to a new segment between the mark and the rewind:

- `WalColumnarRowAppender.beginColumnarWrite` calls `rollSegment` when `rollSegmentOnNextRow` is set, and asserts
  that this happens only with zero uncommitted rows; `WalWriter.newRow` rolls the same way. `openNewSegment` then
  sets both `segmentRowCount` and `currentTxnStartRowNum` to 0. A savepoint of zero rows marked before that roll
  still names row 0 of the new segment.
- `rollUncommittedToNewSegment` copies the uncommitted rows into a new segment and sets
  `segmentRowCount = uncommittedRows` and `currentTxnStartRowNum = 0`. The relative count survives. The batch never
  reaches this path with rows, because it refuses column creation for batch tables and takes no structure changes
  through a batch table's writer; a table new to the batch may create columns while its writer holds no rows, which
  only sets `rollSegmentOnNextRow`. The method's javadoc tells callers not to change the structure between a mark and
  a rollback, because the metadata change would stay.

### 6.3 rollbackToSavepoint

`WalWriter.rollbackToSavepoint()`:

1. Calls `throwIfInColumnarWrite("rollback to savepoint")`. `WalColumnarRowAppender.cancelColumnarWrite` clears
   `inColumnarWrite` only after `WalWriter.cancelColumnarWrite` returned, so a cancel that threw leaves the flag set,
   and the writer's columns are at unknown positions. The rewind refuses, and leaves the writer untouched.
2. Calls `checkDistressed()`, which throws for a distressed writer, and for an engine whose durability barrier
   failed. `rollback0` skips a distressed writer silently; the savepoint must not, because the batch would then
   commit rows it did not account for.
3. Computes `target = currentTxnStartRowNum + savepointRows`. When `segmentRowCount > target` or
   `hasDirtyColumns(target)`, it calls `setAppendPosition(target)`, sets `segmentRowCount = target`, and restores
   `txnMinTimestamp`, `txnMaxTimestamp` and `txnOutOfOrder` from the saved values. When `savepointRows` is 0 this is
   what `rollback0` does.
4. On any throwable from step 3 it sets `distressed = true` and rethrows, as `rollback0` does, so the pool never
   reuses a writer whose columns may be at inconsistent positions.

The `hasDirtyColumns(target)` term stays (section 15): it drops the values of a row that was started with the row
API and neither appended nor cancelled, as `rollback0` does. On the columnar path it is never the deciding term.

### 6.4 Why the rewind is exact

- Column positions. `setAppendPosition(target)` jumps every live column's data memory, and for var-size and array
  columns reads the aux entry at `target` to find the data offset (`setAppendAuxMemAppendPosition`). The aux entries
  below `target` are intact, because appends only ever write past the current end. `cancelColumnarWrite` already
  uses this rewind at a row in the middle of a transaction whenever a joining block fails behind batch rows, so the
  savepoint adds no new kind of rewind.
- `rowValueIsNotNull`. `setAppendPosition` sets every live column's entry to `target - 1`. This is exact because
  every row below `target` is fully populated: `finishColumnarWrite` null-fills every column of a completed block,
  and the null setters (`configureNullSetters`) append data without updating `rowValueIsNotNull`, so the entry may
  lag behind the data but the data never lags behind the entry. `rollback0` relies on the same fact when it rewinds
  to `currentTxnStartRowNum`.
- Transaction bounds. `commit` passes `txnMinTimestamp`, `txnMaxTimestamp` and `txnOutOfOrder` to the sequencer and
  the WAL event (`nextSeqTxn`, `getSequencerTxn`). `finishColumnarWrite` and `rowAppend` only widen them, so the
  values at the mark are the values for the rows below `target`, and restoring the saved copy is exact.
- Deleted columns. `setAppendPosition` skips columns with a negative type, and `finishColumnarWrite` skips them
  because their `rowValueIsNotNull` entry is `COLUMN_DELETED_NULL_FLAG`.
- Metadata. The batch appends to a batch table with column creation refused, so no block of a joined frame changes
  the writer's metadata, and the rewind has nothing structural to undo. A table new to the batch may have had
  columns created by the lookup or by its first block; those stay, as they do on the normal path after a failed
  frame.

### 6.5 What the rewind leaves behind

A rewound block's new symbol keys stay in the writer's in-memory symbol maps until `resetDataTxnProperties` clears
them at the next commit, and that commit records them in the transaction's symbol map section even when no row
references them. A null symbol in a rewound block leaves `symbolMapNullFlags` set, and the next commit reports it to
the apply job, which marks the symbol column as containing nulls. Both are what `cancelColumnarWrite` followed by a
commit, or `rollback` followed by a commit, leave behind; neither changes query results.

`TableUpdateDetails` keeps no per-append state the rewind must reset: `isFirstRow`,
`commitIfMaxUncommittedRowsCountReached` and `commit` read the writer's `getUncommittedRowCount`, and
`nextCommitTime` changes only on a commit.

## 7. Per-table caches

### 7.1 Schema parse

For each block, `QwpTableBlockCursor` asks a `QwpSchemaCache` for the table's slot, a `QwpSchemaCacheEntry`.
`QwpTudCache` implements the interface and keeps the slot on the table's `WalTableUpdateDetails`. A block reuses the
slot's parse when its column count matches and its schema section is byte-identical to the slot's copy. Otherwise the
cursor empties the slot, parses the section, and stores the new parse and bytes. A block of a table that has no
entry yet is parsed and its parse is not kept.

`QwpTableBlockCursor.cacheSchema` and the `schema`, `schemaAddress` and `schemaLength` fields that served it are
gone, with the call in `QwpWalAppender.appendToWalStreaming`. The method stored a table's first parse in the slot the
lookup had just created, so that the table's second block found the object Phase 1 had resolved. Without it, the
second block parses into the slot, `ColumnMapping.isResolvedFor` misses on the new object, Phase 1 runs once more
and stores it, and the third block hits. `QwpSchema.parse` allocates a new `QwpSchema` and column list on every
call, so object identity remains a safe cache key. The cost is one parse and one Phase 1 per table per connection,
at the table's second block.

A parse lives as long as its table's entry, so parses are bounded by the tables a connection holds open.
`WalTableUpdateDetails.close` drops them in a finally, also when closing the writer throws. The QWP UDP receiver
uses the same cache.

### 7.2 Column mapping

Phase 1 of `QwpWalAppender` maps each block column to a writer column and creates missing columns. Its result lives
on the table's entry as `ColumnMapping`. A block skips Phase 1 when its schema object is the one the mapping was
resolved for and the writer's metadata version is unchanged. A table's writer is fixed for the life of its entry, so
the key needs no writer. Schemas with array columns run Phase 1 for every block, because array dimensions are checked
against each block's data. Unchanged from PR #8195.

### 7.3 Table lookup

`QwpTudCache.getCachedTableUpdateDetails` finds a table's entry without side effects. It checks the entry it
returned last before hashing, because a block's schema lookup and its batch lookup ask for the same table back to
back. Every removal from the map goes through `onEntryRemoved`, which forgets that entry. `findCommitBatchEntry`
reads a slot hint from the entry (`qwpCommitBatchSlot`) and confirms it by identity, so the hint never needs a reset.
The hint stays an int: `CommitBatchEntry` is private to `QwpIngressProcessorState`, and `WalTableUpdateDetails`
lives in another package.

`CommitBatchEntry` objects are kept in `commitBatchEntries` and reused in place behind a `commitBatchEntryCount`;
`resetCommitBatch` clears the references of the entries in use and the count. The separate `commitBatchEntryPool`
and its `popLast` and `clear` traffic are gone.

`LowerCaseUtf8SequenceObjHashMap.clear` nulls its values, as in PR #8195.

## 8. Acks larger than the send buffer

Unchanged from PR #8195. `trySendAck` and `trySendDurableAckFrame` get the address to write a frame to from
`reserveFrame`. A frame that fits the send buffer goes there. A larger one goes to staging memory on the connection
state (`stageFrame`, `MemoryTag.NATIVE_HTTP_CONN`), which grows to the largest frame and is freed at disconnect.
`sendStagedFrame` copies the frame into the send buffer one buffer at a time and sends each piece. When a piece
blocks, the send buffer keeps its unsent part, the rest stays staged, and the caller parks its usual resume state.
`finishStagedFrame` runs first in every resume path and in `drainPendingResponse`, so no other frame goes out
between the pieces. The bytes on the wire are the same as for an ack that fits; only the number of `send` calls
changes.

## 9. Memory and allocation

Once warm, a frame that joins a batch allocates nothing (`QwpIngressProcessorStateTest#testInterleavedTables-
AllocateNothingOnceWarm`). A table is warm after its second block on a connection.

| Structure | Owner | Size | Released |
|---|---|---|---|
| `CommitBatchEntry` list | connection state | one entry per table of the largest batch, seven fields each | when the state closes |
| savepoint | `WalWriter` | four fields | with the writer |
| staging memory | connection state | the largest staged frame | at disconnect |
| `ColumnMapping`, `QwpSchemaCacheEntry` | table entry | one each per table | with the entry |

The PR's `commitBatchFrames` record (three longs per frame of the largest pass, kept while the connection lived)
and the processor's `redoFrames` copy are gone. No structure grows with the number of frames in a pass.

## 10. Performance expectations

Nothing in this section is measured; the numbers of PR #8195 are the baseline, and `QwpIngressMultiTableBenchmark`
must be run again on this branch before merge.

On the per-frame path the design removes three `LongList.add` calls per joined frame and adds, per table a frame
touches for the first time, one long compare (`entry.frameSequence != seq`), two field stores on the entry and the
four field stores of `markSavepoint`. For a one-row frame of one table that is one compare and six stores against
three bounds-checked adds. The benchmark workloads never take a rewind, so their throughput should match the PR
within run-to-run noise.

The failure path gets cheaper. The PR decoded and appended a whole pass again after a frame failed behind batch
rows, up to 2 MiB of frames; this design rewinds the writers the failing frame touched, which is a `jumpTo` per
column, and commits what joined. Under a lowered row cap the PR re-appended up to 1.9x the rows; this design appends
each row once.

Removing the redefined-dictionary boundary removes one forced commit per redefining frame. The Java client's
orphan-adoption replay is the known source of such frames.

## 11. Tradeoffs and limitations

Duplication after a partial commit, demote latency, visibility of rows at the pass's commit, and the Java client's
one-row-frame throughput are as in PR #8195; see sections 4.6 and 5.

A lost batch commits nothing (4.5). The PR's redo rebuilt the batch with fresh writers, committed it, and NACKed the
failed frame; this design abandons the batch and NACKs the failed frame, as master did for its one-table batch. For a
fault that clears on the replay the outcome is the same, one pass later. For a failure that recurs on every replay,
such as an unexpected error in one table's writer acquisition, the healthy frames of the batch stay uncommitted while
the failed frame keeps failing, and the client's poison detector halts on the failed frame after its strikes; the
PR committed them. Losing a batch needs a storage fault in the rewind, which does no I/O beyond `MemoryMA.jumpTo` per
column and the aux read for var-size columns, or an unexpected throwable in the frame's processing.

A table's second block on a connection runs Phase 1 and a schema parse once more than in the PR.

A rewound block's symbol keys and null flags reach the next commit's WAL event, as they do after a cancelled block.

## 12. Changes relative to PR #8195

Production lines from `git diff --stat a82a4afe95 -- core/src/main`, javadoc included:

| Area | Change | Lines |
|---|---|---|
| `QwpIngressUpgradeProcessor` | Remove `redoCommitBatch`, `redoCommitBatch0`, `redoFrame`, `rejectRedoneFrame`, `redoFrames`, `REDO_FRAMES_MAX_RETAINED_CAPACITY`, `getRedoFramesCapacity`; in `handleBinaryMessage` remove the redo flags, the redo capture block and the redo dispatch, test a boolean from both `joinCommitBatch` calls, and abandon a lost batch (4.5) | +44 -249 |
| `QwpIngressProcessorState` | Remove `commitBatchFrames`, `COMMIT_BATCH_FRAMES_INITIAL_CAPACITY`, `copyCommitBatchFrames`, `getCommitBatchFramesCapacity`, the `restoreInitialCapacity` call in `onDisconnected`, `COMMIT_BATCH_JOINED`, `COMMIT_BATCH_BOUNDARY`, `COMMIT_BATCH_REDO`, `cannotJoin`, the `hasAppended`, `appendingWriter` and `appendingWriterRows` bookkeeping, the redefined-dictionary rule, `onSymbolDictApplied` and `commitBatchEntryPool`; add `rewindFrame`, `leaveCommitBatch`, `isCommitBatchIntact`, the savepoint marks, `CommitBatchEntry.frameSequence` and `frameStartRows`, and `commitBatchEntryCount`; `joinCommitBatch(long seq)` returns boolean | +157 -165 |
| `WalWriter` | Add `markSavepoint`, `rollbackToSavepoint` and four fields | +52 |
| `QwpMessageCursor` | Add `firstTableAddress` and `resetTableIteration` | +14 |
| `QwpTableBlockCursor`, `QwpWalAppender` | Remove `cacheSchema`, `schema`, `schemaAddress`, `schemaLength` and the call | -37 |

Net: 184 production lines fewer (+267 -451). `TableSequencerAPI.throwIfCommitRefused`,
`TableSequencerImpl.throwIfCommitRefused`, `TableUpdateDetails.authorizeCommit`, `ColumnMapping`, `QwpSchemaCache`,
`QwpSchemaCacheEntry`, the staged-ack methods, `LowerCaseUtf8SequenceObjHashMap.clear` and `QwpUdpReceiver`'s use
of the schema cache stay as they are. The proposal's row for `docs/qwp-commit-batch.md` does not apply: that
document is not part of the repository; this one replaces it.

## 13. Tests

Existing tests keep their table-content and ack assertions, except where the redo committed what a lost batch now
leaves uncommitted (4.5); the commit that removes the redo lists every changed expectation and why.

| Test | Covers |
|---|---|
| `WalWriterTest#testSavepoint*` | `markSavepoint`, append, `rollbackToSavepoint`, append, commit: `wal_transactions()` reports the row count, minTimestamp and maxTimestamp of the surviving rows, the WAL event carries the out-of-order flag of the rows before the mark, both when they are in order and when they are not, and the apply job lands the rows sorted; a rollback with nothing appended since the mark changes nothing; fixed-size, VARCHAR, STRING, BINARY, array, symbol, LONG256, UUID and designated-timestamp columns, and a null row after the rollback; a savepoint of zero rows before the segment roll that the next columnar block makes; a rollback during a columnar write throws and leaves the writer as it was; a rollback whose mapping of an earlier page fails distresses the writer, and a second rollback of the distressed writer throws; a row started and neither appended nor cancelled is dropped (`hasDirtyColumns`); a second mark replaces the first |
| `QwpIngressCommitBatchTest` | Batches across tables and multi-block frames; boundaries; partial failure and the ack; refusals at join and before commit; concurrent ALTER, DROP, RENAME and demote; sync and nosync; the ack-contract fuzz. The PR's redo scenarios keep their frames, contents and acks under rewind names, with one commit per table of the batch before the failed frame. Lost batches: `testFirstBlockCancelFailureLosesBatch`, `testFirstBlockUnexpectedErrorLosesBatch`, `testUnexpectedLookupErrorLosesBatch`, `testUnexpectedPreparationErrorLosesBatch`, `testWriterAcquisitionUnexpectedErrorAfterAppendLosesBatch`, `testRewindFailureLosesBatchAndReplayCommitsItOnce`, where the rewind's own mapping fails and the replay on a fresh connection commits the batch once, and `testRewindFailureOfFrameThatCannotJoinLosesBatch`, where the frame had not failed and fails with `INTERNAL_ERROR`. New rewind shapes: a second block failing after a block of every column type, a third block failing after a batch table and a new one in both orders, two blocks of a new table where the second fails or cannot join, a later block reaching the cap, and a new table whose cancel fails (4.4). `testRedefiningSymbolDictionaryJoinsCommitBatch` checks that a redefining frame joins |
| `QwpIngressProcessorStateTest` | `joinCommitBatch` returns; a failed frame leaves the batch's rows and the entry list as they stood, also when it brought a table into the batch; a distressed cache leaves the batch not intact and its commit refused; caches; allocation once warm |
| `QwpIngressCommitBatchTest#testCommitBatchFuzz*` | The generator keeps every failing shape; the coverage test counts rewind shapes; the pinned seeds stay, because the generator did not change |
| `QwpIngressUpgradeProcessorResumeRecvTest` | `testRewoundFrameNackBehindBlockedAckResumesInOrder`: a rewound frame's NACK behind a blocked ack |
| `QwpTableBlockCursorSchemaCacheTest`, `QwpParserCacheRetentionTest` | Unchanged. `QwpIngressProcessorStateTest#testSchemaParseLivesWithItsTableEntry` asserts that the append of a block parsed before its table opened leaves the slot empty, that the second block parses into it, and that the third hits |
| `QwpIngressStagedAckFrameTest`, `QwpIngressOversizeAckTest`, `QwpIngressOracleFuzzTest`, `WalWriterTest#testCommitRefusalCheck*`, `LineHttpTudCacheTest`, `LowerCaseUtf8SequenceObjHashMapTest` | Unchanged |

How a test makes a rewind throw: with a 16 KiB WAL data append page, a block that moves a column onto a later page
forces `setAppendPosition` back onto the first page, which `MemoryPMARImpl` maps again through `FilesFacade.mmap`; a
`TestFilesFacadeImpl` that returns `MAP_FAILED` for that one call fails the rewind. No test hook in `WalWriter` is
needed.

No test drives the real Java client into a lost batch. A lost batch needs several frames in one receive pass, and
nothing in the existing harnesses makes the real client's frames arrive in one pass deterministically; the unit
tests assert the NACK's sequence, which is what the client's poison detector keys on.

## 14. Risks

- A wrong restoration of `txnMinTimestamp`, `txnMaxTimestamp` or `txnOutOfOrder` reaches the sequencer transaction
  and the apply job without any check in between. The `WalWriterTest` savepoint tests assert the bounds through
  `wal_transactions()` and the flag through the WAL event, and a flag reset to in-order over out-of-order rows
  fails them.
- `rollbackToSavepoint` is new shared `WalWriter` surface. Only the QWP batch calls it; ILP over TCP and HTTP and
  the UDP receiver do not. A caller that marks a savepoint, adds a column and then rewinds would get row positions
  right (6.2) and metadata wrong; the method's javadoc says so, and the batch never does it.
- A frame that fails on a block of a table already in the batch after a block of a table new to the batch is the
  shape most likely to expose an ordering bug in `rewindFrame`: the new table's entry must be removed and the old
  table's rows restored in one pass. The tests in section 13 cover it with the new table first and with the old table
  first.
- `resetTableIteration` parses the first block's header and schema again. When a later block of the same table with
  another schema refilled the table's slot, the first block's parse misses and refills it: a normal cache miss. A
  frame that failed, for example on a parse error of a later block, is not prepared again (4.4).

## 15. Decisions

- `rollbackToSavepoint` keeps the `hasDirtyColumns(target)` term. It makes the method a superset of `rollback0`
  for a savepoint of zero rows and drops the values of a row started with the row API and neither appended nor
  cancelled, which would otherwise shift the next row's timestamp slot. It costs one loop over the columns, on a
  failure path, and only when no row was appended since the mark.
  `WalWriterTest#testSavepointRollbackDropsStartedRow` pins it.
- `markSavepoint` does not refuse a distressed writer and does not assert against one. A distressed writer can reach
  it: a segment roll that fails in `beginColumnarWrite` distresses the writer and throws a `CairoException`, which
  does not distress the table cache, and the frame's `clear()` keeps the entry, because `rollback0` skips a
  distressed writer without throwing. The next frame of that table, new to the batch, marks a savepoint on it. The
  mark is harmless there: the append that follows refuses the distressed writer in `beginColumnarWrite`, and
  `rollbackToSavepoint` refuses it too.
- Column creation stays refused for tables already in the batch. `addColumn` rolls uncommitted rows to a new segment
  and changes metadata the rewind cannot restore; a frame that needs a column on a batch table rewinds, the batch
  commits, and the frame creates the column on its own commit, as in PR #8195.
- A test makes `rollbackToSavepoint` throw through a failed page mapping (section 13); `WalWriter` has no test hook.
- A lost batch NACKs the failed frame, not the batch's first frame (4.5).
- A table new to the batch whose cancel fails leaves the batch committable (4.4).

## 16. Where the code differed from the proposal

- The proposal set a rewound entry's rows from the writer (`entry.rows = writer.getUncommittedRowCount()`) and
  asserted they equal the rows before the frame, which nothing recorded. The entry now records them
  (`frameStartRows`) when the frame marks the savepoint; the rewind sets `entry.rows` from them, and the commit-time
  check compares them with the writer, so a wrong rewind is refused instead of trusted.
- The proposal re-prepared every frame that did not join, inside `rewindFrame`. A failed frame is not prepared again:
  the normal path does not process it, and after a parse failure `rejectMessageError` has already released the
  message cursor's resources, so `resetTableIteration` would find a cleared cursor. The re-preparation lives in
  `leaveCommitBatch`, inside `joinCommitBatch`'s try, which handles its parse failure; a separate catch for it could
  run only on a parse that failed the second time over the same bytes, so no test could reach it.
- The proposal let a failed rewind return without failing the frame. A frame that only could not join would then
  take the normal path on a distressed writer; `rewindFrame` fails such a frame with `INTERNAL_ERROR`.
- The proposal routed a lost batch through `commitBatch`, refusing it at its first entry and NACKing the batch's
  first frame. Against the pinned Java client that misattributes a recurring failure to a healthy frame (4.5); the
  supervisor approved NACKing the failed frame instead, as master did.
- The proposal's risk that `resetTableIteration` meets a slot emptied by a parse error of a later block does not
  arise: a parse error fails the frame, and a failed frame is not prepared again.
- The proposal expected an assertion in `markSavepoint` against a distressed writer; a distressed writer is
  reachable there (section 15), so the assertion would fail on a legitimate state.
- The PR branch's `joinCommitBatch` took the frame's payload address and length for the per-frame record; with the
  record gone it takes only the sequence.
- The proposal estimated about 260 production lines fewer; the branch has 184 fewer.

## 17. Code map

| Class | Role in this design |
|---|---|
| `QwpIngressUpgradeProcessor` | Frame loop, batch boundaries, `handleBinaryMessage` with the lost-batch path, `commitPendingBatch`, staged sends (`reserveFrame`, `sendFrame`, `sendStagedFrame`, `finishStagedFrame`) |
| `QwpIngressProcessorState` | Batch state and rules: `joinCommitBatch`, `leaveCommitBatch`, `rewindFrame`, `isCommitBatchIntact`, `commitBatch`, `commitBatchEntries`, `onCommitBatchFailed`, `abandonCommitBatch`, `CommitBatchEntry`, staging memory |
| `WalWriter` | `markSavepoint`, `rollbackToSavepoint`, and the existing `cancelColumnarWrite`, `finishColumnarWrite`, `setAppendPosition`, `rollback0` they build on |
| `WalColumnarRowAppender` | `beginColumnarWrite`, `endColumnarWrite`, `cancelColumnarWrite`; the segment-roll assertion the relative savepoint relies on |
| `QwpMessageCursor` | `resetTableIteration`, so a frame that only could not join restarts at its first block without re-applying its dictionary delta |
| `QwpTudCache` | Table entries; `commitBatch` for one table; `QwpSchemaCache` implementation; side-effect-free lookup; the distressed `clear()` that frees every writer |
| `QwpWalAppender` | Appends a block; per-table `ColumnMapping`; refuses column creation when asked |
| `QwpTableBlockCursor`, `QwpSchemaCache`, `QwpSchemaCacheEntry` | Per-table schema parse cache |
| `WalTableUpdateDetails` | Holds a table's mapping, schema slot and batch slot hint; drops the caches on close |
| `TableSequencerAPI.throwIfCommitRefused` | The sequencer's commit refusals, checked without committing |
| `LowerCaseUtf8SequenceObjHashMap` | `clear` releases values |
