/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.DeleteOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;

import java.io.Closeable;

public class OperationExecutor implements Closeable {
    private static final Log LOG = LogFactory.getLog(OperationExecutor.class);
    private final BindVariableService bindVariableService;
    private final CairoEngine engine;
    // Sized to all survivor-cursor columns to build a 1:1 SELECT*->writer copier (see executeDelete).
    private final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
    private final WalApplySqlExecutionContext executionContext;
    private final int maxRecompilationAttempts;
    private final Rnd rnd;

    OperationExecutor(
            CairoEngine engine,
            int sharedQueryWorkerCount
    ) {
        rnd = new Rnd();
        bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
        executionContext = new WalApplySqlExecutionContext(
                engine,
                sharedQueryWorkerCount
        );
        executionContext.with(
                engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                bindVariableService,
                rnd,
                -1,
                null
        );
        this.engine = engine;
        this.maxRecompilationAttempts = engine.getConfiguration().getMaxSqlRecompileAttempts();
    }

    /**
     * Acquires a {@link MemoryTrackerWorkload#WAL_APPLY} tracker and binds it on the
     * apply context, so SQL applied in the batch (ALTER, UPDATE) inherits it via the
     * {@code QueryRegistry} nesting check. Pair with
     * {@link #releaseMemoryTracker(MemoryTracker)}.
     */
    public MemoryTracker acquireMemoryTracker(int tableId) {
        assert executionContext.getMemoryTracker() == null;
        final MemoryTracker memoryTracker = engine.getMemoryTrackerProvider().acquire(
                executionContext.getSecurityContext(),
                tableId,
                MemoryTrackerWorkload.WAL_APPLY
        );
        executionContext.setMemoryTracker(memoryTracker);
        return memoryTracker;
    }

    @Override
    public void close() {
        Misc.free(executionContext);
    }

    /**
     * Returns result of underlying {@link AlterOperation#matViewInvalidationReason()}.
     */
    public String executeAlter(TableWriter tableWriter, CharSequence alterSql, long seqTxn) throws SqlException {
        final TableToken tableToken = tableWriter.getTableToken();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            executionContext.remapTableNameResolutionTo(tableToken);
            CompiledQuery compiledQuery;
            int stallCount = 0;
            while (true) {
                try {
                    compiledQuery = compiler.compile(alterSql, executionContext);
                    break;
                } catch (TableReferenceOutOfDateException ex) {
                    // The table is renamed in the table registry
                    // just before the compilation of this ALTER
                    TableToken updatedToken = engine.getUpdatedTableToken(tableToken);
                    if (updatedToken != null && !updatedToken.equals(tableToken)) {
                        tableWriter.updateTableToken(updatedToken);
                        executionContext.remapTableNameResolutionTo(updatedToken);
                    } else {
                        // This is a transient error, we should retry
                        // it can happen if the table renamed in the middle
                        // of alter compilation but then renamed back.
                        // This is highly unlikely to stall in real life
                        // but keeping the DB in live lock is not a good idea, hence there is a limit
                        if (stallCount++ > maxRecompilationAttempts) {
                            throw ex;
                        }
                    }
                }
            }
            try (AlterOperation alterOp = compiledQuery.getAlterOperation()) {
                alterOp.withContext(executionContext);
                assert !alterOp.isStructural() : "alter operation must not be structural when applied as SQL";
                tableWriter.apply(alterOp, seqTxn);
                return alterOp.matViewInvalidationReason();
            }
        } catch (SqlException ex) {
            tableWriter.markSeqTxnCommitted(seqTxn);
            throw ex;
        }
    }

    /**
     * Applies a {@code DELETE FROM t WHERE <pred>} that arrived through the WAL as SQL text. Runs on the
     * {@link ApplyWal2TableJob} thread holding {@code tableWriter} (single-threaded per table, all prior
     * transactions already applied), so the survivor cursor may freely read the same table the replace
     * then overwrites (as {@code executeUpdate} does).
     * <p>
     * Recompiles the DELETE under this apply context ({@code isWalApplication()==true}); the compiler
     * negates the predicate and hands back a schema-identical {@code SELECT * FROM t WHERE NOT(pred)}
     * survivor factory (see {@link io.questdb.griffin.SqlCompilerImpl#generateDelete}). The survivors
     * replace the whole populated timestamp range via {@link TableWriter#replaceRange}: matched rows drop,
     * unmatched rows are rewritten, and a fully-emptied table truncates.
     * <p>
     * Task 2.1 fast path: when the compiler classifies the predicate as a pure single designated-timestamp
     * interval with no residual filter ({@link DeleteOperation#isPureTimeRange()}), the delete is instead
     * applied as one empty {@code replaceRange} over the DELETED interval ({@link #deleteTimeRange}) - O(rows
     * deleted), no survivor staging. Both branches are a single table commit, so the seqTxn handling is
     * identical.
     *
     * @return number of rows removed
     */
    public long executeDelete(TableWriter tableWriter, CharSequence deleteSql, long seqTxn) throws SqlException {
        final TableToken tableToken = tableWriter.getTableToken();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            executionContext.remapTableNameResolutionTo(tableToken);
            CompiledQuery compiledQuery;
            int stallCount = 0;
            while (true) {
                try {
                    compiledQuery = compiler.compile(deleteSql, executionContext);
                    break;
                } catch (TableReferenceOutOfDateException ex) {
                    // The table was renamed in the registry between apply and this recompile; re-point and
                    // retry (mirrors executeAlter). Bounded to avoid a live lock on a rename/rename-back.
                    TableToken updatedToken = engine.getUpdatedTableToken(tableToken);
                    if (updatedToken != null && !updatedToken.equals(tableToken)) {
                        tableWriter.updateTableToken(updatedToken);
                        executionContext.remapTableNameResolutionTo(updatedToken);
                    } else if (stallCount++ > maxRecompilationAttempts) {
                        throw ex;
                    }
                }
            }
            try (DeleteOperation deleteOp = compiledQuery.getDeleteOperation()) {
                try {
                    // Opt-in non-atomic disk-bounded route (H1, cairo.wal.delete.disk.bounded): only for the
                    // arbitrary (non-time-range) survivor-replace on a table that actually has Parquet
                    // partitions to convert. It manages its OWN seqTxn: each window is its own commit at the
                    // still-current durable seqTxn S-1 (progressively deleting, visible to concurrent readers),
                    // and one final commitSeqTxn(seqTxn) advances S-1 -> S. So it must NOT run the up-front
                    // convert pre-pass (each window converts only its own overlapping Parquet partitions) and
                    // must NOT setSeqTxn(seqTxn) up front. It bounds transient Parquet-convert disk to one
                    // window at the cost of atomicity: a concurrent reader may observe a partial delete during
                    // apply. Still crash-safe: a crash mid-loop leaves durable S-1, the whole delete re-applies,
                    // finished windows re-apply as no-ops (survivors-of-survivors) and already-native partitions
                    // re-convert as no-ops.
                    if (!deleteOp.isPureTimeRange()
                            && engine.getConfiguration().getWalDeleteDiskBounded()
                            && tableWriterHasParquet(tableWriter)) {
                        return replaceWithSurvivorsDiskBounded(compiler, tableWriter, deleteOp, seqTxn);
                    }

                    // Task 3.1 Parquet convert-to-native fallback. Any Parquet partition the replace below would
                    // REWRITE (a boundary trim on the time-range route, or an arbitrary-condition rewrite) is
                    // converted to native FIRST, because the replace path cannot rewrite Parquet in place. That
                    // conversion is its own physical commit (convertPartitionParquetToNative's
                    // bumpPartitionTableVersion + the self-committing commitPendingParquetToNativeConversions),
                    // so convert-then-replace is inherently TWO commits. Crash-safety comes from a single seqTxn
                    // advance: this pre-pass runs BEFORE setSeqTxn(seqTxn), so its commit (#1) persists the PRIOR
                    // seqTxn S-1 plus the native-format change; the replace (commit #2) is the ONLY commit that
                    // advances to S. A crash between the two leaves the table durably at S-1 with some partitions
                    // converted (data fully intact); ApplyWal2TableJob re-runs txn S, the re-issued
                    // convertPartitionParquetToNative(false) on a now-native partition is an idempotent no-op, and
                    // the replace proceeds - no lost or partial delete. NEVER move setSeqTxn(seqTxn) before this
                    // pre-pass: if commit #1 persisted S, a crash would silently LOSE the delete (S marked
                    // applied, delete never performed, never retried).
                    convertParquetPartitionsForDelete(tableWriter, deleteOp);

                    // Advance the sequencer txn exactly as TableWriter.apply(op, seqTxn) does. setSeqTxn before
                    // the mutation so the replace commit persists THIS seqTxn; if no data txn was written
                    // (empty table, or an identical-data no-op replace), force a seqTxn commit. Without this the
                    // writer's persisted seqTxn never reaches this txn and ApplyWal2TableJob re-runs the DELETE
                    // forever. On error, rollback and mirror apply()'s WAL-tolerable / retry handling.
                    tableWriter.setSeqTxn(seqTxn);
                    final long txnBefore = tableWriter.getTxn();
                    // Time-range fast path (Task 2.1): a pure single designated-timestamp interval delete is
                    // one empty replaceRange over the DELETED interval (O(deleted), no survivor staging).
                    // Everything else keeps the always-correct whole-range survivor-replace. Both are a single
                    // table commit, so the seqTxn / no-op-advance handling below is identical for either.
                    final long deleted = deleteOp.isPureTimeRange()
                            ? deleteTimeRange(tableWriter, deleteOp)
                            : replaceWithSurvivors(compiler, tableWriter, deleteOp);
                    if (tableWriter.getTxn() == txnBefore) {
                        tableWriter.commitSeqTxn(seqTxn);
                    }
                    return deleted;
                } catch (CairoException ex) {
                    // Rollback in case of any dirty state. Do not catch rollback exceptions here:
                    // let the calling code handle a distressed writer (mirrors TableWriter.apply).
                    tableWriter.rollback();
                    if (ex.isWALTolerable()) {
                        // Mark this txn applied and skip it (mirrors TableWriter.apply).
                        tableWriter.commitSeqTxn(seqTxn);
                        return 0;
                    }
                    // Mark as not applied so the apply job can retry.
                    tableWriter.setSeqTxn(seqTxn - 1);
                    throw ex;
                } catch (Throwable th) {
                    // Any other throwable (an Error such as OOM, a SqlException thrown by
                    // survivorFactory.getCursor() before any row was staged, or a failure inside the Parquet
                    // convert pre-pass) must not escape without rolling back and marking this txn as not
                    // applied, or the writer is left dirty with an advanced in-memory seqTxn that the apply
                    // job will never retry. A throw before the convert commit (#1) discards the uncommitted
                    // conversion; a throw after it rolls back to the durable S-1 (partitions converted, data
                    // intact) and retries txn S. Guard the rollback itself so a distressed-writer failure
                    // during cleanup doesn't mask the original throwable (mirrors TableWriter.apply's broad
                    // Throwable branch).
                    try {
                        tableWriter.rollback();
                    } catch (Throwable th2) {
                        LOG.critical().$("could not rollback, table is distressed [table=")
                                .$(tableToken).$(", error=").$(th2).I$();
                    }
                    // This explicit set is LOAD-BEARING, not mere defensive parity. TableWriter.rollback()
                    // reloads the durable seqTxn (S-1) from disk via txWriter.unsafeLoadAll() ONLY when
                    // (o3InError || inTransaction()) is true (see TableWriter.rollback: the whole body, reload
                    // included, is gated on that condition). A throw from survivorFactory.getCursor() -- or any
                    // failure BEFORE the replace enters a transaction -- leaves both flags false, so rollback()
                    // is a complete no-op that does NOT restore seqTxn. Without this line the writer would keep
                    // the advanced in-memory seqTxn (S) and the apply job would never retry the delete. It also
                    // backstops the case where the guarded rollback() above threw partway through its own
                    // cleanup (th2, logged above) before reaching the reload.
                    tableWriter.setSeqTxn(seqTxn - 1);
                    throw th;
                }
            }
        }
        // Do not catch SqlException from compile / mark the txn committed: like executeUpdate, a compile
        // failure here can be transient (e.g. table busy) and must be retried by the apply job.
    }

    /**
     * Task 3.1 Parquet convert-to-native fallback. Converts to native - ahead of the delete's
     * {@link TableWriter#replaceRange} - every Parquet partition the replace would REWRITE (the replace path
     * cannot rewrite a Parquet partition in place). Which partitions those are depends on the delete's route
     * (see {@link #executeDelete}):
     * <ul>
     *   <li><b>Time-range route</b> ({@link DeleteOperation#isPureTimeRange()}, an empty replace over the
     *       clamped deleted interval {@code [dLo, dHiExcl)}): only the &le;2 <b>boundary</b> Parquet
     *       partitions - those a delete endpoint splits, i.e. that OVERLAP the interval but are NOT fully
     *       covered by it. Fully-covered interior Parquet partitions are dropped inline by the replace
     *       (Task 2.2) with no data rewrite, so they are deliberately NOT converted. The coverage test mirrors
     *       the replace path's own fully-covered check (exact data bounds at the table ends via
     *       {@code getMinTimestamp()}/{@code getMaxTimestamp()}, partition floor / next-floor otherwise). A
     *       Parquet partition is always a whole logical (calendar-day) partition and is NEVER split (see the
     *       per-partition comment), so for a CONTIGUOUS partition this is the exact set the replace would
     *       reject; under a CALENDAR GAP it is a sound SUPERSET (may over-convert a gap-adjacent Parquet
     *       partition the delete does not touch - safe, never under-converts). See the per-partition comment
     *       for the bound detail.</li>
     *   <li><b>Arbitrary route</b> (whole-range survivor-replace over {@code [minTs, maxTs+1)}): EVERY Parquet
     *       partition, because the whole-range replace rewrites every partition and there is no cheap
     *       per-partition match test. <b>HEAVY v1 side-effect:</b> an arbitrary DELETE un-tiers ALL Parquet
     *       partitions of the table (even a delete that matches no rows). This is correct but pessimistic; it
     *       is bounded once a per-partition survivor fast-path lands that can skip partitions with no matched
     *       rows.</li>
     * </ul>
     * <p>
     * When at least one partition is converted, the single batched
     * {@link TableWriter#commitPendingParquetToNativeConversions()} is <b>commit #1</b>: because this runs
     * BEFORE {@code executeDelete}'s {@code setSeqTxn(seqTxn)} it persists the PRIOR seqTxn {@code S-1} together
     * with the native-format change and its housekeeping; the delete's replace is commit #2 (advances to
     * {@code S}). See {@code executeDelete} for the crash-safety argument (the re-issued convert on a
     * now-native partition is an idempotent no-op on WAL re-apply). When no partition is Parquet the whole
     * pre-pass is a no-op - identical single-commit behavior for all-native tables. A throw here propagates to
     * {@code executeDelete}'s rollback scaffolding, which discards the uncommitted conversion.
     */
    private void convertParquetPartitionsForDelete(TableWriter tableWriter, DeleteOperation deleteOp) {
        final int partitionCount = tableWriter.getPartitionCount();
        if (partitionCount == 0) {
            return; // empty table: nothing to convert
        }
        int converted = 0;
        if (deleteOp.isPureTimeRange()) {
            // Clamp the deleted interval to the populated range, exactly as deleteTimeRange does.
            final long dLo = Math.max(deleteOp.getTimeRangeLo(), tableWriter.getMinTimestamp());
            final long dHiExcl = Math.min(deleteOp.getTimeRangeHiExcl(), tableWriter.getMaxTimestamp() + 1);
            if (dLo >= dHiExcl) {
                return; // interval entirely outside the populated range: the replace is a no-op, convert nothing
            }
            final long minTs = tableWriter.getMinTimestamp();
            final long maxTs = tableWriter.getMaxTimestamp();
            for (int i = 0; i < partitionCount; i++) {
                if (tableWriter.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                    continue;
                }
                final long floor = tableWriter.getPartitionTimestamp(i);
                // Sound data-extent bounds: exact at the table ends (getMin/getMaxTimestamp), else the partition
                // floor (a true LOWER bound - every row is >= its floor) and next-partition-floor-minus-one (a
                // true UPPER bound on this partition's max data ts).
                //
                // This uses the next PHYSICAL floor where the replace guard (TableWriter.processO3Block ~9954)
                // uses the calendar-aware getCurrentPartitionMaxTimestamp (= getNextPartitionTimestamp(floor)-1).
                // Those two bounds could in principle diverge for a SPLIT partition (physical-next-floor = the
                // split boundary, tighter than the whole-day calendar ceiling), which would risk UNDER-convert.
                // They cannot diverge that way here, because this branch only ever inspects PARQUET partitions
                // and a Parquet partition is NEVER in a split state - it is always one whole logical (calendar-
                // day) partition whose physical successor is never a same-day split sibling. Why split-Parquet
                // is unreachable (a Parquet partition is immutable w.r.t. splitting):
                //   - O3 into an existing Parquet partition REWRITES the whole partition file (it never carves a
                //     split): O3PartitionJob.processParquetPartition hard-writes o3SplitPartitionSize=0 and the
                //     SAME partitionTimestamp to the update sink; the native split path (processPartition's
                //     "Split partition if the prefix is large enough") is dispatch-guarded by isParquet and is
                //     unreachable for Parquet. A brand-new Parquet partition is written whole by
                //     writeFreshParquetFromO3.
                //   - convert-to-parquet SQUASHES first: TableWriter.convertPartitionNativeToParquet maps the
                //     target to its logical floor then calls squashPartitionForce, so the converted partition is
                //     one whole logical partition, never a split piece.
                //   - there is no ALTER TABLE ... SPLIT PARTITION statement.
                // Given that, next-physical-floor equals the guard's calendar ceiling for a CONTIGUOUS partition
                // (the bound is EXACT - byte-identical decisions to the guard) and is only LARGER across a
                // CALENDAR GAP (missing partitions between two physical ones). A larger dataMax makes
                // fullyCovered STRICTER and overlaps LOOSER - both push toward MORE conversion - so this set is
                // always a sound SUPERSET of the guard's reject set (convert here whenever the guard would
                // reject): it can only over-convert a gap-adjacent Parquet partition the delete does not touch
                // (a benign extra un-tier), NEVER under-convert. So it never skips a Parquet partition the guard
                // would rewrite -> the replace can never hit "commit replace mode is not supported for Parquet
                // partitions" -> a valid DELETE never spuriously suspends the table. Removing the (safe) gap
                // over-conversion would need a public calendar-ceiling accessor on TableWriter
                // (getCurrentPartitionMaxTimestamp lives on txWriter/TxReader only); deliberately not added.
                final long dataMin = (i == 0) ? minTs : floor;
                final long dataMax = (i == partitionCount - 1) ? maxTs : (tableWriter.getPartitionTimestamp(i + 1) - 1);
                final boolean overlaps = dataMin < dHiExcl && dataMax >= dLo;
                final boolean fullyCovered = dataMin >= dLo && dataMax < dHiExcl;
                if (overlaps && !fullyCovered) {
                    // Partially-covered Parquet boundary partition: the replace would trim it (a data rewrite
                    // Parquet cannot do in place). Convert to native first; doCommit=false batches all
                    // conversions into the single commit below.
                    tableWriter.convertPartitionParquetToNative(floor, false);
                    converted++;
                }
            }
        } else {
            // Arbitrary route: the whole-range survivor-replace rewrites EVERY partition, and there is no cheap
            // per-partition match test, so EVERY Parquet partition must be converted. HEAVY v1 side-effect: an
            // arbitrary DELETE un-tiers ALL Parquet partitions of the table (even a no-match delete). Correct
            // but pessimistic; improved when a per-partition survivor fast-path can skip unmatched partitions.
            for (int i = 0; i < partitionCount; i++) {
                if (tableWriter.getPartitionFormat(i) == PartitionFormat.PARQUET) {
                    tableWriter.convertPartitionParquetToNative(tableWriter.getPartitionTimestamp(i), false);
                    converted++;
                }
            }
        }
        if (converted > 0) {
            // Commit #1 at seqTxn S-1 (setSeqTxn(seqTxn) has NOT run yet): publishes the native-format change
            // and its housekeeping. The delete's replace is commit #2, the only one that advances to S.
            tableWriter.commitPendingParquetToNativeConversions();
        }
    }

    /**
     * Windowed survivor replace (Task 5 / C1): overwrites the table's whole populated timestamp range
     * {@code [minTimestamp, maxTimestamp+1)} with the survivor rows, tiled into ~{@code rowsPerStep}-sized
     * windows so peak O3 memory is bounded to one window regardless of table size - instead of staging every
     * surviving row into O3 memory at once (the prior whole-range {@code replaceRange} call, which could OOM
     * on a large table).
     * <p>
     * Each window is applied via {@link TableWriter#applyReplaceRangeWindow} under a single
     * {@link TableWriter#beginReplaceRange}/{@link TableWriter#finishReplaceRange} bracket, so the whole
     * delete remains ONE commit (one seqTxn advance), exactly like the single-window path it replaces. Window
     * bounds are tiled gaplessly: window K's exclusive high becomes window K+1's inclusive low, so deleted
     * rows that fall in the gap between two adjacent windows' survivors are still covered by exactly one
     * window.
     * <p>
     * Per window, the survivor cursor is re-obtained from {@code survivorFactory} after rebinding
     * {@link DeleteOperation#WINDOW_LO_BIND}/{@link DeleteOperation#WINDOW_HI_BIND} (via
     * {@link DeleteOperation#setWindowBound}, never a raw {@code bind.setTimestamp} - see the field comment
     * on {@code tsColType} below) to the window's {@code [wLo, wHiExcl)}, so
     * {@code SqlCompilerImpl.generateDelete}'s ANDed interval predicate restricts each pass to an interval
     * scan of just that window rather than a full-table rescan. Every window reads the table's COMMITTED
     * (pre-delete) state, because the loop does not commit until {@code finishReplaceRange} - the same
     * read-the-table-being-overwritten pattern {@code executeUpdate} relies on - so each window's cursor sees
     * its own untouched, disjoint slice with no snapshotting required.
     */
    private long replaceWithSurvivors(SqlCompiler compiler, TableWriter tableWriter, DeleteOperation deleteOp) throws SqlException {
        final RecordCursorFactory survivorFactory = deleteOp.getSurvivorFactory();
        assert survivorFactory != null : "survivor factory must be built at WAL apply time (isWalApplication)";

        if (tableWriter.getPartitionCount() == 0) {
            // Empty table: nothing to delete, nothing to stage.
            return 0;
        }

        // The survivor cursor is a schema-identical SELECT * over the table, so its columns line up 1:1 with
        // the writer's and the designated timestamp sits at the same index. Build the copier over an
        // EntityColumnFilter covering all columns (mirrors MatViewRefreshJob.getRecordToRowCopier).
        final int timestampCursorIndex = tableWriter.getMetadata().getTimestampIndex();
        entityColumnFilter.of(survivorFactory.getMetadata().getColumnCount());
        final RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(
                compiler.getAsm(),
                survivorFactory.getMetadata(),
                tableWriter.getMetadata(),
                entityColumnFilter,
                engine.getConfiguration()
        );

        final long minTs = tableWriter.getMinTimestamp();
        final long maxTs = tableWriter.getMaxTimestamp();
        final long rowsPerStep = engine.getConfiguration().getWalDeleteRowsPerStep();
        final long step = deleteWindowStep(minTs, maxTs, tableWriter.size(), rowsPerStep);
        final BindVariableService bind = executionContext.getBindVariableService();
        // Designated-ts column type (TIMESTAMP_MICRO or TIMESTAMP_NANO). The window bounds MUST be set in
        // this unit via DeleteOperation.setWindowBound: a raw bind.setTimestamp is micros-only and overflows
        // in NanosTimestampDriver.from on a nanos table -> ImplicitCastException -> table SUSPENDED. Never
        // call bind.setTimestamp on WINDOW_LO_BIND/WINDOW_HI_BIND directly.
        final int tsColType = tableWriter.getMetadata().getColumnType(timestampCursorIndex);

        tableWriter.beginReplaceRange();
        boolean finished = false;
        try {
            long wLo = minTs;
            while (wLo <= maxTs) {
                // hiExcl = min(wLo + step, maxTs + 1), overflow-safe: if step covers the rest, this is the
                // last window.
                final long remaining = maxTs - wLo + 1; // >= 1
                final long wHiExcl = (step >= remaining) ? (maxTs + 1) : (wLo + step);

                DeleteOperation.setWindowBound(bind, DeleteOperation.WINDOW_LO_BIND, tsColType, wLo);
                DeleteOperation.setWindowBound(bind, DeleteOperation.WINDOW_HI_BIND, tsColType, wHiExcl);
                try (RecordCursor survivorCursor = survivorFactory.getCursor(executionContext)) {
                    tableWriter.applyReplaceRangeWindow(wLo, wHiExcl, survivorCursor, copier, timestampCursorIndex, executionContext);
                }
                wLo = wHiExcl;
            }
            final long removed = tableWriter.finishReplaceRange();
            finished = true;
            return removed;
        } finally {
            if (!finished) {
                tableWriter.abortReplaceRange(); // executeDelete's catch performs the txn rollback + setSeqTxn(S-1)
            }
        }
    }

    /**
     * Opt-in non-atomic, disk-bounded arbitrary-DELETE survivor replace (H1, {@code cairo.wal.delete.disk.bounded}).
     * Overwrites the table's whole populated range {@code [minTimestamp, maxTimestamp+1)} with the survivor rows,
     * tiled into ~{@code rowsPerStep}-sized windows exactly like {@link #replaceWithSurvivors}, but with two
     * differences that bound BOTH staged O3 memory AND transient Parquet-convert disk to a single window (the
     * atomic path instead un-tiers ALL Parquet partitions up front, which can transiently double table disk):
     * <ul>
     *   <li>Each window converts only its OWN overlapping Parquet partitions to native (
     *       {@link #convertParquetPartitionsForDeleteWindow}) - so at most one window's partitions are transiently
     *       native - and applies via the SINGLE-CALL {@link TableWriter#replaceRange} (begin+apply+finish = one
     *       commit) rather than the atomic path's one begin/apply.../finish bracket. So every window is its OWN
     *       commit.</li>
     *   <li>Those per-window commits happen at the still-current durable seqTxn {@code S-1} (this method does NOT
     *       {@code setSeqTxn(seqTxn)} up front - see {@code executeDelete}), progressively deleting; after the last
     *       window one final {@link TableWriter#commitSeqTxn(long)} advances the durable seqTxn {@code S-1 -> S}.</li>
     * </ul>
     * <b>Non-atomic:</b> a concurrent reader may observe a partially-applied delete while this loop runs (each
     * window is visible as soon as it commits). <b>Crash-safe:</b> a crash mid-loop leaves the durable seqTxn at
     * {@code S-1} (the final {@code commitSeqTxn(S)} never ran), so {@code ApplyWal2TableJob} re-runs the WHOLE
     * delete for txn {@code S}; the finished windows re-apply as no-ops (their survivor cursor now returns only
     * survivors-of-survivors = the same survivor rows already on disk) and their already-native partitions
     * re-convert as no-ops, so the final state is identical (idempotent). A throw mid-loop propagates to
     * {@code executeDelete}'s catch, which rolls back the in-flight window and {@code setSeqTxn(seqTxn - 1)} so the
     * apply job retries the whole delete over the partially-committed (still at {@code S-1}) table - the same
     * re-apply a crash triggers.
     * <p>
     * Because each window is its own commit here, {@code txWriter.txn} advances every window, so Task 5's
     * same-bracket corruption guard (which only fires when {@code srcNameTxn == txWriter.txn} within a single
     * frozen-txn bracket) is inherently a no-op on this path - safe.
     */
    private long replaceWithSurvivorsDiskBounded(SqlCompiler compiler, TableWriter tableWriter, DeleteOperation deleteOp, long seqTxn) throws SqlException {
        final RecordCursorFactory survivorFactory = deleteOp.getSurvivorFactory();
        assert survivorFactory != null : "survivor factory must be built at WAL apply time (isWalApplication)";
        if (tableWriter.getPartitionCount() == 0) {
            // Empty table: nothing to delete, but still advance the durable seqTxn S-1 -> S so the apply job
            // does not re-run this txn forever.
            tableWriter.commitSeqTxn(seqTxn);
            return 0;
        }

        // The survivor cursor is a schema-identical SELECT * over the table, so its columns line up 1:1 with the
        // writer's and the designated timestamp sits at the same index (mirrors replaceWithSurvivors).
        final int timestampCursorIndex = tableWriter.getMetadata().getTimestampIndex();
        entityColumnFilter.of(survivorFactory.getMetadata().getColumnCount());
        final RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(
                compiler.getAsm(),
                survivorFactory.getMetadata(),
                tableWriter.getMetadata(),
                entityColumnFilter,
                engine.getConfiguration()
        );

        final long minTs = tableWriter.getMinTimestamp();
        final long maxTs = tableWriter.getMaxTimestamp();
        final long rowsPerStep = engine.getConfiguration().getWalDeleteRowsPerStep();
        final long step = deleteWindowStep(minTs, maxTs, tableWriter.size(), rowsPerStep);
        final BindVariableService bind = executionContext.getBindVariableService();
        // Designated-ts column type (TIMESTAMP_MICRO or TIMESTAMP_NANO): the window bounds MUST be set in this
        // unit via DeleteOperation.setWindowBound, never a raw bind.setTimestamp (micros-only -> overflow/suspend
        // on a nanos table). See replaceWithSurvivors' identical field comment.
        final int tsColType = tableWriter.getMetadata().getColumnType(timestampCursorIndex);

        long removed = 0;
        long wLo = minTs;
        while (wLo <= maxTs) {
            // hiExcl = min(wLo + step, maxTs + 1), overflow-safe: if step covers the rest, this is the last window.
            final long remaining = maxTs - wLo + 1; // >= 1
            final long wHiExcl = (step >= remaining) ? (maxTs + 1) : (wLo + step);
            // Convert only THIS window's overlapping Parquet partitions to native (its own commit at S-1), so at
            // most one window's partitions are transiently native.
            convertParquetPartitionsForDeleteWindow(tableWriter, wLo, wHiExcl);
            DeleteOperation.setWindowBound(bind, DeleteOperation.WINDOW_LO_BIND, tsColType, wLo);
            DeleteOperation.setWindowBound(bind, DeleteOperation.WINDOW_HI_BIND, tsColType, wHiExcl);
            try (RecordCursor survivorCursor = survivorFactory.getCursor(executionContext)) {
                // Single-call replaceRange => this window is its own commit (still at durable seqTxn S-1).
                removed += tableWriter.replaceRange(wLo, wHiExcl, survivorCursor, copier, timestampCursorIndex, executionContext);
            }
            wLo = wHiExcl;
        }
        tableWriter.commitSeqTxn(seqTxn); // FINAL: advance durable seqTxn S-1 -> S (one small commit)
        return removed;
    }

    /**
     * Converts to native every Parquet partition that OVERLAPS the window {@code [wLo, wHiExcl)} - the only
     * partitions this window's {@link TableWriter#replaceRange} would rewrite (the replace path cannot rewrite a
     * Parquet partition in place). Its own physical commit (
     * {@link TableWriter#commitPendingParquetToNativeConversions()}), which is correct here because the
     * disk-bounded path is intentionally multi-commit (each window commits at seqTxn {@code S-1}). Bounds transient
     * native-format disk to one window's partitions. On WAL re-apply after a crash, an already-native partition is
     * skipped (format gate) and a re-issued {@code convertPartitionParquetToNative(false)} is an idempotent no-op.
     */
    private void convertParquetPartitionsForDeleteWindow(TableWriter tableWriter, long wLo, long wHiExcl) {
        final int partitionCount = tableWriter.getPartitionCount();
        int converted = 0;
        for (int i = 0; i < partitionCount; i++) {
            if (tableWriter.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                continue;
            }
            final long floor = tableWriter.getPartitionTimestamp(i);
            // Physical next floor is a sound UPPER bound on this partition's data extent; the last partition can
            // never be Parquet (the active partition is never converted), so getMaxTimestamp()+1 is only a
            // defensive fallback here.
            final long nextFloor = (i == partitionCount - 1) ? (tableWriter.getMaxTimestamp() + 1) : tableWriter.getPartitionTimestamp(i + 1);
            if (floor < wHiExcl && nextFloor > wLo) { // partition [floor, nextFloor) overlaps window [wLo, wHiExcl)
                tableWriter.convertPartitionParquetToNative(floor, false);
                converted++;
            }
        }
        if (converted > 0) {
            tableWriter.commitPendingParquetToNativeConversions();
        }
    }

    /**
     * True when the table has at least one Parquet partition - the gate for the disk-bounded route (which only
     * differs from the atomic path in how it bounds Parquet-convert disk; on an all-native table there is nothing
     * to convert, so the atomic path already bounds everything and is preferred).
     */
    private static boolean tableWriterHasParquet(TableWriter tableWriter) {
        final int partitionCount = tableWriter.getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            if (tableWriter.getPartitionFormat(i) == PartitionFormat.PARQUET) {
                return true;
            }
        }
        return false;
    }

    /**
     * Ts-width (in the table's designated-timestamp unit) that spans roughly {@code rowsPerStep} rows over the
     * populated range {@code [minTs, maxTs]}, used to tile an arbitrary DELETE's survivor-replace into
     * memory-bounded windows. Reuses {@link MatViewRefreshJob#estimateBucketsForRows} with {@code bucket=1},
     * {@code partitionDuration=span}, {@code partitionCount=1}, which reduces to
     * {@code max(1, span * rowsPerStep / tableRows)} computed in double (overflow-safe for large spans). Returns
     * {@code Long.MAX_VALUE} (one window) for an empty table.
     */
    // public for testing (mirrors MatViewRefreshJob.estimateBucketsForRows, which this delegates to)
    public static long deleteWindowStep(long minTs, long maxTs, long tableRows, long rowsPerStep) {
        if (tableRows <= 0) {
            return Long.MAX_VALUE;
        }
        final long span = maxTs - minTs + 1; // caller guarantees maxTs >= minTs (non-empty populated range)
        return MatViewRefreshJob.estimateBucketsForRows(rowsPerStep, tableRows, 1, span, 1);
    }

    /**
     * Time-range fast path (Task 2.1): the whole DELETE predicate reduces to a single designated-timestamp
     * interval {@code [lo, hiExcl)} with no residual filter (classified in
     * {@link io.questdb.griffin.SqlCompilerImpl}, exposed via {@link DeleteOperation#isPureTimeRange()}), so
     * the delete is applied as ONE empty {@link TableWriter#replaceRange} over the DELETED interval - O(rows
     * deleted), with no survivor staging: fully-covered partitions drop and the boundary partition is trimmed
     * in a single commit, reusing the same empty-replace path a whole-range survivor-replace of zero survivors
     * would reach.
     * <p>
     * The interval is clamped to the table's populated range ({@code [minTimestamp, maxTimestamp+1)}); if that
     * leaves nothing to delete (interval entirely outside the data, or an empty table) it is a no-op returning
     * 0, and the caller advances the seqTxn exactly as the empty-table survivor path does (no table commit was
     * made, so {@code getTxn()} is unchanged).
     */
    private long deleteTimeRange(TableWriter tableWriter, DeleteOperation deleteOp) {
        if (tableWriter.getPartitionCount() == 0) {
            // Empty table: nothing to delete.
            return 0;
        }
        // Clamp the deleted interval to the populated range. maxTimestamp is the last row's inclusive
        // timestamp, so maxTimestamp+1 is the exclusive upper bound matching replaceRange's [lo, hiExcl).
        final long dLo = Math.max(deleteOp.getTimeRangeLo(), tableWriter.getMinTimestamp());
        final long dHiExcl = Math.min(deleteOp.getTimeRangeHiExcl(), tableWriter.getMaxTimestamp() + 1);
        if (dLo >= dHiExcl) {
            // Interval falls entirely outside the populated range: nothing to delete.
            return 0;
        }
        final int timestampIndex = tableWriter.getMetadata().getTimestampIndex();
        return tableWriter.replaceRange(dLo, dHiExcl, null, null, timestampIndex, executionContext);
    }

    public long executeUpdate(TableWriter tableWriter, CharSequence updateSql, long seqTxn) throws SqlException {
        final TableToken tableToken = tableWriter.getTableToken();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            executionContext.remapTableNameResolutionTo(tableToken);
            final CompiledQuery compiledQuery = compiler.compile(updateSql, executionContext);
            try (UpdateOperation updateOperation = compiledQuery.getUpdateOperation()) {
                updateOperation.withSqlStatement(updateSql);
                updateOperation.withContext(executionContext);
                return tableWriter.apply(updateOperation, seqTxn);
            }
        }
        // Do not catch the exception and mark transaction as committed
        // it can be transient, like table does not exist and should be retried.
    }

    public BindVariableService getBindVariableService() {
        return bindVariableService;
    }

    /**
     * Clears the apply context's tracker and returns it to the pool.
     */
    public void releaseMemoryTracker(MemoryTracker memoryTracker) {
        executionContext.setMemoryTracker(null);
        Misc.free(memoryTracker);
    }

    public void resetRnd(long seed0, long seed1) {
        rnd.reset(seed0, seed1);
    }

    public void setNowAndFixClock(long now, int nowTimestampType) {
        executionContext.setNowAndFixClock(now, nowTimestampType);
    }
}
