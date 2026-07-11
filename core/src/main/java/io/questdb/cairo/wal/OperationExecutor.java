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

class OperationExecutor implements Closeable {
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
                    // Mark as not applied so the apply job can retry.
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
     *       {@code getMinTimestamp()}/{@code getMaxTimestamp()}, partition floor / next-floor otherwise). For
     *       contiguous or split partitions this is the exact set the replace would reject; under a CALENDAR
     *       GAP it is a sound SUPERSET (may over-convert a gap-adjacent Parquet partition the delete does not
     *       touch - safe, never under-converts). See the per-partition comment for the bound detail.</li>
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
                // true UPPER bound on this partition's max data ts). These reproduce the replace path's
                // fully-covered test (Task 2.2, TableWriter.processO3Block) exactly for contiguous/split
                // partitions. Under a CALENDAR GAP (missing partitions between two physical ones)
                // next-floor-minus-one is looser than the replace path's calendar-aware
                // getCurrentPartitionMaxTimestamp, so this set is a sound SUPERSET: it may over-convert a
                // Parquet partition adjacent to a gap that the delete does not touch (safe - never
                // under-converts, never data loss), but never leaves a to-be-rewritten Parquet partition
                // unconverted. An exact match would need a public calendar-ceiling accessor on TableWriter.
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
     * Whole-range survivor replace: overwrites the table's whole populated timestamp range
     * {@code [minTimestamp, maxTimestamp+1)} with the survivor rows.
     * <p>
     * <b>Memory note (deferred to Task 2.1):</b> a single whole-table survivor stage copies every surviving
     * row into O3 memory at once, which can be heavy for a large table. The per-partition fast path that
     * bounds the staged set to one partition is Task 2.1; this whole-range version is correct for all
     * predicates and passes every case.
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

        // hi is EXCLUSIVE, so maxTimestamp+1 keeps the max-timestamp row inside the replaced range. Every
        // survivor is a subset of the table's rows, so its timestamp is in [minTimestamp, maxTimestamp]
        // and satisfies replaceRange's [lo, hiExcl) contract.
        final long loInclusive = tableWriter.getMinTimestamp();
        final long hiExclusive = tableWriter.getMaxTimestamp() + 1;
        try (RecordCursor survivorCursor = survivorFactory.getCursor(executionContext)) {
            return tableWriter.replaceRange(loInclusive, hiExclusive, survivorCursor, copier, timestampCursorIndex, executionContext);
        }
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
