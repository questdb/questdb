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
                // Advance the sequencer txn exactly as TableWriter.apply(op, seqTxn) does. setSeqTxn before
                // the mutation so the replace commit persists THIS seqTxn; if no data txn was written
                // (empty table, or an identical-data no-op replace), force a seqTxn commit. Without this the
                // writer's persisted seqTxn never reaches this txn and ApplyWal2TableJob re-runs the DELETE
                // forever. On error, rollback and mirror apply()'s WAL-tolerable / retry handling.
                tableWriter.setSeqTxn(seqTxn);
                try {
                    final long txnBefore = tableWriter.getTxn();
                    final long deleted = replaceWithSurvivors(compiler, tableWriter, deleteOp);
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
                    // Any other throwable (an Error such as OOM, or e.g. a SqlException thrown by
                    // survivorFactory.getCursor() before any row was staged) must not escape without
                    // rolling back and marking this txn as not applied, or the writer is left dirty
                    // with an advanced in-memory seqTxn that the apply job will never retry. Guard the
                    // rollback itself so a distressed-writer failure during cleanup doesn't mask the
                    // original throwable (mirrors TableWriter.apply's broad Throwable branch).
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
