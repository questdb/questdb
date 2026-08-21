/*+****************************************************************************
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

package io.questdb.cutlass.qwp.server;

import io.questdb.Telemetry;
import io.questdb.TelemetryEvent;
import io.questdb.TelemetryOrigin;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitFailedException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.QwpWalAppender;
import io.questdb.cutlass.line.tcp.SymbolCache;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.cutlass.qwp.protocol.QwpArrayColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseUtf8SequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.WeakClosableObjectPool;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.TelemetryTask;

/**
 * Cache for table update details in QWP v1 processing.
 */
public class QwpTudCache implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(QwpTudCache.class);
    private final boolean autoCreateNewColumns;
    private final boolean autoCreateNewTables;
    private final long commitInterval;
    private final DefaultColumnTypes defaultColumnTypes;
    private final int defaultPartitionBy;
    private final CairoEngine engine;
    private final long maxUncommittedRows;
    private final StringSink tableNameUtf16 = new StringSink();
    private final LowerCaseUtf8SequenceObjHashMap<WalTableUpdateDetails> tableUpdateDetails = new LowerCaseUtf8SequenceObjHashMap<>();
    private final Telemetry<TelemetryTask> telemetry;
    private volatile int cachedTableCount;
    // Optional callback mirroring commitAll(consumer)'s: invoked after a
    // successful salvage commit (see salvageBufferedRows), which bypasses
    // commitAll/commitIfMaxUncommittedRowsReached. Set by
    // QwpIngressProcessorState so its durable-ack bookkeeping sees the
    // salvaged txn; UDP receivers leave it null (no ack channel to update).
    // QwpWalAppender's internal threshold commit (appendToWalColumnar ->
    // tud.commitIfMaxUncommittedRowsCountReached()) also has no direct consumer
    // hook; the commit entry points reconcile that advance through
    // reportCommittedTxn() before acknowledging the frame.
    private CommittedTxnConsumer committedTxnConsumer;
    private MemoryMARW ddlMem;
    private boolean isDistressed = false;
    private Path path;
    private WeakClosableObjectPool<SymbolCache> symbolCachePool;

    public QwpTudCache(
            CairoEngine engine,
            boolean autoCreateNewColumns,
            boolean autoCreateNewTables,
            DefaultColumnTypes defaultColumnTypes,
            int defaultPartitionBy
    ) {
        this(
                engine,
                autoCreateNewColumns,
                autoCreateNewTables,
                defaultColumnTypes,
                defaultPartitionBy,
                -1,
                Long.MAX_VALUE
        );
    }

    public QwpTudCache(
            CairoEngine engine,
            boolean autoCreateNewColumns,
            boolean autoCreateNewTables,
            DefaultColumnTypes defaultColumnTypes,
            int defaultPartitionBy,
            long commitInterval,
            long maxUncommittedRows
    ) {
        try {
            this.ddlMem = Vm.getCMARWInstance();
            this.path = new Path();
            this.engine = engine;
            this.telemetry = engine.getTelemetry();
            this.autoCreateNewColumns = autoCreateNewColumns;
            this.autoCreateNewTables = autoCreateNewTables;
            this.commitInterval = commitInterval;
            this.defaultColumnTypes = defaultColumnTypes;
            this.defaultPartitionBy = defaultPartitionBy;
            this.maxUncommittedRows = maxUncommittedRows;
            this.symbolCachePool = new WeakClosableObjectPool<>(
                    () -> new SymbolCache(engine.getConfiguration().getMicrosecondClock(), 10_000),
                    5
            );
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public void clear() {
        ObjList<Utf8Sequence> keys = tableUpdateDetails.keys();
        if (!isDistressed) {
            for (int i = 0, n = keys.size(); i < n; i++) {
                Utf8Sequence tableName = keys.get(i);
                WalTableUpdateDetails tud = tableUpdateDetails.get(tableName);
                try {
                    tud.rollback();
                } catch (Throwable th) {
                    LOG.error().$("could not rollback [table=").$(tableName).$(", e=").$(th).I$();
                    isDistressed = true;
                }
            }
        }
        if (isDistressed) {
            for (int i = 0, n = keys.size(); i < n; i++) {
                Utf8Sequence tableName = keys.get(i);
                WalTableUpdateDetails tud = tableUpdateDetails.get(tableName);
                try {
                    Misc.free(tud);
                } catch (Throwable th) {
                    // Freeing a discarded writer still closes it, rolling back
                    // its buffered rows through real file IO that can fail on
                    // ENOSPC/EIO. Swallow so the loop frees the remaining
                    // entries and the map/flag/count reset below still runs.
                    // An escape here would wedge the cache -- entries kept,
                    // isDistressed latched true, cachedTableCount stale -- and
                    // abort the per-message finally that routes a distressed
                    // commit into clear(). Mirrors reset(); the commit loops
                    // rely on this branch to absorb their own free failures.
                    LOG.error().$("could not close discarded writer [table=").$(tableName)
                            .$(", e=").$safe(th.getMessage()).I$();
                }
            }
            tableUpdateDetails.clear();
            cachedTableCount = 0;
            isDistressed = false;
        }
    }

    @Override
    public void close() {
        reset();
        tableUpdateDetails.clear();
        cachedTableCount = 0;
        // Thread the frees so one failing close cannot skip the rest; the
        // first failure carries the later ones as suppressed and rethrows
        // at the end.
        final var ddlMemToFree = ddlMem;
        ddlMem = null;
        Throwable cleanupFailure = Misc.freeBestEffort(null, ddlMemToFree);
        final var pathToFree = path;
        path = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, pathToFree);
        final var symbolCachePoolToFree = symbolCachePool;
        symbolCachePool = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, symbolCachePoolToFree);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    /**
     * Commits all cached tables. Aborts on the first non-dropped-table commit
     * failure, leaving the remaining tables uncommitted. Suitable for callers
     * that propagate the error to the client (e.g. HTTP/WebSocket).
     */
    public void commitAll() throws Throwable {
        commitAll(null);
    }

    /**
     * Same as {@link #commitAll()} but also reports committed sequencer txns to
     * {@code consumer}. For every table it commits, it compares the writer's last
     * seqTxn against the last seqTxn already reported for that table and invokes
     * {@code consumer} when it has advanced -- whether or not this call is the one
     * that committed it, and whether or not the table had uncommitted rows. A table
     * with nothing to flush must not be skipped: the advance can come from a commit
     * made outside this class, and losing it lets a durable ack cover a WAL segment
     * the upload tracker never saw (#7482); see {@code reportCommittedTxn}. Used by
     * QWP durable-ack tracking to record which client messages still need an
     * object-store upload.
     */
    public void commitAll(CommittedTxnConsumer consumer) throws Throwable {
        ObjList<Utf8Sequence> keys = tableUpdateDetails.keys();
        Utf8Sequence discardedTableName = null;
        for (int i = 0; i < keys.size(); ) {
            Utf8Sequence tableName = keys.getQuick(i);
            int keyIndex = tableUpdateDetails.keyIndex(tableName);
            WalTableUpdateDetails tud = tableUpdateDetails.valueAt(keyIndex);
            // Capture before the commit: a table-dropped commit fails without
            // committing, and freeing a commitOnClose=false TUD below rolls its
            // buffered rows back, so we must remember whether rows are about to
            // be discarded.
            final boolean hadBufferedRows = !tud.isFirstRow();
            try {
                if (!tud.isDropped()) {
                    tud.commit(false);
                    reportCommittedTxn(tud, consumer);
                }
            } catch (CommitFailedException e) {
                if (!e.isTableDropped()) {
                    throw e.getReason();
                }
                tud.setIsDropped();
            }

            if (tud.isDropped()) {
                tableUpdateDetails.removeAtQuick(keyIndex, i);
                cachedTableCount = tableUpdateDetails.size();
                // The free below rolls back this dropped table's buffered rows.
                // On the QWP deferred-ack path the caller must NOT clear its
                // uncommitted-deferred-rows clamp, or the cumulative durable-ack
                // would cover the discarded rows (a phantom ack -> silent data
                // loss). Propagate after the loop so the client replays the
                // group (at-least-once) rather than losing the rows; any other
                // tables committed above stay durable and are simply re-sent.
                if (hadBufferedRows && discardedTableName == null) {
                    discardedTableName = tableName;
                }
                Misc.free(tud);
            } else {
                i++;
            }
        }
        if (discardedTableName != null) {
            throw CairoException.nonCritical()
                    .put("dropped table discarded buffered rows, cannot acknowledge: ")
                    .put(discardedTableName);
        }
    }

    public void commitIfMaxUncommittedRowsReached(CommittedTxnConsumer consumer) throws Throwable {
        ObjList<Utf8Sequence> keys = tableUpdateDetails.keys();
        Utf8Sequence discardedTableName = null;
        for (int i = 0; i < keys.size(); ) {
            Utf8Sequence tableName = keys.getQuick(i);
            int keyIndex = tableUpdateDetails.keyIndex(tableName);
            WalTableUpdateDetails tud = tableUpdateDetails.valueAt(keyIndex);
            // Capture before the forced commit: a table-dropped commit fails
            // without committing, and freeing the TUD below rolls its buffered
            // rows back, so we must remember whether rows are about to be
            // discarded.
            final boolean hadBufferedRows = !tud.isFirstRow();
            try {
                if (!tud.isDropped()) {
                    // The guard belongs to the commit (an empty writer has nothing to
                    // flush), not to reporting: a prior force-commit inside the appender
                    // may have left work that is committed but unreported.
                    if (hadBufferedRows) {
                        tud.commitIfMaxUncommittedRowsCountReached();
                    }
                    reportCommittedTxn(tud, consumer);
                }
            } catch (CommitFailedException e) {
                if (!e.isTableDropped()) {
                    throw e.getReason();
                }
                tud.setIsDropped();
            }

            if (tud.isDropped()) {
                tableUpdateDetails.removeAtQuick(keyIndex, i);
                cachedTableCount = tableUpdateDetails.size();
                // See commitAll: a mid-group forced commit that discards a
                // dropped table's buffered rows must not let the deferred-ack
                // clamp release, or the group-closing commit would phantom-ack
                // the discarded rows. Propagate so the client replays.
                if (hadBufferedRows && discardedTableName == null) {
                    discardedTableName = tableName;
                }
                Misc.free(tud);
            } else {
                i++;
            }
        }
        if (discardedTableName != null) {
            throw CairoException.nonCritical()
                    .put("dropped table discarded buffered rows, cannot acknowledge: ")
                    .put(discardedTableName);
        }
    }

    /**
     * Commits all cached tables, continuing past per-table failures so that
     * one table's error does not prevent the remaining tables from being
     * committed. Errors are logged. Suitable for fire-and-forget callers
     * (e.g. UDP receivers) where there is no client to report the error to.
     */
    public void commitAllBestEffort() {
        ObjList<Utf8Sequence> keys = tableUpdateDetails.keys();
        for (int i = 0; i < keys.size(); ) {
            Utf8Sequence tableName = keys.getQuick(i);
            int keyIndex = tableUpdateDetails.keyIndex(tableName);
            WalTableUpdateDetails tud = tableUpdateDetails.valueAt(keyIndex);
            try {
                if (!tud.isDropped()) {
                    tud.commit(false);
                }
            } catch (CommitFailedException e) {
                if (e.isTableDropped()) {
                    tud.setIsDropped();
                } else {
                    tud.setWriterInError();
                    LOG.error().$("commit error [table=").$(tableName).$(", e=").$(e.getReason()).I$();
                }
            } catch (Throwable t) {
                tud.setWriterInError();
                LOG.error().$("commit error [table=").$(tableName).$(", e=").$safe(t.getMessage()).I$();
            }

            // A DROP is detected the moment the name registry marks the token
            // dropped (getTableTokenByDirName returns null before any physical
            // dir purge). This also covers the zero-row dropped-table case,
            // where commit() short-circuits and throws nothing, and a distressed
            // writer that throws a plain CairoException after DROP; without it
            // the loop would retry and log the same table forever. This is a
            // fire-and-forget path (no ack), so no rows are silently acknowledged.
            //
            // A writer whose commit failed for any other reason is evicted as
            // well: retrying generally cannot succeed for a distressed or
            // out-of-date writer, and this fire-and-forget path has no client
            // to report to -- the next datagram rebuilds a fresh entry.
            //
            // That includes the one TRANSIENT failure, a role-derived
            // read-only refusal (TableUpdateDetails.commit() throws
            // readOnlyAccess() before marking the writer in error), and the
            // eviction there is deliberate, not collateral: ENT quiesces this
            // whole loop on demote (switchRole publishes acceptOpen=false
            // before runSerially's commit check), so the refusal can only fire
            // in the narrow flip window -- at most one tick, discarding at
            // most one commit window's rows, within the UDP no-ack contract.
            // Retaining the entry instead would keep an already-acquired
            // writer absorbing rows on a node whose getWalWriter fence (ENT
            // refuses acquisition while read-only) exists precisely to block
            // client writes there -- and would inject those rows on a later
            // promote. The WS path answers this same refusal by severing the
            // connection (rejectCairoError -> roleChangeClosePending);
            // eviction is this ack-less path's analog of that close.
            if (!tud.isDropped() && (tud.isWriterInError() || isTableTokenStale(tud))) {
                tud.setIsDropped();
            }

            if (tud.isDropped()) {
                tableUpdateDetails.removeAtQuick(keyIndex, i);
                cachedTableCount = tableUpdateDetails.size();
                try {
                    Misc.free(tud);
                } catch (Throwable th) {
                    // Closing the evicted writer rolls back its buffered rows --
                    // real file IO that can fail on ENOSPC/EIO. The entry is
                    // already out of the map; swallowing keeps this best-effort
                    // loop alive for the remaining tables and keeps close()
                    // releasing the rest of the cache.
                    LOG.error().$("could not close evicted writer [table=").$(tableName)
                            .$(", e=").$safe(th.getMessage()).I$();
                }
            } else {
                i++;
            }
        }
    }

    public long commitWalTables(long wallClockMillis) {
        long minTableNextCommitTime = Long.MAX_VALUE;
        ObjList<Utf8Sequence> keys = tableUpdateDetails.keys();
        for (int i = 0; i < keys.size(); ) {
            Utf8Sequence tableName = keys.getQuick(i);
            int keyIndex = tableUpdateDetails.keyIndex(tableName);
            WalTableUpdateDetails tud = tableUpdateDetails.valueAt(keyIndex);
            try {
                if (!tud.isDropped()) {
                    long tableNextCommitTime = tud.commitIfIntervalElapsed(wallClockMillis);
                    wallClockMillis = tud.getMillisecondClock().getTicks();
                    if (tableNextCommitTime < minTableNextCommitTime) {
                        minTableNextCommitTime = tableNextCommitTime;
                    }
                }
            } catch (CommitFailedException e) {
                if (e.isTableDropped()) {
                    tud.setIsDropped();
                } else {
                    tud.setWriterInError();
                    LOG.error().$("commit error [table=").$(tableName).$(", e=").$(e.getReason()).I$();
                }
            } catch (Throwable t) {
                tud.setWriterInError();
                LOG.error().$("commit error [table=").$(tableName).$(", e=").$safe(t.getMessage()).I$();
            }

            // A DROP is detected the moment the name registry marks the token
            // dropped (getTableTokenByDirName returns null before any physical
            // dir purge). This also covers the zero-row dropped-table case,
            // where commit() short-circuits and throws nothing, and a distressed
            // writer that throws a plain CairoException after DROP; without it
            // the loop would retry and log the same table forever. This is a
            // fire-and-forget path (no ack), so no rows are silently acknowledged.
            //
            // A writer whose commit failed for any other reason is evicted as
            // well: retrying generally cannot succeed for a distressed or
            // out-of-date writer, and this fire-and-forget path has no client
            // to report to -- the next datagram rebuilds a fresh entry.
            //
            // That includes the one TRANSIENT failure, a role-derived
            // read-only refusal (TableUpdateDetails.commit() throws
            // readOnlyAccess() before marking the writer in error), and the
            // eviction there is deliberate, not collateral: ENT quiesces this
            // whole loop on demote (switchRole publishes acceptOpen=false
            // before runSerially's commit check), so the refusal can only fire
            // in the narrow flip window -- at most one tick, discarding at
            // most one commit window's rows, within the UDP no-ack contract.
            // Retaining the entry instead would keep an already-acquired
            // writer absorbing rows on a node whose getWalWriter fence (ENT
            // refuses acquisition while read-only) exists precisely to block
            // client writes there -- and would inject those rows on a later
            // promote. The WS path answers this same refusal by severing the
            // connection (rejectCairoError -> roleChangeClosePending);
            // eviction is this ack-less path's analog of that close.
            if (!tud.isDropped() && (tud.isWriterInError() || isTableTokenStale(tud))) {
                tud.setIsDropped();
            }

            if (tud.isDropped()) {
                tableUpdateDetails.removeAtQuick(keyIndex, i);
                cachedTableCount = tableUpdateDetails.size();
                try {
                    Misc.free(tud);
                } catch (Throwable th) {
                    // Closing the evicted writer rolls back its buffered rows --
                    // real file IO that can fail on ENOSPC/EIO. The entry is
                    // already out of the map; swallowing keeps this best-effort
                    // loop alive for the remaining tables and keeps close()
                    // releasing the rest of the cache.
                    LOG.error().$("could not close evicted writer [table=").$(tableName)
                            .$(", e=").$safe(th.getMessage()).I$();
                }
            } else {
                i++;
            }
        }
        return minTableNextCommitTime;
    }

    public WalTableUpdateDetails getTableUpdateDetails(
            SecurityContext securityContext,
            Utf8Sequence tableNameUtf8,
            ObjList<QwpColumnDef> schema,
            QwpTableBlockCursor cursor,
            int maxTables
    ) {
        int key = tableUpdateDetails.keyIndex(tableNameUtf8);
        if (key < 0) {
            WalTableUpdateDetails tud = tableUpdateDetails.valueAt(key);
            if (!isTableTokenStale(tud)) {
                try {
                    applyPendingStructureChanges(tud);
                } catch (Throwable th) {
                    // Two ways to get here. (1) goActive() failed: the writer is
                    // distressed (the flag never clears) while the table is alive,
                    // so no staleness check would ever evict this entry and the
                    // table would be wedged on this receiver until restart.
                    // (2) goActive() replayed a concurrent RENAME (see
                    // applyPendingStructureChanges): buffered rows are already
                    // salvaged and the entry must not serve another lookup under
                    // the old name. Either way: evict and free the entry --
                    // rolling back any still-uncommitted rows -- and rethrow so
                    // the QWP layer refuses the frame (the deferred-ack clamp
                    // holds; the client replays). The next lookup acquires a
                    // fresh writer from the pool.
                    tableUpdateDetails.removeAt(key);
                    cachedTableCount = tableUpdateDetails.size();
                    Misc.free(tud, th);
                    throw th;
                }
                return tud;
            }
            evictStaleTud(key, tableNameUtf8, tud);
            key = tableUpdateDetails.keyIndex(tableNameUtf8);
        }

        if (tableUpdateDetails.size() >= maxTables) {
            throw CairoException.nonCritical()
                    .put("too many distinct tables, limit: ").put(maxTables);
        }

        tableNameUtf16.clear();
        Utf8s.utf8ToUtf16(tableNameUtf8, tableNameUtf16);
        TableToken tableToken = getOrCreateTable(securityContext, tableNameUtf16, schema, cursor);
        if (tableToken == null) {
            return null;
        }

        if (!engine.isWalTable(tableToken)) {
            throw CairoException.schemaMismatch().put("cannot insert into non-WAL table: ").put(tableNameUtf16);
        }

        TelemetryTask.store(telemetry, TelemetryOrigin.ILP_TCP, TelemetryEvent.ILP_RESERVE_WRITER);
        path.of(engine.getConfiguration().getDbRoot());

        // Copy table name to heap - needed for WalTableUpdateDetails and cache key
        Utf8String tableNameCopy = Utf8String.newInstance(tableNameUtf8);

        TableWriterAPI walWriter = engine.getWalWriter(tableToken);
        WalTableUpdateDetails tud = null;
        try {
            tud = new WalTableUpdateDetails(
                    engine,
                    securityContext,
                    walWriter,
                    defaultColumnTypes,
                    tableNameCopy,
                    symbolCachePool,
                    commitInterval,
                    false,
                    maxUncommittedRows
            );
            tableUpdateDetails.putAt(key, tableNameCopy, tud);
            cachedTableCount = tableUpdateDetails.size();
            return tud;
        } catch (Throwable th) {
            // Fold a close-IO failure (rollback-on-close can hit ENOSPC/EIO)
            // into the primary as suppressed instead of masking it -- matches
            // the sibling free on the applyPendingStructureChanges failure path
            // above.
            Misc.free(tud != null ? tud : walWriter, th);
            throw th;
        }
    }

    public void reset() {
        ObjList<Utf8Sequence> keys = tableUpdateDetails.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            Utf8Sequence tableName = tableUpdateDetails.keys().get(i);
            WalTableUpdateDetails tud = tableUpdateDetails.get(tableName);
            try {
                Misc.free(tud);
            } catch (Throwable th) {
                // Closing a discarded writer rolls back its buffered rows --
                // real file IO that can fail on ENOSPC/EIO. Swallowing keeps
                // this loop freeing the remaining entries and keeps reset()
                // throw-free for both callers: close() and
                // QwpIngressProcessorState.onDisconnected().
                LOG.error().$("could not close discarded writer [table=").$(tableName)
                        .$(", e=").$safe(th.getMessage()).I$();
            }
        }
        tableUpdateDetails.clear();
        cachedTableCount = 0;
    }

    /**
     * Registers the callback invoked after a successful salvage commit in
     * {@link #salvageBufferedRows}, called from both {@link #evictStaleTud}
     * and {@link #applyPendingStructureChanges}. See {@link #committedTxnConsumer}.
     */
    public void setCommittedTxnConsumer(CommittedTxnConsumer consumer) {
        this.committedTxnConsumer = consumer;
    }

    public void setDistressed() {
        this.isDistressed = true;
    }

    /**
     * Number of tables currently held in the cache.
     */
    public int size() {
        return cachedTableCount;
    }

    /**
     * Callback invoked by {@link #commitAll(CommittedTxnConsumer)} and by
     * {@link #commitIfMaxUncommittedRowsReached(CommittedTxnConsumer)} for every
     * table whose sequencer txn is ahead of the last seqTxn reported for it, even
     * when the commit that advanced it happened in an earlier call or outside this
     * class. Successful stale-writer salvage commits also invoke the callback.
     * <p>
     * An implementation must not re-enter the cache's commit entry points. The
     * watermark advances only after {@code accept} returns, so a re-entrant consumer
     * finds the same seqTxn still unreported and recurses.
     */
    @FunctionalInterface
    public interface CommittedTxnConsumer {
        void accept(String tableName, String tableDirName, long seqTxn);
    }

    /**
     * Hands the consumer every seqTxn advance this connection has produced for
     * {@code tud} that it has not already seen.
     * <p>
     * This reconciles against the last REPORTED seqTxn rather than trying to detect
     * commit events, because not every commit happens here: {@code QwpWalAppender}
     * force-commits inside the append when a table crosses
     * {@code qwp.max.uncommitted.rows}, outside any wrapper in this class. The two
     * event-detecting proxies this method replaces both went blind to that commit --
     * {@code isFirstRow()} because the force-commit drains the writer, and the
     * local seqTxn bracket because the advance happened before it was taken -- so the
     * txn never reached the ack / durable-upload watermarks and a durable ack could
     * be issued over a WAL segment the upload tracker never saw (#7482).
     * <p>
     * Reconciliation is also why a future commit site cannot reintroduce the bug:
     * correctness depends on the writer's seqTxn, not on the caller remembering to
     * bracket anything.
     * <p>
     * A reported advance is not necessarily a data commit. Structural txns share the
     * seqTxn space with data txns, so the implicit ALTER {@code QwpWalAppender} runs for
     * a column the frame introduces advances the writer's seqTxn as well; a
     * FLAG_DEFER_COMMIT frame below the max-uncommitted-rows cap therefore reports that
     * metadata-only txn with no rows behind it. This method accepts such a txn rather
     * than gating on it, on three counts.
     * The ALTER is permanent work this connection produced, and a rollback of the
     * deferred group does not undo it. No DURABLE ack can outrun an upload:
     * {@code QwpIngressProcessorState.collectDurableProgress} forwards only the
     * registry's own uploadedSeqTxn, never the pending value, and on the normal path
     * the group-closing {@link #commitAll(CommittedTxnConsumer)} supersedes the pending
     * entry with the data txn -- on a rollback or error exit, and on a normal exit that
     * commits no data txn for that table, the metadata-only entry instead stands until
     * {@code QwpIngressProcessorState.onDurableAckSent()} prunes it once the upload
     * covers that seqTxn, or {@code onDisconnected()} clears it. (The cumulative OK ack
     * is a different watermark and is not upload-gated at all; this count is about the
     * durable ack only.) And the upload watermark already had to clear structure txns
     * before this change: a data txn reported after an implicit ALTER is numbered above
     * it, so a registry that could not advance past a metadata-only txn would already
     * stall on the far commoner non-deferred ALTER-then-insert frame. That last count
     * is an assumption about the enterprise uploader -- this repository ships only
     * {@code DefaultDurableAckRegistry}, which is disabled and returns -1 -- so the
     * first two counts have to carry the argument on their own. Gating instead on "a
     * commit drained the writer in this call" would reinstate #7482 for a frame whose
     * force-commit is followed by further rows.
     * <p>
     * The one visible effect is a delay: a demote landing between such a frame and its
     * group-closing commit now waits for the ALTER's upload coverage, bounded for a live
     * client by {@code QwpIngressProcessorState.ROLE_CHANGE_CLOSE_UPLOAD_GRACE_MICROS},
     * where the previous shape waited for nothing on that table. That constant is not a
     * wall-clock teardown bound, as its own declaration comment states: the deferred
     * close re-evaluates coverage-or-expiry only on inbound, recv-driven re-entry, so a
     * silent peer lingers deferred until the transport idle reaper tears the connection
     * down.
     * <p>
     * The consumer runs BEFORE the watermark advances, so at THIS layer a consumer that
     * throws leaves the txn unreported and the next call to this method offers it again,
     * at the cost of a duplicate set of overwriting map puts in
     * {@code QwpIngressProcessorState.recordCommittedTable}. On the current production
     * wiring that next call never comes: {@code QwpIngressProcessorState.commit()} and
     * {@code commitIfMaxUncommittedRowsReached()} call {@code setDistressed()} when the
     * consumer throws, and the frame handler's {@code state.clear()} then frees every
     * TUD, so the txn is lost under either ordering -- harmlessly, because the frame is
     * NACKed and the client replays from its acked watermark. This ordering is defence
     * in depth for a future caller that does not distress the cache, not a recovery that
     * runs today.
     */
    private static void reportCommittedTxn(WalTableUpdateDetails tud, CommittedTxnConsumer consumer) {
        if (consumer == null || tud.isDropped()) {
            return;
        }
        final long seqTxn = tud.getLastSeqTxn();
        // getLastSeqTxn() is negative when the writer is gone or has no txn yet.
        if (seqTxn < 0 || seqTxn <= tud.getLastReportedSeqTxn()) {
            return;
        }
        consumer.accept(
                tud.getTableToken().getTableName(),
                tud.getTableToken().getDirName(),
                seqTxn
        );
        tud.setLastReportedSeqTxn(seqTxn);
    }

    private static boolean isValidQwpSchemaColumnName(QwpColumnDef columnDef, int maxFileNameLength) {
        final String columnName = columnDef.getName();
        if (columnName.isEmpty()) {
            final byte typeCode = columnDef.getTypeCode();
            return typeCode == QwpConstants.TYPE_TIMESTAMP || typeCode == QwpConstants.TYPE_TIMESTAMP_NANOS;
        }
        return TableUtils.isValidColumnName(columnName, maxFileNameLength);
    }

    /**
     * Bring a cached table's writer up to date with structure changes committed
     * since it was last refreshed.
     * <p>
     * Nothing else on this path does. {@link #isTableTokenStale} only notices a
     * change of table IDENTITY -- a DROP mints a new {@link TableToken} -- while
     * {@code ALTER TABLE ... ALTER COLUMN ... TYPE} keeps the token and merely
     * bumps the metadata version. The cached writer therefore kept the column's
     * old type indefinitely, and rows of the new type were converted against it:
     * refused on QWP/WebSocket, and silently dropped on QWP/UDP, which has no
     * ack channel to refuse through. The UDP receiver makes that unbounded --
     * it holds ONE cache for every sender, with no reconnect to heal it.
     * <p>
     * Gated on the sequencer's transaction counter. The counter advances on
     * EVERY committed txn -- structure changes, other writers' data commits,
     * and this entry's own commits -- so the gate does not mean "a structure
     * change happened": on the per-frame WS commit path the previous frame's
     * own commit reopens it, and goActive() runs about once per frame. What
     * the gate does avoid is replaying the change log more than once within
     * one commit batch, and any replay at all on idle tables. An up-to-date
     * goActive() is cheap: when the writer's structure version is current the
     * sequencer answers with EmptyOperationCursor, no IO -- but it still
     * costs a sequencer acquire/release per open gate.
     * <p>
     * Any failure here -- a table dropped concurrently, or a transient I/O error
     * reading the change log -- makes {@code goActive} throw and leaves the
     * writer permanently distressed. The gate is advanced only after a
     * successful call, so a failed replay is retried by the next lookup rather
     * than latched as done; the caller evicts and frees this entry on failure
     * and rethrows, refusing the frame rather than acknowledging it -- the safe
     * direction. Relying on {@link #isTableTokenStale} alone would not do: a
     * transient failure leaves the table alive, so the token never goes stale
     * and the entry would stay wedged in the cache with a distressed writer.
     * <p>
     * A renamed table's writer is deliberately left untouched: the change log
     * carries the RENAME TABLE entry, and replaying it here would rebind the
     * writer's token and defeat the sequencer's token-mismatch check, silently
     * committing rows keyed by the old name into the renamed table. See the
     * directory-name guard below.
     * When the rename lands concurrently with the lookup -- the registry not
     * yet updated when the guard reads it -- the replay does rebind the
     * writer; the token comparison around goActive() below catches exactly
     * that case, salvages and refuses. See the in-method comment.
     */
    private void applyPendingStructureChanges(WalTableUpdateDetails tud) {
        if (!(tud.getWriter() instanceof WalWriter walWriter)) {
            return;
        }
        final TableToken cachedToken = tud.getTableToken();
        // Never bring a renamed table's writer up to date here: the change log
        // contains the RENAME TABLE entry, and replaying it rebinds the writer's
        // token -- defeating the sequencer's token-mismatch check and silently
        // committing rows keyed by the OLD name into the renamed table. Leaving
        // the writer as-is preserves the loud commit-time
        // TableReferenceOutOfDateException.
        if (engine.getTableTokenByDirName(cachedToken.getDirName()) != cachedToken) {
            return;
        }
        final long seqTxn = engine.getTableSequencerAPI()
                .getTxnTracker(cachedToken)
                .getSeqTxn();
        if (seqTxn == tud.getLastStructureCheckSeqTxn()) {
            return;
        }
        final TableToken tokenBeforeReplay = walWriter.getTableToken();
        walWriter.goActive();
        if (walWriter.getTableToken() != tokenBeforeReplay) {
            // goActive() replayed a RENAME that the registry guard above could
            // not see yet: CairoEngine.rename() publishes the sequencer txn
            // BEFORE it updates the name registry, and this lookup ran inside
            // that window. The writer is now bound to the renamed table;
            // letting the lookup proceed would silently commit rows keyed by
            // the OLD name into it, with OK acks, for the life of the
            // connection. Salvage the buffered rows -- they belong to this
            // same physical table -- then throw: the caller evicts the entry
            // and refuses the frame, and the client's retry lands after the
            // registry has caught up. Rebuilding within this lookup instead
            // would re-acquire a writer that rebinds the same way. On the WS
            // deferred-ack path the salvaged rows' frames were never acked
            // before this throw, so the client's replay re-delivers them into
            // whatever the old name resolves to after the rename -- duplicate
            // delivery across tables, consistent with commitAll's
            // partial-commit replay posture.
            if (salvageBufferedRows(tud, walWriter)) {
                reportCommittedTxn(tud, committedTxnConsumer);
            }
            throw CairoException.nonCritical()
                    .put("table is being renamed, cannot ingest [table=")
                    .put(tokenBeforeReplay.getTableName())
                    .put(']');
        }
        // Only a successful replay advances the gate: a failure must be retried
        // by the next lookup, not latched as done.
        tud.setLastStructureCheckSeqTxn(seqTxn);
    }

    // Evicts a stale cached entry (see isTableTokenStale): either the table was
    // DROPped -- its token resolves by neither name nor directory -- or it was
    // RENAMEd and the old name was reused by a new table. A pure rename that
    // does NOT reuse the old name is not stale and never reaches here from the
    // lookup path -- its entry stays cached and every commit through it fails
    // loudly with TableReferenceOutOfDateException; on the UDP fire-and-forget
    // loops that failure latches writerInError, which evicts the entry in the
    // same commit pass. A dropped table's rows cannot be re-homed and are
    // discarded; a renamed table's rows are salvaged below instead.
    private void evictStaleTud(int key, Utf8Sequence tableNameUtf8, WalTableUpdateDetails tud) {
        final boolean hadBufferedRows = !tud.isFirstRow();
        final boolean isRenamed = engine.getTableTokenByDirName(tud.getTableToken().getDirName()) != null;
        boolean isSalvaged = false;
        Throwable reportFailure = null;
        if (hadBufferedRows && isRenamed && tud.getWriter() instanceof WalWriter walWriter) {
            // The writer's table is alive under a new name, so its buffered rows
            // are salvageable. goActive() replays the rename into the writer --
            // rebinding its token -- after which the sequencer accepts the commit
            // and the rows land in the renamed table: the same table identity
            // that accepted them. The entry is evicted right after, so the healed
            // token can never serve another lookup under the old name.
            try {
                walWriter.goActive();
                isSalvaged = salvageBufferedRows(tud, walWriter);
            } catch (Throwable th) {
                LOG.error().$("could not salvage buffered rows of a renamed table [table=")
                        .$(tableNameUtf8).$(", e=").$safe(th.getMessage()).I$();
            }
            if (isSalvaged) {
                try {
                    reportCommittedTxn(tud, committedTxnConsumer);
                } catch (Throwable th) {
                    // The rows are committed, but the caller must still reject the
                    // frame because durable-ack bookkeeping did not accept the txn.
                    // Preserve that failure across the eviction cleanup below rather
                    // than misreporting the committed rows as discarded.
                    reportFailure = th;
                }
            }
        }
        tableUpdateDetails.removeAt(key);
        cachedTableCount = tableUpdateDetails.size();
        try {
            // Freeing a commitOnClose=false TUD rolls back whatever is still
            // uncommitted -- real file IO that can fail on ENOSPC/EIO.
            Misc.free(tud);
        } catch (Throwable th) {
            // Neither branch below may let that failure escape. After a
            // successful salvage the rows are already committed and durable, so
            // an escape would turn a success into a frame refusal and the
            // client would replay rows the renamed table already holds --
            // duplicate data. In the discard branch the throw below is the more
            // informative one: it names the table and the reason the rows went
            // away, where a raw IO error refuses the frame just the same but
            // tells the operator nothing. Either way the writer is already out
            // of the map and discarded, and the condition that broke this close
            // resurfaces on the next commit through the fresh writer, which does
            // propagate. This log keeps the IO detail.
            LOG.error().$("could not close evicted stale writer [table=").$(tableNameUtf8)
                    .$(", e=").$safe(th.getMessage()).I$();
        }
        if (reportFailure != null) {
            CairoException.rethrowCleanupFailure(reportFailure);
        }
        // Rows the eviction could not re-home are gone: refuse so the QWP layer
        // rejects instead of acknowledging them; the UDP receiver has no ack and
        // simply drops the datagram.
        if (hadBufferedRows && !isSalvaged) {
            throw CairoException.nonCritical()
                    .put(isRenamed ? "renamed" : "dropped")
                    .put(" table discarded buffered rows, cannot acknowledge: ")
                    .put(tableNameUtf8);
        }
    }

    private TableToken getOrCreateTable(SecurityContext securityContext, StringSink tableNameUtf16,
                                        ObjList<QwpColumnDef> schema, QwpTableBlockCursor cursor) {
        int maxFileNameLength = engine.getConfiguration().getMaxFileNameLength();
        if (!TableUtils.isValidTableName(tableNameUtf16, maxFileNameLength)) {
            return null;
        }
        TableToken tableToken = engine.getTableTokenIfExists(tableNameUtf16);
        int status = engine.getTableStatus(path, tableToken);
        if (status != TableUtils.TABLE_EXISTS) {
            if (!autoCreateNewTables) {
                return null;
            }
            if (!autoCreateNewColumns) {
                return null;
            }

            for (int i = 0; i < schema.size(); i++) {
                if (!isValidQwpSchemaColumnName(schema.getQuick(i), maxFileNameLength)) {
                    return null;
                }
            }

            // Create table using QWP v1 schema
            QwpTableStructureAdapter tsa = new QwpTableStructureAdapter(
                    engine.getConfiguration(),
                    tableNameUtf16.toString(),
                    schema,
                    cursor,
                    defaultPartitionBy
            );

            for (int i = 0, n = tsa.getColumnCount(); i < n; i++) {
                CharSequence columnName = tsa.getColumnName(i);
                if (!TableUtils.isValidColumnName(columnName, maxFileNameLength)) {
                    return null;
                }
            }
            tableToken = engine.createTable(securityContext, ddlMem, path, true, tsa, false, TableUtils.TABLE_KIND_REGULAR_TABLE);
        }
        // A mat view with a REFRESH LIMIT accepts direct backfill into its frozen zone,
        // so it is the one non-TABLE kind this gate lets through.
        if (tableToken != null && tableToken.getType() != TableToken.Type.TABLE && !engine.isBackfillableMatView(tableToken)) {
            // schemaMismatch(), not a bare nonCritical(): the target's kind is a property of
            // the name the frame carries, so byte-identical replay hits the same refusal
            // forever. QwpIngressProcessorState.cairoExceptionStatus maps an unmarked
            // non-critical CairoException to NOT_ACCEPTING_WRITES, which the upgrade
            // processor encodes as STATUS_WRITE_ERROR and a store-and-forward sender treats
            // as RETRIABLE - so a view named where a table was expected made the client
            // reconnect and replay the doomed frame from its SF log up to
            // max_frame_rejections times (default 4, over at least a 5s dwell window),
            // stalling every frame queued behind it, before its poison-frame detector gave
            // up and halted with a PROTOCOL_VIOLATION that named the wrong cause. The
            // marker instead selects SCHEMA_MISMATCH, which the client halts on at the
            // first strike with the accurate category. The other protocol front-ends raise this same
            // refusal without the marker because they have no retriable/terminal NACK to
            // choose between: ILP/TCP disconnects and ILP/HTTP answers in the response body.
            throw CairoException.schemaMismatch()
                    .put("cannot modify ").put(tableToken.getType().keyword()).put(" [view=")
                    .put(tableToken.getTableName())
                    .put(']');
        }
        return tableToken;
    }

    private boolean isTableTokenStale(WalTableUpdateDetails tud) {
        final TableToken cachedToken = tud.getTableToken();
        final TableToken byName = engine.getTableTokenIfExists(cachedToken.getTableName());
        if (byName == cachedToken) {
            return false;
        }
        // The cached name no longer resolves to the cached table. Either the
        // table was dropped -- its directory no longer resolves either -- or
        // another live table took the name after a rename (byName is a
        // different, live token). A pure rename whose old name is NOT re-used
        // resolves the directory but not the name, and is deliberately not
        // stale: on the lookup path the entry stays cached and commits keep
        // failing with TableReferenceOutOfDateException, exactly as on master.
        // The UDP commit loops additionally evict such an entry once a failed
        // commit latches writerInError -- a deliberate change from master,
        // which retried the wedged entry forever.
        return byName != null || engine.getTableTokenByDirName(cachedToken.getDirName()) == null;
    }

    /**
     * Commits a stale entry's buffered rows through its writer AFTER
     * {@code goActive()} replayed a RENAME into it -- the rows land in the
     * renamed table: the same physical table that accepted them. Rebinds the
     * TUD's token to the writer's first, so the commit's insert authorization
     * names the table the rows actually land in, not the old name (which on
     * the evictStaleTud path already belongs to a different table). The callers
     * notify {@link #committedTxnConsumer} through {@link #reportCommittedTxn}
     * after this method confirms the commit, because this path bypasses
     * commitAll/commitIfMaxUncommittedRowsReached.
     * <p>
     * No-ops (returns false, no rebind, no commit, no notify) when the entry
     * has no buffered rows: the normal inter-batch state on the WS path, which
     * commits after every batch. Without this guard, {@code tud.commit(false)}
     * short-circuits on zero uncommitted rows without advancing the sequencer,
     * yet the caller would still notify {@link #committedTxnConsumer} with the
     * seqTxn of an earlier, already-acked commit -- keyed under the RENAMED
     * table's name instead. That plants a phantom per-table watermark and
     * makes durable-ack gating wait on a table the client never wrote to.
     *
     * @return true when the rows were committed; false when there was nothing
     * to salvage, or the commit failed (the caller frees the entry, rolling
     * back whatever remains uncommitted)
     */
    private boolean salvageBufferedRows(WalTableUpdateDetails tud, WalWriter walWriter) {
        if (tud.isFirstRow()) {
            return false;
        }
        try {
            tud.updateTableToken(walWriter.getTableToken());
            tud.commit(false);
        } catch (Throwable th) {
            LOG.error().$("could not salvage-commit buffered rows of a renamed table [table=")
                    .$safe(walWriter.getTableToken().getTableName())
                    .$(", e=").$safe(th.getMessage()).I$();
            return false;
        }
        return true;
    }

    /**
     * Table structure adapter for QWP v1 schema.
     * <p>
     * When no timestamp column is provided in the schema, this adapter automatically
     * adds a "timestamp" column as the designated timestamp. This matches the behavior
     * of the old ILP text protocol.
     */
    private static class QwpTableStructureAdapter implements TableStructure {
        private static final String DEFAULT_TIMESTAMP_FIELD = "timestamp";
        private final IntList columnTypes = new IntList();
        private final CairoConfiguration configuration;
        private final QwpTableBlockCursor cursor;
        private final IntList includedSchemaIndexes = new IntList();
        private final int outputTimestampIndex;
        private final int partitionBy;
        private final ObjList<QwpColumnDef> schema;
        private final String tableName;
        private int timestampSchemaIndex = -1;

        QwpTableStructureAdapter(CairoConfiguration configuration, String tableName, ObjList<QwpColumnDef> schema,
                                 QwpTableBlockCursor cursor, int partitionBy) {
            this.configuration = configuration;
            this.tableName = tableName;
            this.schema = schema;
            this.cursor = cursor;
            this.partitionBy = partitionBy;

            // Find designated timestamp column - empty name with TIMESTAMP or TIMESTAMP_NANOS type
            for (int i = 0, n = schema.size(); i < n; i++) {
                byte typeCode = schema.getQuick(i).getTypeCode();
                if (schema.getQuick(i).getName().isEmpty() &&
                        (typeCode == QwpConstants.TYPE_TIMESTAMP || typeCode == QwpConstants.TYPE_TIMESTAMP_NANOS)) {
                    timestampSchemaIndex = i;
                    break;
                }
            }
            for (int i = 0; i < schema.size(); i++) {
                final int columnType = getSchemaColumnType(i);
                if (columnType == ColumnType.UNDEFINED) {
                    continue;
                }
                includedSchemaIndexes.add(i);
                columnTypes.add(columnType);
            }
            outputTimestampIndex = timestampSchemaIndex == -1 ? includedSchemaIndexes.size() : includedSchemaIndexes.binarySearchUniqueList(timestampSchemaIndex);
            // If no designated timestamp found, we'll add one automatically (see getColumnCount)
        }

        @Override
        public int getColumnCount() {
            // If no timestamp column in schema, add one automatically
            return timestampSchemaIndex == -1 ? includedSchemaIndexes.size() + 1 : includedSchemaIndexes.size();
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            // If this is the auto-added timestamp column (no designated timestamp in schema)
            if (columnIndex == getTimestampIndex() && timestampSchemaIndex == -1) {
                return DEFAULT_TIMESTAMP_FIELD;
            }
            // If this is the designated timestamp column from schema, use default name
            // (the schema column name is empty for TYPE_DESIGNATED_TIMESTAMP)
            if (columnIndex == outputTimestampIndex) {
                return DEFAULT_TIMESTAMP_FIELD;
            }
            return schema.getQuick(includedSchemaIndexes.get(columnIndex)).getName();
        }

        @Override
        public int getColumnType(int columnIndex) {
            // If this is the auto-added timestamp column
            if (columnIndex == getTimestampIndex() && timestampSchemaIndex == -1) {
                return ColumnType.TIMESTAMP;
            }
            return columnTypes.get(columnIndex);
        }

        @Override
        public int getIndexBlockCapacity(int columnIndex) {
            return 0;
        }

        @Override
        public byte getIndexType(int columnIndex) {
            return 0;
        }

        @Override
        public int getMaxUncommittedRows() {
            return configuration.getMaxUncommittedRows();
        }

        @Override
        public long getO3MaxLag() {
            return configuration.getO3MaxLag();
        }

        @Override
        public int getPartitionBy() {
            return partitionBy;
        }

        @Override
        public boolean getSymbolCacheFlag(int columnIndex) {
            return configuration.getDefaultSymbolCacheFlag();
        }

        @Override
        public int getSymbolCapacity(int columnIndex) {
            return configuration.getDefaultSymbolCapacity();
        }

        @Override
        public CharSequence getTableName() {
            return tableName;
        }

        @Override
        public int getTimestampIndex() {
            // If no timestamp column in schema, it's the auto-added one at the end
            return outputTimestampIndex;
        }

        @Override
        public boolean isDedupKey(int columnIndex) {
            return false;
        }

        @Override
        public boolean isIndexed(int columnIndex) {
            return false;
        }

        @Override
        public boolean isWalEnabled() {
            return true; // QWP v1 uses WAL
        }

        private static int getArrayBatchDimensionality(
                QwpArrayColumnCursor cursor,
                int rowCount,
                CharSequence columnName
        ) {
            int batchDims = -1;
            cursor.resetRowPosition();
            for (int row = 0; row < rowCount; row++) {
                cursor.advanceRow();
                if (cursor.isNull()) {
                    continue;
                }

                final int rowDims = cursor.getNDims();
                if (batchDims == -1) {
                    batchDims = rowDims;
                } else if (batchDims != rowDims) {
                    throw CairoException.schemaMismatch()
                            .put("array dimensionality mismatch in QWP batch [column=")
                            .put(columnName)
                            .put(", expectedDims=")
                            .put(batchDims)
                            .put(", actualDims=")
                            .put(rowDims)
                            .put(']');
                }
            }
            cursor.resetRowPosition();
            return batchDims;
        }

        private int getSchemaColumnType(int schemaIndex) {
            final byte typeCode = schema.getQuick(schemaIndex).getTypeCode();
            if (typeCode == QwpConstants.TYPE_DOUBLE_ARRAY) {
                final int nDims = getArrayBatchDimensionality(
                        cursor.getArrayColumn(schemaIndex),
                        cursor.getRowCount(),
                        schema.getQuick(schemaIndex).getName()
                );
                if (nDims < 1) {
                    return ColumnType.UNDEFINED;
                }
                return ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            }
            return QwpWalAppender.mapQwpTypeToQuestDB(typeCode, cursor, schemaIndex);
        }
    }
}
