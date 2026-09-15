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

package io.questdb.griffin;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.pool.ResourcePoolSupervisor;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.model.IntrinsicModel;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.mp.continuation.CancellationBinding;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Rnd;
import io.questdb.std.Transient;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public interface SqlExecutionContext extends Sinkable, Closeable {

    // Returns true when the context doesn't require all SQL functions to be deterministic.
    // Deterministic-only functions are enforced e.g. when compiling a mat view.
    boolean allowNonDeterministicFunctions();

    void changePageFrameSizes(int minRows, int maxRows);

    default void clearCancelledFlag(AtomicBoolean expected) {
        getCircuitBreaker().clearCancelledFlag(expected);
        getSimpleCircuitBreaker().clearCancelledFlag(expected);
    }

    default void clearCancelledFlag(AtomicBoolean expected, long expectedGeneration) {
        getCircuitBreaker().clearCancelledFlag(expected, expectedGeneration);
        getSimpleCircuitBreaker().clearCancelledFlag(expected, expectedGeneration);
    }

    void clearWindowContext();

    @Override
    default void close() {
    }

    void configureWindowContext(
            @Nullable VirtualRecord partitionByRecord,
            @Nullable RecordSink partitionBySink,
            @Transient @Nullable ColumnTypes keyTypes,
            boolean isOrdered,
            int scanDirection,
            int orderByDirection,
            boolean baseSupportsRandomAccess,
            int framingMode,
            long rowsLo,
            char rowsLoUnit,
            int rowsLoExprPos,
            int rowsLoKindPos,
            long rowsHi,
            char rowsHiUnit,
            int rowsHiExprPos,
            int rowsHiKindPos,
            int exclusionKind,
            int exclusionKindPos,
            int timestampIndex,
            int timestampType,
            boolean ignoreNulls,
            int nullsDescPos
    ) throws SqlException;

    default void containsSecret(boolean b) {
    }

    default boolean containsSecret() {
        return false;
    }

    default void copyCancelledFlagsTo(CancellationBinding circuitBreakerTarget, CancellationBinding simpleCircuitBreakerTarget) {
        getCircuitBreaker().copyCancelledFlagTo(circuitBreakerTarget);
        getSimpleCircuitBreaker().copyCancelledFlagTo(simpleCircuitBreakerTarget);
    }

    default Rnd getAsyncRandom() {
        return SharedRandom.getAsyncRandom(getCairoEngine().getConfiguration());
    }

    BindVariableService getBindVariableService();

    @NotNull
    CairoEngine getCairoEngine();

    @NotNull
    SqlExecutionCircuitBreaker getCircuitBreaker();

    boolean getCloneSymbolTables();

    Decimal128 getDecimal128();

    Decimal256 getDecimal256();

    Decimal64 getDecimal64();

    default @Nullable ExecutionState getExecutionState() {
        return null;
    }

    int getIntervalFunctionType();

    /**
     * Returns the dynamic-interval plan handoff generation: zero outside EXPLAIN, negative while
     * EXPLAIN prepares its base cursor, and positive while it renders the successfully prepared plan.
     */
    default long getIntervalPlanGeneration() {
        return 0;
    }

    int getJitMode();

    /**
     * Returns the tracker bound to the currently active workload, or
     * {@code null} between workloads. The workload entry point sets it via
     * {@link #setMemoryTracker(MemoryTracker)} on acquisition and clears it
     * on workload end.
     */
    @Nullable
    default MemoryTracker getMemoryTracker() {
        return null;
    }

    default @NotNull MessageBus getMessageBus() {
        return getCairoEngine().getMessageBus();
    }

    default TableRecordMetadata getMetadataForWrite(TableToken tableToken, long desiredVersion) {
        return getCairoEngine().getLegacyMetadata(tableToken, desiredVersion);
    }

    default TableRecordMetadata getMetadataForWrite(TableToken tableToken) {
        return getMetadataForWrite(tableToken, TableUtils.ANY_TABLE_VERSION);
    }

    long getMicrosecondTimestamp();

    long getNanosecondTimestamp();

    /**
     * Gets the current timestamp with specified precision.
     *
     * @param timestampType the timestamp precision type (micros or nanos)
     * @return current timestamp value in the specified precision
     */
    long getNow(int timestampType);

    int getNowTimestampType();

    int getPageFrameMaxRows();

    int getPageFrameMinRows();

    QueryFutureUpdateListener getQueryFutureUpdateListener();

    Rnd getRandom();

    default TableReader getReader(TableToken tableToken, long version) {
        return getCairoEngine().getReader(tableToken, version, this.getReaderPoolSupervisor());
    }

    default TableReader getReader(TableToken tableToken) {
        return getCairoEngine().getReader(tableToken, this.getReaderPoolSupervisor());
    }

    /**
     * The reader-pool supervisor that table-reader borrows made through this context are
     * attributed to (for query-scoped reader-leak detection). Carried on the execution
     * context rather than a carrier/thread local so it survives a continuation that parks
     * and resumes on a different worker. Returns {@code null} when no supervisor is active.
     */
    default ResourcePoolSupervisor<TableReader> getReaderPoolSupervisor() {
        return null;
    }

    long getRequestFd();

    @NotNull
    SecurityContext getSecurityContext();

    int getSharedQueryWorkerCount();

    @NotNull
    SqlExecutionCircuitBreaker getSimpleCircuitBreaker();

    default int getTableStatus(Path path, CharSequence tableName) {
        return getCairoEngine().getTableStatus(path, tableName);
    }

    default int getTableStatus(Path path, TableToken tableToken) {
        return getCairoEngine().getTableStatus(path, tableToken);
    }

    default TableToken getTableToken(CharSequence tableName) {
        return getCairoEngine().verifyTableName(tableName);
    }

    default TableToken getTableToken(CharSequence tableName, int lo, int hi) {
        return getCairoEngine().verifyTableName(tableName, lo, hi);
    }

    default TableToken getTableTokenIfExists(CharSequence tableName) {
        return getCairoEngine().getTableTokenIfExists(tableName);
    }

    default TableToken getTableTokenIfExists(CharSequence tableName, int lo, int hi) {
        return getCairoEngine().getTableTokenIfExists(tableName, lo, hi);
    }

    /**
     * Tells the context which name the statement being compiled uses for the table it targets - the
     * table named by {@code UPDATE <name>} or {@code ALTER TABLE <name>}. Called before that name,
     * or any other table in the statement, is resolved.
     * <p>
     * Only contexts that resolve a target differently from the name in the SQL need this; for
     * everything else it is a no-op. See {@code WalApplySqlExecutionContext}, where the stored SQL
     * may name a table that has since been renamed, or whose name now belongs to a different table.
     */
    default void setStatementTargetTableName(CharSequence tableName) {
    }

    WindowContext getWindowContext();

    int hasInterval();

    void initNow();

    boolean isCacheHit();

    // Returns false only for materialized view refresh contexts when the
    // cairo.mat.view.covering.index.enabled property is set to false.
    // All other contexts always return true.
    default boolean isCoveringIndexEnabled() {
        return true;
    }

    // Returns true when the current compile is the CREATE-time or refresh-time
    // compile of a live view's SELECT. Compile-time switch that lets window
    // function factories opt into live-view-only machinery (e.g. the
    // tombstone value-layout slot that drives anchor-driven compaction)
    // and lets WhereClauseParser suppress indexed-symbol key
    // extraction so the planner falls back to a plain FilteredRecordCursorFactory
    // shape that the incremental refresh path can handle.
    default boolean isLiveViewCompile() {
        return false;
    }

    // Returns true when where intrinsics are overridden, i.e. by a materialized view refresh
    default boolean isOverriddenIntrinsics(TableToken tableToken) {
        return false;
    }

    boolean isParallelFilterEnabled();

    boolean isParallelGroupByEnabled();

    boolean isParallelHorizonJoinEnabled();

    boolean isParallelReadParquetEnabled();

    boolean isParallelTopKEnabled();

    boolean isParallelWindowJoinEnabled();

    boolean isParquetRowGroupPruningEnabled();

    /**
     * Returns whether cached table scans may retain their compiled optimization state across
     * partition-format changes. A tolerant context accepts that Parquet row-group pruning may no
     * longer match the current table format; the ordinary row filter still preserves SQL semantics.
     */
    default boolean isPartitionFormatChangeTolerated() {
        return false;
    }

    boolean isTimestampRequired();

    default boolean isUninterruptible() {
        return false;
    }

    boolean isValidationOnly();

    boolean isWalApplication();

    /**
     * Starts a new dynamic-interval plan preparation and returns its negative generation.
     */
    default long nextIntervalPlanGeneration() {
        return 0;
    }

    // This method is used to override intrinsic values in the query execution context
    // Its initial usage is in the materialized view refresh
    // where the queried timestamp of the base table is limited to the range affected since last refresh
    default void overrideWhereIntrinsics(TableToken tableToken, IntrinsicModel intrinsicModel, int timestampType) {
    }

    RuntimeIntrinsicIntervalModel peekIntervalModel();

    void popHasInterval();

    void popIntervalModel();

    void popTimestampRequiredFlag();

    void pushHasInterval(int hasInterval);

    void pushIntervalModel(RuntimeIntrinsicIntervalModel intervalModel);

    void pushTimestampRequiredFlag(boolean flag);

    void reset();

    default void restoreCancelledFlag(
            AtomicBoolean expected,
            CancellationBinding circuitBreakerPrevious,
            CancellationBinding simpleCircuitBreakerPrevious
    ) {
        final SqlExecutionCircuitBreaker circuitBreaker = getCircuitBreaker();
        final SqlExecutionCircuitBreaker simpleCircuitBreaker = getSimpleCircuitBreaker();
        if (circuitBreaker.getCancelledFlag() == expected) {
            circuitBreaker.setCancelledFlag(circuitBreakerPrevious);
        }
        if (simpleCircuitBreaker != circuitBreaker && simpleCircuitBreaker.getCancelledFlag() == expected) {
            simpleCircuitBreaker.setCancelledFlag(simpleCircuitBreakerPrevious);
        }
    }

    void restoreToDefaultPageFrameSizes();

    void setAllowNonDeterministicFunction(boolean value);

    void setCacheHit(boolean value);

    void setCancelledFlag(AtomicBoolean cancelled);

    default void setCancelledFlag(CancellationBinding source) {
        getCircuitBreaker().setCancelledFlag(source);
        getSimpleCircuitBreaker().setCancelledFlag(source);
    }

    default void setCancelledFlag(AtomicBoolean cancelled, long generation) {
        getCircuitBreaker().setCancelledFlag(cancelled, generation);
        getSimpleCircuitBreaker().setCancelledFlag(cancelled, generation);
    }

    void setCloneSymbolTables(boolean cloneSymbolTables);

    void setIntervalFunctionType(int intervalType);

    default void setIntervalPlanGeneration(long generation) {
    }

    void setJitMode(int jitMode);

    default void setLiveViewCompile(boolean value) {
    }

    /**
     * Stashes the active per-workload memory tracker on this context. Set at
     * workload start by the entry point that called
     * {@code MemoryTrackerProvider.acquire(...)}; cleared (with {@code null})
     * at workload end so the context is ready for the next workload.
     */
    default void setMemoryTracker(@Nullable MemoryTracker tracker) {
    }

    void setNowAndFixClock(long now, int nowTimestampType);

    void setParallelFilterEnabled(boolean parallelFilterEnabled);

    void setParallelGroupByEnabled(boolean parallelGroupByEnabled);

    void setParallelHorizonJoinEnabled(boolean parallelHorizonJoinEnabled);

    void setParallelReadParquetEnabled(boolean parallelReadParquetEnabled);

    void setParallelTopKEnabled(boolean parallelTopKEnabled);

    void setParallelWindowJoinEnabled(boolean parallelWindowJoinEnabled);

    void setParquetRowGroupPruningEnabled(boolean parquetRowGroupPruningEnabled);

    void setRandom(Rnd rnd);

    /**
     * Sets the reader-pool supervisor for table-reader borrows made through this context.
     * {@link io.questdb.griffin.engine.QueryProgress} installs itself here for the duration
     * of cursor open so that readers borrowed while building the cursor are attributed to
     * the query, then restores the previous value. Default is a no-op for contexts that do
     * not track reader leaks.
     */
    default void setReaderPoolSupervisor(@Nullable ResourcePoolSupervisor<TableReader> supervisor) {
    }

    void setUseSimpleCircuitBreaker(boolean value);

    default boolean shouldLogSql() {
        return true;
    }

    default void storeTelemetry(short event, short origin) {
    }

    default void toSink(@NotNull CharSink<?> sink) {
    }
}
