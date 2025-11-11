/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.model.IntrinsicModel;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
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
            long rowsHi,
            char rowsHiUnit,
            int rowsHiExprPos,
            int exclusionKind,
            int exclusionKindPos,
            int timestampIndex,
            int timestampType,
            boolean ignoreNulls,
            int nullsDescPos
    );

    default void containsSecret(boolean b) {
    }

    default boolean containsSecret() {
        return false;
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

    int getIntervalFunctionType();

    int getJitMode();

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

    QueryFutureUpdateListener getQueryFutureUpdateListener();

    Rnd getRandom();

    default TableReader getReader(TableToken tableName, long version) {
        return getCairoEngine().getReader(tableName, version);
    }

    default TableReader getReader(TableToken tableName) {
        return getCairoEngine().getReader(tableName);
    }

    long getRequestFd();

    @NotNull
    SecurityContext getSecurityContext();

    int getSharedQueryWorkerCount();

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

    WindowContext getWindowContext();

    void initNow();

    boolean isCacheHit();

    // Returns true when where intrinsics are overridden, i.e. by a materialized view refresh
    default boolean isOverriddenIntrinsics(TableToken tableToken) {
        return false;
    }

    boolean isParallelFilterEnabled();

    boolean isParallelGroupByEnabled();

    boolean isParallelReadParquetEnabled();

    boolean isParallelTopKEnabled();

    boolean isTimestampRequired();

    default boolean isUninterruptible() {
        return false;
    }

    boolean isWalApplication();

    // This method is used to override intrinsic values in the query execution context
    // Its initial usage is in the materialized view refresh
    // where the queried timestamp of the base table is limited to the range affected since last refresh
    default void overrideWhereIntrinsics(TableToken tableToken, IntrinsicModel intrinsicModel, int timestampType) {
    }

    void popTimestampRequiredFlag();

    void pushTimestampRequiredFlag(boolean flag);

    void resetFlags();

    void setAllowNonDeterministicFunction(boolean value);

    void setCacheHit(boolean value);

    void setCancelledFlag(AtomicBoolean cancelled);

    void setCloneSymbolTables(boolean cloneSymbolTables);

    void setIntervalFunctionType(int intervalType);

    void setJitMode(int jitMode);

    void setNowAndFixClock(long now, int nowTimestampType);

    void setParallelFilterEnabled(boolean parallelFilterEnabled);

    void setParallelGroupByEnabled(boolean parallelGroupByEnabled);

    void setParallelReadParquetEnabled(boolean parallelReadParquetEnabled);

    void setParallelTopKEnabled(boolean parallelTopKEnabled);

    void setRandom(Rnd rnd);

    void setUseSimpleCircuitBreaker(boolean value);

    default boolean shouldLogSql() {
        return true;
    }

    default void storeTelemetry(short event, short origin) {
    }

    default void toSink(@NotNull CharSink<?> sink) {
    }
}
