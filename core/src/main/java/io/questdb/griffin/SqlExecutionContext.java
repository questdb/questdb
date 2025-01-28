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
import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.std.Rnd;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public interface SqlExecutionContext extends Closeable {

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
            int rowsLoExprPos,
            long rowsHi,
            int rowsHiExprPos,
            int exclusionKind,
            int exclusionKindPos,
            int timestampIndex,
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

    long getNow();

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

    default int getSharedWorkerCount() {
        return getWorkerCount();
    }

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

    int getWorkerCount();

    void initNow();

    boolean isCacheHit();

    boolean isColumnPreTouchEnabled();

    boolean isParallelFilterEnabled();

    boolean isTimestampRequired();

    default boolean isUninterruptible() {
        return false;
    }

    boolean isWalApplication();

    void popTimestampRequiredFlag();

    void pushTimestampRequiredFlag(boolean flag);

    void setCacheHit(boolean value);

    void setCancelledFlag(AtomicBoolean cancelled);

    void setCloneSymbolTables(boolean cloneSymbolTables);

    void setColumnPreTouchEnabled(boolean columnPreTouchEnabled);

    void setJitMode(int jitMode);

    void setNowAndFixClock(long now);

    void setParallelFilterEnabled(boolean parallelFilterEnabled);

    void setRandom(Rnd rnd);

    void setUseSimpleCircuitBreaker(boolean value);

    default void storeTelemetry(short event, short origin) {
    }
}
