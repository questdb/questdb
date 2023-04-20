/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.analytic.AnalyticContext;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public interface SqlExecutionContext extends Closeable {

    void clearAnalyticContext();

    @Override
    default void close() {
    }

    void configureAnalyticContext(
            @Nullable VirtualRecord partitionByRecord,
            @Nullable RecordSink partitionBySink,
            @Transient @Nullable ColumnTypes keyTypes,
            boolean isOrdered,
            boolean baseSupportsRandomAccess
    );

    AnalyticContext getAnalyticContext();

    default Rnd getAsyncRandom() {
        return SharedRandom.getAsyncRandom(getCairoEngine().getConfiguration());
    }

    BindVariableService getBindVariableService();

    @NotNull
    CairoEngine getCairoEngine();

    SecurityContext getSecurityContext();

    @NotNull
    SqlExecutionCircuitBreaker getCircuitBreaker();

    boolean getCloneSymbolTables();

    int getJitMode();

    default @NotNull MessageBus getMessageBus() {
        return getCairoEngine().getMessageBus();
    }

    default TableRecordMetadata getMetadata(TableToken tableToken) {
        return getCairoEngine().getMetadata(tableToken);
    }

    default TableRecordMetadata getMetadata(TableToken tableToken, long structureVersion) {
        return getCairoEngine().getMetadata(tableToken, structureVersion);
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

    default int getSharedWorkerCount() {
        return getWorkerCount();
    }

    default int getTableStatus(Path path, TableToken tableName) {
        return getCairoEngine().getTableStatus(path, tableName);
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

    int getWorkerCount();

    void initNow();

    boolean isColumnPreTouchEnabled();

    boolean isParallelFilterEnabled();

    boolean isTimestampRequired();

    boolean isWalApplication();

    void popTimestampRequiredFlag();

    void pushTimestampRequiredFlag(boolean flag);

    void setCloneSymbolTables(boolean cloneSymbolTables);

    void setColumnPreTouchEnabled(boolean columnPreTouchEnabled);

    void setJitMode(int jitMode);

    void setNowAndFixClock(long now);

    void setParallelFilterEnabled(boolean parallelFilterEnabled);

    void setRandom(Rnd rnd);

    default void storeTelemetry(short event, short origin) {
    }
}
