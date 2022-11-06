/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.analytic.AnalyticContext;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public interface SqlExecutionContext extends Closeable {

    QueryFutureUpdateListener getQueryFutureUpdateListener();

    BindVariableService getBindVariableService();

    CairoSecurityContext getCairoSecurityContext();

    default @NotNull MessageBus getMessageBus() {
        return getCairoEngine().getMessageBus();
    }

    boolean isTimestampRequired();

    void popTimestampRequiredFlag();

    void pushTimestampRequiredFlag(boolean flag);

    int getWorkerCount();

    default int getSharedWorkerCount() {
        return getWorkerCount();
    }

    Rnd getRandom();

    default Rnd getAsyncRandom() {
        return SharedRandom.getAsyncRandom(getCairoEngine().getConfiguration());
    }

    void setRandom(Rnd rnd);

    @NotNull CairoEngine getCairoEngine();

    long getRequestFd();

    @NotNull SqlExecutionCircuitBreaker getCircuitBreaker();

    void storeTelemetry(short event, short origin);

    AnalyticContext getAnalyticContext();

    void configureAnalyticContext(
            @Nullable VirtualRecord partitionByRecord,
            @Nullable RecordSink partitionBySink,
            @Transient @Nullable ColumnTypes keyTypes,
            boolean isOrdered,
            boolean baseSupportsRandomAccess
    );

    void clearAnalyticContext();

    void initNow();

    long getNow();

    int getJitMode();

    void setJitMode(int jitMode);

    @Override
    default void close(){
    }

    void setCloneSymbolTables(boolean cloneSymbolTables);

    boolean getCloneSymbolTables();
}
