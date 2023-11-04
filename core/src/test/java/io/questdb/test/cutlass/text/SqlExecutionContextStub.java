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

package io.questdb.test.cutlass.text;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.QueryFutureUpdateListener;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.std.Rnd;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SqlExecutionContextStub implements SqlExecutionContext {

    private final CairoEngine engine;

    public SqlExecutionContextStub(@NotNull CairoEngine engine) {
        this.engine = engine;
    }

    @Override
    public void clearWindowContext() {
    }

    @Override
    public void configureWindowContext(@Nullable VirtualRecord partitionByRecord, @Nullable RecordSink partitionBySink, @Nullable ColumnTypes keyTypes, boolean isOrdered, int orderByDirection, int orderByPos, boolean baseSupportsRandomAccess, int framingMode, long rowsLo, int rowsLoExprPos, long rowsHi, int rowsHiExprPos, int exclusionKind, int exclusionKindPos, int timestampIndex) {

    }

    @Override
    public BindVariableService getBindVariableService() {
        return null;
    }

    @Override
    public @NotNull CairoEngine getCairoEngine() {
        return engine;
    }

    @Override
    public @NotNull SqlExecutionCircuitBreaker getCircuitBreaker() {
        return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
    }

    @Override
    public boolean getCloneSymbolTables() {
        return false;
    }

    @Override
    public int getJitMode() {
        return 0;
    }

    @Override
    public long getMicrosecondTimestamp() {
        return 0L;
    }

    @Override
    public long getNow() {
        return 0L;
    }

    @Override
    public QueryFutureUpdateListener getQueryFutureUpdateListener() {
        return null;
    }

    @Override
    public Rnd getRandom() {
        return null;
    }

    @Override
    public int getRequestFd() {
        return 0;
    }

    @Override
    public @NotNull SecurityContext getSecurityContext() {
        return engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext();
    }

    @Override
    public WindowContext getWindowContext() {
        return null;
    }

    @Override
    public int getWorkerCount() {
        return 0;
    }

    @Override
    public void initNow() {
    }

    @Override
    public boolean isColumnPreTouchEnabled() {
        return false;
    }

    @Override
    public boolean isParallelFilterEnabled() {
        return false;
    }

    @Override
    public boolean isTimestampRequired() {
        return false;
    }

    @Override
    public boolean isWalApplication() {
        return false;
    }

    @Override
    public void popTimestampRequiredFlag() {
    }

    @Override
    public void pushTimestampRequiredFlag(boolean flag) {
    }

    @Override
    public void setCloneSymbolTables(boolean cloneSymbolTables) {
    }

    @Override
    public void setColumnPreTouchEnabled(boolean columnPreTouchEnabled) {
    }

    @Override
    public void setJitMode(int jitMode) {
    }

    @Override
    public void setNowAndFixClock(long now) {
    }

    @Override
    public void setParallelFilterEnabled(boolean parallelFilterEnabled) {
    }

    @Override
    public void setRandom(Rnd rnd) {
    }
}
