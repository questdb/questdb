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

import io.questdb.Telemetry;
import io.questdb.cairo.*;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowContextImpl;
import io.questdb.std.IntStack;
import io.questdb.std.Rnd;
import io.questdb.std.Transient;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SqlExecutionContextImpl implements SqlExecutionContext {
    private final CairoConfiguration cairoConfiguration;
    private final CairoEngine cairoEngine;
    private final int sharedWorkerCount;
    private final Telemetry<TelemetryTask> telemetry;
    private final TelemetryFacade telemetryFacade;
    private final IntStack timestampRequiredStack = new IntStack();
    private final WindowContextImpl windowContext = new WindowContextImpl();
    private final int workerCount;
    private BindVariableService bindVariableService;
    private SqlExecutionCircuitBreaker circuitBreaker = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
    private MicrosecondClock clock;
    private boolean cloneSymbolTables = false;
    private boolean columnPreTouchEnabled = true;
    private boolean containsSecret;
    private int jitMode;
    private long now;
    private final MicrosecondClock nowClock = () -> now;
    private boolean parallelFilterEnabled;
    private Rnd random;
    private int requestFd = -1;
    private SecurityContext securityContext;

    public SqlExecutionContextImpl(CairoEngine cairoEngine, int workerCount, int sharedWorkerCount) {
        assert workerCount > 0;
        this.workerCount = workerCount;
        assert sharedWorkerCount > 0;
        this.sharedWorkerCount = sharedWorkerCount;
        this.cairoEngine = cairoEngine;

        cairoConfiguration = cairoEngine.getConfiguration();
        clock = cairoConfiguration.getMicrosecondClock();
        securityContext = DenyAllSecurityContext.INSTANCE;
        jitMode = cairoConfiguration.getSqlJitMode();
        parallelFilterEnabled = cairoConfiguration.isSqlParallelFilterEnabled();
        telemetry = cairoEngine.getTelemetry();
        telemetryFacade = telemetry.isEnabled() ? this::doStoreTelemetry : this::storeTelemetryNoop;
        this.containsSecret = false;
    }

    public SqlExecutionContextImpl(CairoEngine cairoEngine, int workerCount) {
        this(cairoEngine, workerCount, workerCount);
    }

    @Override
    public void clearWindowContext() {
        windowContext.clear();
    }

    @Override
    public void configureWindowContext(
            @Nullable VirtualRecord partitionByRecord,
            @Nullable RecordSink partitionBySink,
            @Transient @Nullable ColumnTypes partitionByKeyTypes,
            boolean ordered,
            int orderByDirection,
            int orderByPos,
            boolean baseSupportsRandomAccess,
            int framingMode,
            long rowsLo,
            int rowsLoKindPos,
            long rowsHi,
            int rowsHiKindPos,
            int exclusionKind,
            int exclusionKindPos,
            int timestampIndex
    ) {
        windowContext.of(
                partitionByRecord,
                partitionBySink,
                partitionByKeyTypes,
                ordered,
                orderByDirection,
                orderByPos,
                baseSupportsRandomAccess,
                framingMode,
                rowsLo,
                rowsLoKindPos,
                rowsHi,
                rowsHiKindPos,
                exclusionKind,
                exclusionKindPos,
                timestampIndex);
    }

    @Override
    public boolean containsSecret() {
        return containsSecret;
    }

    @Override
    public void containsSecret(boolean containsSecret) {
        this.containsSecret = containsSecret;
    }

    @Override
    public BindVariableService getBindVariableService() {
        return bindVariableService;
    }

    @Override
    public @NotNull CairoEngine getCairoEngine() {
        return cairoEngine;
    }

    @Override
    public @NotNull SqlExecutionCircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    @Override
    public boolean getCloneSymbolTables() {
        return cloneSymbolTables;
    }

    @Override
    public int getJitMode() {
        return jitMode;
    }

    @Override
    public long getMicrosecondTimestamp() {
        return clock.getTicks();
    }

    @Override
    public long getNow() {
        return now;
    }

    @Override
    public QueryFutureUpdateListener getQueryFutureUpdateListener() {
        return QueryFutureUpdateListener.EMPTY;
    }

    @Override
    public Rnd getRandom() {
        return random != null ? random : SharedRandom.getRandom(cairoConfiguration);
    }

    @Override
    public int getRequestFd() {
        return requestFd;
    }

    @Override
    public @NotNull SecurityContext getSecurityContext() {
        return securityContext;
    }

    @Override
    public int getSharedWorkerCount() {
        return sharedWorkerCount;
    }

    @Override
    public WindowContext getWindowContext() {
        return windowContext;
    }

    @Override
    public int getWorkerCount() {
        return workerCount;
    }

    @Override
    public void initNow() {
        now = clock.getTicks();
    }

    @Override
    public boolean isColumnPreTouchEnabled() {
        return columnPreTouchEnabled;
    }

    @Override
    public boolean isParallelFilterEnabled() {
        return parallelFilterEnabled;
    }

    @Override
    public boolean isTimestampRequired() {
        return timestampRequiredStack.notEmpty() && timestampRequiredStack.peek() == 1;
    }

    @Override
    public boolean isWalApplication() {
        return false;
    }

    @Override
    public void popTimestampRequiredFlag() {
        timestampRequiredStack.pop();
    }

    @Override
    public void pushTimestampRequiredFlag(boolean flag) {
        timestampRequiredStack.push(flag ? 1 : 0);
    }

    @Override
    public void setCloneSymbolTables(boolean cloneSymbolTables) {
        this.cloneSymbolTables = cloneSymbolTables;
    }

    @Override
    public void setColumnPreTouchEnabled(boolean columnPreTouchEnabled) {
        this.columnPreTouchEnabled = columnPreTouchEnabled;
    }

    @Override
    public void setJitMode(int jitMode) {
        this.jitMode = jitMode;
    }

    @Override
    public void setNowAndFixClock(long now) {
        this.now = now;
        clock = nowClock;
    }

    @Override
    public void setParallelFilterEnabled(boolean parallelFilterEnabled) {
        this.parallelFilterEnabled = parallelFilterEnabled;
    }

    @Override
    public void setRandom(Rnd rnd) {
        this.random = rnd;
    }

    @Override
    public void storeTelemetry(short event, short origin) {
        telemetryFacade.store(event, origin);
    }

    public SqlExecutionContextImpl with(@NotNull SecurityContext securityContext, @Nullable BindVariableService bindVariableService, @Nullable Rnd rnd) {
        this.securityContext = securityContext;
        this.bindVariableService = bindVariableService;
        this.random = rnd;
        this.containsSecret = false;
        return this;
    }

    public void with(int requestFd) {
        this.requestFd = requestFd;
    }

    public void with(BindVariableService bindVariableService) {
        this.bindVariableService = bindVariableService;
    }

    public void with(SqlExecutionCircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    public SqlExecutionContextImpl with(@NotNull SecurityContext securityContext, @Nullable BindVariableService bindVariableService) {
        return with(securityContext, bindVariableService, null, -1, null);
    }

    public SqlExecutionContextImpl with(
            @NotNull SecurityContext securityContext,
            @Nullable BindVariableService bindVariableService,
            @Nullable Rnd rnd,
            int requestFd,
            @Nullable SqlExecutionCircuitBreaker circuitBreaker
    ) {
        this.securityContext = securityContext;
        this.bindVariableService = bindVariableService;
        this.random = rnd;
        this.requestFd = requestFd;
        this.circuitBreaker = circuitBreaker == null ? SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER : circuitBreaker;
        this.containsSecret = false;
        return this;
    }

    private void doStoreTelemetry(short event, short origin) {
        TelemetryTask.store(telemetry, origin, event);
    }

    private void storeTelemetryNoop(short event, short origin) {
    }

    @FunctionalInterface
    private interface TelemetryFacade {
        void store(short event, short origin);
    }
}
