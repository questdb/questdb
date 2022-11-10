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

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.analytic.AnalyticContext;
import io.questdb.griffin.engine.analytic.AnalyticContextImpl;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.IntStack;
import io.questdb.std.Rnd;
import io.questdb.std.Transient;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SqlExecutionContextImpl implements SqlExecutionContext {
    private final AnalyticContextImpl analyticContext = new AnalyticContextImpl();
    private final CairoConfiguration cairoConfiguration;
    private final CairoEngine cairoEngine;
    private final MicrosecondClock clock;
    private final int sharedWorkerCount;
    private final RingQueue<TelemetryTask> telemetryQueue;
    private final IntStack timestampRequiredStack = new IntStack();
    private final int workerCount;
    private BindVariableService bindVariableService;
    private CairoSecurityContext cairoSecurityContext;
    private SqlExecutionCircuitBreaker circuitBreaker = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
    private boolean cloneSymbolTables = false;
    private boolean columnPreTouchEnabled = true;
    private int jitMode;
    private long now;
    private Rnd random;
    private long requestFd = -1;
    private TelemetryTask.TelemetryMethod telemetryMethod = this::storeTelemetryNoop;
    private Sequence telemetryPubSeq;
    private boolean walApplication;

    public SqlExecutionContextImpl(CairoEngine cairoEngine, int workerCount, int sharedWorkerCount) {
        this.cairoConfiguration = cairoEngine.getConfiguration();
        assert workerCount > 0;
        this.workerCount = workerCount;
        assert sharedWorkerCount > 0;
        this.sharedWorkerCount = sharedWorkerCount;
        this.cairoEngine = cairoEngine;
        this.clock = cairoConfiguration.getMicrosecondClock();
        this.cairoSecurityContext = AllowAllCairoSecurityContext.INSTANCE;
        this.jitMode = cairoConfiguration.getSqlJitMode();

        this.telemetryQueue = cairoEngine.getTelemetryQueue();
        if (telemetryQueue != null) {
            this.telemetryPubSeq = cairoEngine.getTelemetryPubSequence();
            this.telemetryMethod = this::doStoreTelemetry;
        }
        walApplication = false;
    }

    public SqlExecutionContextImpl(CairoEngine cairoEngine, int workerCount) {
        this(cairoEngine, workerCount, workerCount);
    }

    @Override
    public void clearAnalyticContext() {
        analyticContext.clear();
    }

    @Override
    public void configureAnalyticContext(
            @Nullable VirtualRecord partitionByRecord,
            @Nullable RecordSink partitionBySink,
            @Transient @Nullable ColumnTypes partitionByKeyTypes,
            boolean ordered,
            boolean baseSupportsRandomAccess
    ) {
        analyticContext.of(
                partitionByRecord,
                partitionBySink,
                partitionByKeyTypes,
                ordered,
                baseSupportsRandomAccess
        );
    }

    @Override
    public AnalyticContext getAnalyticContext() {
        return analyticContext;
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
    public CairoSecurityContext getCairoSecurityContext() {
        return cairoSecurityContext;
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
    public long getRequestFd() {
        return requestFd;
    }

    @Override
    public int getSharedWorkerCount() {
        return sharedWorkerCount;
    }

    @Override
    public int getWorkerCount() {
        return workerCount;
    }

    @Override
    public void initNow() {
        now = cairoConfiguration.getMicrosecondClock().getTicks();
    }

    @Override
    public boolean isColumnPreTouchEnabled() {
        return columnPreTouchEnabled;
    }

    @Override
    public boolean isTimestampRequired() {
        return timestampRequiredStack.notEmpty() && timestampRequiredStack.peek() == 1;
    }

    @Override
    public boolean isWalApplication() {
        return walApplication;
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
    public void setRandom(Rnd rnd) {
        this.random = rnd;
    }

    @Override
    public void storeTelemetry(short event, short origin) {
        telemetryMethod.store(event, origin);
    }

    public SqlExecutionContextImpl with(
            @NotNull CairoSecurityContext cairoSecurityContext,
            @Nullable BindVariableService bindVariableService,
            @Nullable Rnd rnd
    ) {
        this.cairoSecurityContext = cairoSecurityContext;
        this.bindVariableService = bindVariableService;
        this.random = rnd;
        walApplication = false;
        return this;
    }

    public SqlExecutionContextImpl with(
            long requestFd
    ) {
        this.requestFd = requestFd;
        return this;
    }

    public SqlExecutionContextImpl with(
            @NotNull CairoSecurityContext cairoSecurityContext,
            @Nullable BindVariableService bindVariableService,
            @Nullable Rnd rnd,
            long requestFd,
            @Nullable SqlExecutionCircuitBreaker circuitBreaker
    ) {
        this.cairoSecurityContext = cairoSecurityContext;
        this.bindVariableService = bindVariableService;
        this.random = rnd;
        this.requestFd = requestFd;
        this.circuitBreaker = null == circuitBreaker ? SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER : circuitBreaker;
        walApplication = false;
        return this;
    }

    public SqlExecutionContext withWalApplication() {
        walApplication = true;
        return this;
    }

    private void doStoreTelemetry(short event, short origin) {
        TelemetryTask.store(telemetryQueue, telemetryPubSeq, event, origin, clock);
    }

    private void storeTelemetryNoop(short event, short origin) {
    }
}
