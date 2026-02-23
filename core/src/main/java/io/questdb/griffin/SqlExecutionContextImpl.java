/*******************************************************************************
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

import io.questdb.Telemetry;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowContextImpl;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntStack;
import io.questdb.std.Misc;
import io.questdb.std.ObjStack;
import io.questdb.std.Rnd;
import io.questdb.std.Transient;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.std.str.CharSink;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

public class SqlExecutionContextImpl implements SqlExecutionContext {
    private static final IntHashSet SKIP_TELEMETRY_EVENTS = new IntHashSet();
    private final CairoConfiguration cairoConfiguration;
    private final CairoEngine cairoEngine;
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256 = new Decimal256();
    private final Decimal64 decimal64 = new Decimal64();
    private final int defaultPageFrameMaxRows;
    private final int defaultPageFrameMinRows;
    private final IntStack hasIntervalStack = new IntStack();
    private final ObjStack<RuntimeIntrinsicIntervalModel> intervalModelObjStack = new ObjStack<>();
    private final MicrosecondClock microClock;
    private final NanosecondClock nanoClock;
    private final int sharedQueryWorkerCount;
    private final AtomicBooleanCircuitBreaker simpleCircuitBreaker;
    private final Telemetry<TelemetryTask> telemetry;
    private final TelemetryFacade telemetryFacade;
    private final IntStack timestampRequiredStack = new IntStack();
    private final WindowContextImpl windowContext = new WindowContextImpl();
    protected BindVariableService bindVariableService;
    protected SecurityContext securityContext;
    private boolean allowNonDeterministicFunction = true;
    private boolean cacheHit;
    private SqlExecutionCircuitBreaker circuitBreaker = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
    private boolean clockUseNow = false;
    private boolean cloneSymbolTables;
    private boolean containsSecret;
    private int intervalFunctionType;
    private int jitMode;
    private long nowMicros;
    private long nowNanos;
    // Timestamp type only for now() function, used by NowFunctionFactory
    private int nowTimestampType;
    private int pageFrameMaxRows;
    private int pageFrameMinRows;
    private boolean parallelFilterEnabled;
    private boolean parallelGroupByEnabled;
    private boolean parallelReadParquetEnabled;
    private boolean parallelTopKEnabled;
    private boolean parallelHorizonJoinEnabled;
    private boolean parallelWindowJoinEnabled;
    private QueryFutureUpdateListener queryFutureUpdateListener = QueryFutureUpdateListener.EMPTY;
    private Rnd random;
    private long requestFd = -1;
    private boolean useSimpleCircuitBreaker;
    private boolean validationOnly = false;

    public SqlExecutionContextImpl(CairoEngine cairoEngine, int sharedQueryWorkerCount) {
        assert sharedQueryWorkerCount >= 0;
        this.sharedQueryWorkerCount = sharedQueryWorkerCount;
        this.cairoEngine = cairoEngine;

        cairoConfiguration = cairoEngine.getConfiguration();
        microClock = cairoConfiguration.getMicrosecondClock();
        nanoClock = cairoConfiguration.getNanosecondClock();
        securityContext = DenyAllSecurityContext.INSTANCE;
        jitMode = cairoConfiguration.getSqlJitMode();
        parallelFilterEnabled = cairoConfiguration.isSqlParallelFilterEnabled() && sharedQueryWorkerCount > 0;
        parallelGroupByEnabled = cairoConfiguration.isSqlParallelGroupByEnabled() && sharedQueryWorkerCount > 0;
        parallelTopKEnabled = cairoConfiguration.isSqlParallelTopKEnabled() && sharedQueryWorkerCount > 0;
        parallelHorizonJoinEnabled = cairoConfiguration.isSqlParallelHorizonJoinEnabled() && sharedQueryWorkerCount > 0;
        parallelWindowJoinEnabled = cairoConfiguration.isSqlParallelWindowJoinEnabled() && sharedQueryWorkerCount > 0;
        parallelReadParquetEnabled = cairoConfiguration.isSqlParallelReadParquetEnabled() && sharedQueryWorkerCount > 0;
        telemetry = cairoEngine.getTelemetry();
        telemetryFacade = telemetry.isEnabled() ? this::doStoreTelemetry : this::storeTelemetryNoOp;
        // default set to micro
        nowTimestampType = ColumnType.TIMESTAMP_MICRO;
        intervalFunctionType = IntervalUtils.getIntervalType(nowTimestampType);
        this.containsSecret = false;
        this.useSimpleCircuitBreaker = false;
        this.simpleCircuitBreaker = new AtomicBooleanCircuitBreaker(cairoEngine, cairoConfiguration.getCircuitBreakerConfiguration().getCircuitBreakerThrottle());
        this.defaultPageFrameMaxRows = cairoConfiguration.getSqlPageFrameMaxRows();
        this.defaultPageFrameMinRows = cairoConfiguration.getSqlPageFrameMinRows();
        this.pageFrameMaxRows = defaultPageFrameMaxRows;
        this.pageFrameMinRows = defaultPageFrameMinRows;
    }

    @Override
    public boolean allowNonDeterministicFunctions() {
        return allowNonDeterministicFunction;
    }

    @Override
    public void changePageFrameSizes(int minRows, int maxRows) {
        this.pageFrameMinRows = minRows;
        this.pageFrameMaxRows = maxRows;
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
    ) {
        windowContext.of(
                partitionByRecord,
                partitionBySink,
                partitionByKeyTypes,
                ordered,
                orderByDirection,
                orderByPos,
                framingMode,
                rowsLo,
                rowsLoUnit,
                rowsLoExprPos,
                rowsHi,
                rowsHiUnit,
                rowsHiExprPos,
                exclusionKind,
                exclusionKindPos,
                timestampIndex,
                timestampType,
                ignoreNulls,
                nullsDescPos
        );
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
        if (useSimpleCircuitBreaker) {
            return simpleCircuitBreaker;
        } else {
            return circuitBreaker;
        }
    }

    @Override
    public boolean getCloneSymbolTables() {
        return cloneSymbolTables;
    }

    public Decimal128 getDecimal128() {
        return decimal128;
    }

    public Decimal256 getDecimal256() {
        return decimal256;
    }

    public Decimal64 getDecimal64() {
        return decimal64;
    }

    @Override
    public int getIntervalFunctionType() {
        return intervalFunctionType;
    }

    @Override
    public int getJitMode() {
        return jitMode;
    }

    @Override
    public long getMicrosecondTimestamp() {
        return clockUseNow ? nowMicros : microClock.getTicks();
    }

    @Override
    public long getNanosecondTimestamp() {
        return clockUseNow ? nowNanos : nanoClock.getTicks();
    }

    @Override
    public long getNow(int timestampType) {
        assert ColumnType.isTimestamp(timestampType);
        return switch (timestampType) {
            case ColumnType.TIMESTAMP_MICRO -> nowMicros;
            case ColumnType.TIMESTAMP_NANO -> nowNanos;
            default -> 0L;
        };
    }

    @Override
    public int getNowTimestampType() {
        return nowTimestampType;
    }

    @Override
    public int getPageFrameMaxRows() {
        return pageFrameMaxRows;
    }

    @Override
    public int getPageFrameMinRows() {
        return pageFrameMinRows;
    }

    @Override
    public QueryFutureUpdateListener getQueryFutureUpdateListener() {
        return queryFutureUpdateListener;
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
    public @NotNull SecurityContext getSecurityContext() {
        return securityContext;
    }

    @Override
    public int getSharedQueryWorkerCount() {
        return sharedQueryWorkerCount;
    }

    @Override
    public SqlExecutionCircuitBreaker getSimpleCircuitBreaker() {
        return simpleCircuitBreaker;
    }

    @Override
    public WindowContext getWindowContext() {
        return windowContext;
    }

    @Override
    public int hasInterval() {
        return hasIntervalStack.peek();
    }

    @Override
    public void initNow() {
        this.nowNanos = nanoClock.getTicks();
        this.nowMicros = microClock.getTicks();
    }

    public boolean isCacheHit() {
        return cacheHit;
    }

    @Override
    public boolean isParallelFilterEnabled() {
        return parallelFilterEnabled;
    }

    @Override
    public boolean isParallelGroupByEnabled() {
        return parallelGroupByEnabled;
    }

    @Override
    public boolean isParallelReadParquetEnabled() {
        return parallelReadParquetEnabled;
    }

    @Override
    public boolean isParallelTopKEnabled() {
        return parallelTopKEnabled;
    }

    @Override
    public boolean isParallelHorizonJoinEnabled() {
        return parallelHorizonJoinEnabled;
    }

    @Override
    public boolean isParallelWindowJoinEnabled() {
        return parallelWindowJoinEnabled;
    }

    @Override
    public boolean isTimestampRequired() {
        return timestampRequiredStack.notEmpty() && timestampRequiredStack.peek() == 1;
    }

    @Override
    public boolean isValidationOnly() {
        return validationOnly;
    }

    @Override
    public boolean isWalApplication() {
        return false;
    }

    @Override
    public RuntimeIntrinsicIntervalModel peekIntervalModel() {
        return intervalModelObjStack.peek();
    }

    @Override
    public void popHasInterval() {
        hasIntervalStack.pop();
    }

    @Override
    public void popIntervalModel() {
        intervalModelObjStack.pop();
    }

    @Override
    public void popTimestampRequiredFlag() {
        timestampRequiredStack.pop();
    }

    @Override
    public void pushHasInterval(int hasInterval) {
        hasIntervalStack.push(hasInterval);
    }

    @Override
    public void pushIntervalModel(RuntimeIntrinsicIntervalModel intervalModel) {
        intervalModelObjStack.push(intervalModel);
    }

    @Override
    public void pushTimestampRequiredFlag(boolean flag) {
        timestampRequiredStack.push(flag ? 1 : 0);
    }

    @Override
    public void reset() {
        this.containsSecret = false;
        this.useSimpleCircuitBreaker = false;
        this.cacheHit = false;
        this.allowNonDeterministicFunction = true;
        this.validationOnly = false;
        this.timestampRequiredStack.clear();
        this.hasIntervalStack.clear();
        this.intervalModelObjStack.clear();
        Misc.clear(securityContext);
    }

    @Override
    public void restoreToDefaultPageFrameSizes() {
        this.pageFrameMinRows = defaultPageFrameMinRows;
        this.pageFrameMaxRows = defaultPageFrameMaxRows;
    }

    @Override
    public void setAllowNonDeterministicFunction(boolean value) {
        this.allowNonDeterministicFunction = value;
    }

    @Override
    public void setCacheHit(boolean value) {
        cacheHit = value;
    }

    @Override
    public void setCancelledFlag(AtomicBoolean cancelled) {
        circuitBreaker.setCancelledFlag(cancelled);
        simpleCircuitBreaker.setCancelledFlag(cancelled);
    }

    @Override
    public void setCloneSymbolTables(boolean cloneSymbolTables) {
        this.cloneSymbolTables = cloneSymbolTables;
    }

    @Override
    public void setIntervalFunctionType(int intervalType) {
        this.intervalFunctionType = intervalType;
    }

    @Override
    public void setJitMode(int jitMode) {
        this.jitMode = jitMode;
    }

    @Override
    public void setNowAndFixClock(long now, int nowTimestampType) {
        TimestampDriver driver = ColumnType.getTimestampDriver(nowTimestampType);
        this.nowMicros = driver.toMicros(now);
        this.nowNanos = driver.toNanos(now);
        this.nowTimestampType = nowTimestampType;
        this.clockUseNow = true;
    }

    public void setQueryFutureUpdateListener(QueryFutureUpdateListener listener) {
        this.queryFutureUpdateListener = listener != null ? listener : QueryFutureUpdateListener.EMPTY;
    }

    @Override
    public void setParallelFilterEnabled(boolean parallelFilterEnabled) {
        this.parallelFilterEnabled = parallelFilterEnabled;
    }

    @Override
    public void setParallelGroupByEnabled(boolean parallelGroupByEnabled) {
        this.parallelGroupByEnabled = parallelGroupByEnabled;
    }

    @Override
    public void setParallelReadParquetEnabled(boolean parallelReadParquetEnabled) {
        this.parallelReadParquetEnabled = parallelReadParquetEnabled;
    }

    @Override
    public void setParallelTopKEnabled(boolean parallelTopKEnabled) {
        this.parallelTopKEnabled = parallelTopKEnabled;
    }

    @Override
    public void setParallelHorizonJoinEnabled(boolean parallelHorizonJoinEnabled) {
        this.parallelHorizonJoinEnabled = parallelHorizonJoinEnabled;
    }

    @Override
    public void setParallelWindowJoinEnabled(boolean parallelWindowJoinEnabled) {
        this.parallelWindowJoinEnabled = parallelWindowJoinEnabled;
    }

    @Override
    public void setRandom(Rnd rnd) {
        this.random = rnd;
    }

    @Override
    public void setUseSimpleCircuitBreaker(boolean value) {
        this.useSimpleCircuitBreaker = value;
    }

    public void setValidationOnly(boolean validationOnly) {
        this.validationOnly = validationOnly;
    }

    @Override
    public void storeTelemetry(short event, short origin) {
        telemetryFacade.store(event, origin);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("principal=").put(securityContext.getPrincipal()).putAscii(", cache=").put(isCacheHit());
    }

    public SqlExecutionContextImpl with(@NotNull SecurityContext securityContext, @Nullable BindVariableService bindVariableService, @Nullable Rnd rnd) {
        this.securityContext = securityContext;
        this.bindVariableService = bindVariableService;
        this.random = rnd;
        reset();
        return this;
    }

    public void with(long requestFd) {
        this.requestFd = requestFd;
        reset();
    }

    public void with(BindVariableService bindVariableService) {
        this.bindVariableService = bindVariableService;
    }

    public SqlExecutionContext with(SqlExecutionCircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
        return this;
    }

    public SqlExecutionContextImpl with(@NotNull SecurityContext securityContext) {
        return with(securityContext, null, null, -1, null);
    }

    public SqlExecutionContextImpl with(@NotNull SecurityContext securityContext, @Nullable BindVariableService bindVariableService) {
        return with(securityContext, bindVariableService, null, -1, null);
    }

    public SqlExecutionContextImpl with(
            @NotNull SecurityContext securityContext,
            @Nullable BindVariableService bindVariableService,
            @Nullable Rnd rnd,
            long requestFd,
            @Nullable SqlExecutionCircuitBreaker circuitBreaker
    ) {
        this.securityContext = securityContext;
        this.bindVariableService = bindVariableService;
        this.random = rnd;
        this.requestFd = requestFd;
        this.circuitBreaker = circuitBreaker == null ? SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER : circuitBreaker;
        this.pageFrameMaxRows = defaultPageFrameMaxRows;
        this.pageFrameMinRows = defaultPageFrameMinRows;
        reset();
        return this;
    }

    private void doStoreTelemetry(short event, short origin) {
        if (SKIP_TELEMETRY_EVENTS.contains(event)) {
            return;
        }
        TelemetryTask.store(telemetry, origin, event);
    }

    private void storeTelemetryNoOp(short event, short origin) {
    }

    @FunctionalInterface
    private interface TelemetryFacade {
        void store(short event, short origin);
    }

    static {
        SKIP_TELEMETRY_EVENTS.add(CompiledQuery.SET);
        SKIP_TELEMETRY_EVENTS.add(CompiledQuery.BEGIN);
        SKIP_TELEMETRY_EVENTS.add(CompiledQuery.COMMIT);
        SKIP_TELEMETRY_EVENTS.add(CompiledQuery.ROLLBACK);
        SKIP_TELEMETRY_EVENTS.add(CompiledQuery.DEALLOCATE);
    }
}
