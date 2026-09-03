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

package io.questdb.test.tools;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A configuration that throws a {@link FaultInjectedException} out of one getter. Tests use it to fail
 * a constructor at a chosen point of a factory's construction: each {@link FaultMethod} names a getter
 * that exactly one construction step reads, so the throw lands inside that step and nowhere else.
 * <p>
 * {@code faultPoint} is what the exception carries back. A test that arms one of several fault points
 * passes its own enum constant and asserts the fault it asked for is the fault it got; a test with a
 * single fault point passes null and gets the cheaper no-argument exception.
 * <p>
 * An instance that backs a live {@link io.questdb.cairo.CairoEngine} must start disarmed: engine
 * construction and the DDL that follows read configuration too, and only the statement under test may
 * fault. Build such an instance with {@code isArmed} false and arm it around that statement.
 */
public class FaultInjectingConfiguration extends CairoConfigurationWrapper {
    private final FaultMethod faultMethod;
    private final Enum<?> faultPoint;
    private volatile boolean isArmed;

    public FaultInjectingConfiguration(
            @NotNull CairoConfiguration delegate,
            @NotNull FaultMethod faultMethod,
            @Nullable Enum<?> faultPoint
    ) {
        this(delegate, faultMethod, faultPoint, true);
    }

    public FaultInjectingConfiguration(
            @NotNull CairoConfiguration delegate,
            @NotNull FaultMethod faultMethod,
            @Nullable Enum<?> faultPoint,
            boolean isArmed
    ) {
        super(delegate);
        this.faultMethod = faultMethod;
        this.faultPoint = faultPoint;
        this.isArmed = isArmed;
    }

    @Override
    public @NotNull SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        throwIfArmed(FaultMethod.CIRCUIT_BREAKER_CONFIGURATION);
        return super.getCircuitBreakerConfiguration();
    }

    @Override
    public int getSqlAsOfJoinLookAhead() {
        throwIfArmed(FaultMethod.SQL_AS_OF_JOIN_LOOK_AHEAD);
        return super.getSqlAsOfJoinLookAhead();
    }

    @Override
    public int getSqlJitBindVarsMemoryPageSize() {
        throwIfArmed(FaultMethod.SQL_JIT_BIND_VARS_MEMORY_PAGE_SIZE);
        return super.getSqlJitBindVarsMemoryPageSize();
    }

    @Override
    public double getSqlParallelFilterPreTouchThreshold() {
        throwIfArmed(FaultMethod.SQL_PARALLEL_FILTER_PRE_TOUCH_THRESHOLD);
        return super.getSqlParallelFilterPreTouchThreshold();
    }

    @Override
    public int getSqlSmallPageFrameMinRows() {
        throwIfArmed(FaultMethod.SQL_SMALL_PAGE_FRAME_MIN_ROWS);
        return super.getSqlSmallPageFrameMinRows();
    }

    public void setArmed(boolean isArmed) {
        this.isArmed = isArmed;
    }

    private void throwIfArmed(FaultMethod candidate) {
        if (isArmed && faultMethod == candidate) {
            throw faultPoint != null ? new FaultInjectedException(faultPoint) : new FaultInjectedException();
        }
    }

    /**
     * The getter that faults. Each constant names the getter rather than the construction step, because
     * which step reads it differs per test - the fault point enum of each test class documents that.
     */
    public enum FaultMethod {
        /**
         * No getter faults. A fault point whose throw comes from somewhere else entirely - a stubbed
         * collaborator, say - still needs a {@code FaultMethod} to hand this class, and handing it
         * {@code null} would break the {@link NotNull} contract on the constructor parameter. That is
         * inert under {@code mvn test}, but IDEA's "add runtime assertions for notnull-annotated
         * methods and parameters" is on by default and turns it into a synthetic
         * {@code IllegalArgumentException} pointing nowhere near the cause. {@code throwIfArmed} is
         * never called with this constant, so it can never match and this configuration never throws.
         */
        NONE,
        CIRCUIT_BREAKER_CONFIGURATION,
        SQL_AS_OF_JOIN_LOOK_AHEAD,
        SQL_JIT_BIND_VARS_MEMORY_PAGE_SIZE,
        SQL_PARALLEL_FILTER_PRE_TOUCH_THRESHOLD,
        SQL_SMALL_PAGE_FRAME_MIN_ROWS
    }
}
