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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.CompiledTickExpression;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;

/**
 * The interval bounds of a tick expression with a date variable, such as {@code '$today'}, that a
 * JIT filter reads from its bind variable slots. The JIT filter compiles a fixed number of range
 * checks for the expression, {@code maxIntervalCount}, which
 * {@link CompiledTickExpression#getMaxIntervalCount(int)} proves to be an upper bound for any
 * "now". {@link #init} evaluates the expression against the "now" of the execution, as the Java
 * filter does, and pads the interval list up to that count with an interval that matches no row.
 * <p>
 * Each bound has its own slot, backed by a {@link CompiledFilterTickBoundBindVariable}, and all
 * slots of an expression share one instance, which evaluates the expression once per "now".
 */
public class CompiledFilterTickBounds {
    // The padding interval. A non-NULL key fails key <= Long.MIN_VALUE, which is NULL to the JIT,
    // and a NULL key fails NULL >= Long.MAX_VALUE.
    private static final long EMPTY_INTERVAL_HI = Long.MIN_VALUE;
    private static final long EMPTY_INTERVAL_LO = Long.MAX_VALUE;
    private final LongList bounds = new LongList();
    private final CompiledTickExpression compiledExpr;
    private final boolean isDate;
    private final int maxIntervalCount;
    private final int timestampType;
    private long evaluatedNow;
    private boolean isEvaluated;

    /**
     * @param compiledExpr     the expression, compiled with the driver of {@code timestampType}
     * @param timestampType    the timestamp type whose "now" the expression evaluates against
     * @param maxIntervalCount the number of range checks the JIT filter compiles, at least 1
     * @param isDate           true for a DATE key, which the JIT compares in milliseconds
     */
    public CompiledFilterTickBounds(
            CompiledTickExpression compiledExpr,
            int timestampType,
            int maxIntervalCount,
            boolean isDate
    ) {
        assert maxIntervalCount > 0;
        this.compiledExpr = compiledExpr;
        this.timestampType = timestampType;
        this.maxIntervalCount = maxIntervalCount;
        this.isDate = isDate;
    }

    /**
     * Returns bound {@code index}: the lo of interval {@code index / 2} for an even index, and
     * its hi for an odd one. Valid after {@link #init}.
     */
    public long getBound(int index) {
        return bounds.getQuick(index);
    }

    public int getMaxIntervalCount() {
        return maxIntervalCount;
    }

    public void init(SqlExecutionContext executionContext) {
        final long now = executionContext.getNow(timestampType);
        if (isEvaluated && now == evaluatedNow) {
            // Another slot of this expression evaluated it for this execution already. The result
            // depends on "now" only. No "now" value can serve as the "not evaluated" marker, since
            // tests run at 0, hence the flag.
            return;
        }
        isEvaluated = false;
        // evaluate() appends.
        bounds.clear();
        compiledExpr.evaluate(bounds, now);
        final int intervalCount = bounds.size() / 2;
        if (intervalCount > maxIntervalCount) {
            // getMaxIntervalCount() got the bound wrong. Dropping the extra intervals would drop rows.
            throw CairoException.nonCritical()
                    .put("tick expression yields more intervals than the JIT filter checks [expression=")
                    .put(compiledExpr.getExpression())
                    .put(", intervals=").put(intervalCount)
                    .put(", max=").put(maxIntervalCount)
                    .put(']');
        }
        if (isDate) {
            // The Java filter matches a DATE d when d * 1000 lies in [lo, hi]. In milliseconds, that
            // is d in [ceil(lo / 1000), floor(hi / 1000)], which is empty when the interval holds no
            // whole millisecond. NULL stays NULL: DATE and TIMESTAMP share the sentinel.
            for (int i = 0, n = bounds.size(); i < n; i += 2) {
                final long lo = bounds.getQuick(i);
                final long hi = bounds.getQuick(i + 1);
                bounds.setQuick(i, lo == Numbers.LONG_NULL ? Numbers.LONG_NULL : Math.ceilDiv(lo, Micros.MILLI_MICROS));
                bounds.setQuick(i + 1, hi == Numbers.LONG_NULL ? Numbers.LONG_NULL : Math.floorDiv(hi, Micros.MILLI_MICROS));
            }
        }
        // After the DATE conversion, which must not touch the padding.
        for (int i = intervalCount; i < maxIntervalCount; i++) {
            bounds.add(EMPTY_INTERVAL_LO, EMPTY_INTERVAL_HI);
        }
        evaluatedNow = now;
        isEvaluated = true;
    }
}
