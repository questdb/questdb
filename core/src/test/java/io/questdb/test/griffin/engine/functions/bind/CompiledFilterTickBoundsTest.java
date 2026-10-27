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

package io.questdb.test.griffin.engine.functions.bind;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.bind.CompiledFilterTickBoundBindVariable;
import io.questdb.griffin.engine.functions.bind.CompiledFilterTickBounds;
import io.questdb.griffin.model.CompiledTickExpression;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CompiledFilterTickBoundsTest extends AbstractCairoTest {
    // 2024-01-05 is a Friday and 2024-01-06 a Saturday.
    private static final long FRI = Micros.toMicros(2024, 1, 5, 10, 0);
    private static final long FRI_END = Micros.toMicros(2024, 1, 6, 0, 0) - 1;
    private static final long FRI_START = Micros.toMicros(2024, 1, 5, 0, 0);
    private static final long SAT = Micros.toMicros(2024, 1, 6, 10, 0);

    @Test
    public void testBoundAboveMaxIntervalCountFailsQuery() throws Exception {
        // getMaxIntervalCount() proves the bound the JIT filter relies on. Were it wrong, dropping the
        // intervals past the bound would drop rows, so the query fails instead.
        final CompiledFilterTickBounds bounds = newBounds("[$yesterday, $tomorrow]", ColumnType.TIMESTAMP, 1, false);
        for (int i = 0; i < 2; i++) {
            setCurrentMicros(FRI);
            try {
                bounds.init(sqlExecutionContext);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "tick expression yields more intervals than the JIT filter checks [expression=[$yesterday, $tomorrow], intervals=2, max=1]"
                );
            }
        }
    }

    @Test
    public void testBoundsFollowTheClock() throws Exception {
        final CompiledFilterTickBounds bounds = newBounds("$today#workday", ColumnType.TIMESTAMP, 2, false);
        final CompiledFilterTickBoundBindVariable lo0 = new CompiledFilterTickBoundBindVariable(bounds, 0);
        final CompiledFilterTickBoundBindVariable hi1 = new CompiledFilterTickBoundBindVariable(bounds, 3);

        setCurrentMicros(FRI);
        lo0.init(null, sqlExecutionContext);
        hi1.init(null, sqlExecutionContext);
        assertBounds(bounds, FRI_START, FRI_END, Long.MAX_VALUE, Long.MIN_VALUE);
        Assert.assertEquals(FRI_START, lo0.getLong(null));
        Assert.assertEquals(Long.MIN_VALUE, hi1.getLong(null));

        // No interval at a weekend: both checks get the padding, which matches no row.
        setCurrentMicros(SAT);
        lo0.init(null, sqlExecutionContext);
        assertBounds(bounds, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, lo0.getLong(null));

        // The clock at 0, which no "now" sentinel could tell from an unset one: 1970-01-01 is a
        // Thursday.
        setCurrentMicros(0);
        hi1.init(null, sqlExecutionContext);
        assertBounds(bounds, 0, Micros.DAY_MICROS - 1, Long.MAX_VALUE, Long.MIN_VALUE);
    }

    @Test
    public void testDateBoundsInMilliseconds() throws Exception {
        // The JIT compares a DATE key in milliseconds, and a micro interval holds the DATE values
        // whose micros it holds. The padding keeps its values.
        setCurrentMicros(FRI);
        CompiledFilterTickBounds bounds = newBounds("$yesterday", ColumnType.TIMESTAMP, 2, true);
        bounds.init(sqlExecutionContext);
        assertBounds(bounds, FRI_START / 1000 - 86_400_000, FRI_START / 1000 - 1, Long.MAX_VALUE, Long.MIN_VALUE);

        // [09:59:59.999999, 09:59:59.999999] holds no whole millisecond.
        bounds = newBounds("$now - 1u", ColumnType.TIMESTAMP, 1, true);
        bounds.init(sqlExecutionContext);
        assertBounds(bounds, FRI / 1000, FRI / 1000 - 1);

        // [10:00:00.000500, 10:00:00.001499] holds 10:00:00.001.
        bounds = newBounds("$now + 500u;1T", ColumnType.TIMESTAMP, 1, true);
        bounds.init(sqlExecutionContext);
        assertBounds(bounds, FRI / 1000 + 1, FRI / 1000 + 1);
    }

    @Test
    public void testNanosBounds() throws Exception {
        final CompiledFilterTickBounds bounds = newBounds("$now - 1h..$now", ColumnType.TIMESTAMP_NANO, 1, false);
        setCurrentMicros(FRI);
        bounds.init(sqlExecutionContext);
        assertBounds(bounds, (FRI - Micros.HOUR_MICROS) * 1000, FRI * 1000);
    }

    private static void assertBounds(CompiledFilterTickBounds bounds, long... expected) {
        Assert.assertEquals(expected.length, 2 * bounds.getMaxIntervalCount());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("bound " + i, expected[i], bounds.getBound(i));
        }
    }

    private static CompiledFilterTickBounds newBounds(
            String expression,
            int timestampType,
            int maxIntervalCount,
            boolean isDate
    ) throws SqlException {
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        final CompiledTickExpression compiledExpr = IntervalUtils.compileTickExpr(
                driver,
                configuration,
                expression,
                0,
                expression.length(),
                0
        );
        return new CompiledFilterTickBounds(compiledExpr, driver.getTimestampType(), maxIntervalCount, isDate);
    }
}
