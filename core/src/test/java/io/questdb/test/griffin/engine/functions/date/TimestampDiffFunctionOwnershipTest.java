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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.date.TimestampDiffFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TimestampDiffFunctionOwnershipTest extends AbstractCairoTest {
    @Test
    public void testInvalidConstantPeriodClosesBothValuesWithoutEvaluation() throws Exception {
        assertMemoryLeak(() -> {
            final TimestampDiffFunctionFactory factory = new TimestampDiffFunctionFactory();
            for (int type : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                for (char period : new char[]{'?', '\0'}) {
                    final CountingTimestampFunction start = new CountingTimestampFunction(type, false);
                    final CountingTimestampFunction end = new CountingTimestampFunction(type, false);
                    try (Function result = factory.newInstance(0, new ObjList<>(CharConstant.newInstance(period), start, end),
                            new IntList(), configuration, sqlExecutionContext)) {
                        // This branch historically returns a timestamp NULL, unlike valid datediff's LONG.
                        Assert.assertEquals(type, result.getType());
                        Assert.assertEquals(Numbers.LONG_NULL, result.getTimestamp(null));
                        Assert.assertEquals(1, start.closeCount);
                        Assert.assertEquals(1, end.closeCount);
                        Assert.assertEquals(0, start.readCount);
                        Assert.assertEquals(0, end.readCount);
                    }
                    Assert.assertEquals(1, start.closeCount);
                    Assert.assertEquals(1, end.closeCount);
                }
            }
        });
    }

    @Test
    public void testValidConstantPeriodRetainsBothValuesUntilResultCloses() throws Exception {
        assertMemoryLeak(() -> {
            final CountingTimestampFunction start = new CountingTimestampFunction(ColumnType.TIMESTAMP_MICRO, false);
            final CountingTimestampFunction end = new CountingTimestampFunction(ColumnType.TIMESTAMP_NANO, false);
            try (Function result = new TimestampDiffFunctionFactory().newInstance(0,
                    new ObjList<>(CharConstant.newInstance('n'), start, end), new IntList(), configuration, sqlExecutionContext)) {
                Assert.assertEquals(ColumnType.LONG, result.getType());
                Assert.assertEquals(0, start.closeCount);
                Assert.assertEquals(0, end.closeCount);
                Assert.assertNotEquals(Numbers.LONG_NULL, result.getLong(null));
                Assert.assertEquals(1, start.readCount);
                Assert.assertEquals(1, end.readCount);
            }
            Assert.assertEquals(1, start.closeCount);
            Assert.assertEquals(1, end.closeCount);
        });
    }

    @Test
    public void testSelectedConstructionFailureDoesNotCloseDiscardedArgumentTwice() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final FunctionFactoryDescriptor descriptor = new FunctionFactoryDescriptor(new TimestampDiffFunctionFactory());
            for (boolean isFirstFailure : new boolean[]{true, false}) {
                final CountingTimestampFunction start = new CountingTimestampFunction(ColumnType.TIMESTAMP_MICRO, isFirstFailure);
                final CountingTimestampFunction end = new CountingTimestampFunction(ColumnType.TIMESTAMP_NANO, !isFirstFailure);
                try (Function ignored = parser.createFunction(descriptor, 0, "datediff",
                        new ObjList<>(CharConstant.newInstance('?'), start, end), new IntList(), sqlExecutionContext)) {
                    Assert.fail("close failure must propagate");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "datediff argument close");
                }
                Assert.assertEquals(1, start.closeCount);
                Assert.assertEquals(1, end.closeCount);
                Assert.assertEquals(0, start.readCount);
                Assert.assertEquals(0, end.readCount);
            }
        });
    }

    private static class CountingTimestampFunction extends TimestampFunction {
        private final boolean isCloseFailure;
        private int closeCount;
        private int readCount;

        private CountingTimestampFunction(int type, boolean isCloseFailure) {
            super(type);
            this.isCloseFailure = isCloseFailure;
        }

        @Override
        public void close() {
            closeCount++;
            if (isCloseFailure) {
                throw new IllegalStateException("datediff argument close");
            }
        }

        @Override
        public long getTimestamp(Record rec) {
            readCount++;
            return 123;
        }
    }
}
