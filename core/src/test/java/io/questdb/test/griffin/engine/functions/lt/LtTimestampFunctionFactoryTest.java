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

package io.questdb.test.griffin.engine.functions.lt;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.lt.LtTimestampFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.test.TestTimestampType;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class LtTimestampFunctionFactoryTest extends AbstractFunctionFactoryTest {
    private final TestTimestampType timestampType;

    public LtTimestampFunctionFactoryTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testGreaterOrEqThanNull() throws SqlException, NumericException {
        long t1 = timestampType.getDriver().parseFloorLiteral("2020-12-31T23:59:59.000000Z");
        long t2 = Numbers.LONG_NULL;
        callBySignature(">=(NN)", t1, t1).andAssert(true);
        callBySignature(">=(NN)", t1, t2).andAssert(false);
        callBySignature(">=(NN)", t2, t1).andAssert(false);
        callBySignature(">=(NN)", t2, t2).andAssert(true);
    }

    @Test
    public void testGreaterThan() throws SqlException, NumericException {
        long t1 = timestampType.getDriver().parseFloorLiteral("2020-12-31T23:59:59.000000Z");
        long t2 = timestampType.getDriver().parseFloorLiteral("2020-12-31T23:59:59.000001Z");
        callBySignature(">(NN)", t1, t1).andAssert(false);
        callBySignature(">(NN)", t1, t2).andAssert(false);
        callBySignature(">(NN)", t2, t1).andAssert(true);
    }

    @Test
    public void testGreaterThanNull() throws SqlException, NumericException {
        long t1 = timestampType.getDriver().parseFloorLiteral("2020-12-31T23:59:59.000000Z");
        long t2 = Numbers.LONG_NULL;
        callBySignature(">(NN)", t1, t1).andAssert(false);
        callBySignature(">(NN)", t1, t2).andAssert(false);
        callBySignature(">(NN)", t2, t1).andAssert(false);
        callBySignature(">(NN)", t2, t2).andAssert(false);
    }

    @Test
    public void testGreaterThanOrEqualTo() throws SqlException, NumericException {
        long t1 = timestampType.getDriver().parseFloorLiteral("2020-12-31T23:59:59.000000Z");
        long t2 = timestampType.getDriver().parseFloorLiteral("2020-12-31T23:59:59.000001Z");
        callBySignature(">=(NN)", t1, t1).andAssert(true);
        callBySignature(">=(NN)", t1, t2).andAssert(false);
        callBySignature(">=(NN)", t2, t1).andAssert(true);
    }

    @Test
    public void testLessOrEqThanNull() throws SqlException, NumericException {
        long t1 = timestampType.getDriver().parseFloorLiteral("2020-12-31T23:59:59.000000Z");
        long t2 = Numbers.LONG_NULL;
        callBySignature("<=(NN)", t1, t1).andAssert(true);
        callBySignature("<=(NN)", t1, t2).andAssert(false);
        callBySignature("<=(NN)", t2, t1).andAssert(false);
        callBySignature("<=(NN)", t2, t2).andAssert(true);
    }

    @Test
    public void testLessThan() throws SqlException, NumericException {
        long t1 = timestampType.getDriver().parseFloorLiteral("2020-12-31T23:59:59.000000Z");
        long t2 = timestampType.getDriver().parseFloorLiteral("2020-12-31T23:59:59.000001Z");
        callBySignature("<(NN)", t1, t1).andAssert(false);
        callBySignature("<(NN)", t1, t2).andAssert(true);
        callBySignature("<(NN)", t2, t1).andAssert(false);
    }

    @Test
    public void testLessThanOrEqualTo() throws SqlException, NumericException {
        long t1 = timestampType.getDriver().parseFloorLiteral("2020-12-31T23:59:59.000000Z");
        long t2 = timestampType.getDriver().parseFloorLiteral("2020-12-31T23:59:59.000001Z");
        callBySignature("<=(NN)", t1, t1).andAssert(true);
        callBySignature("<=(NN)", t1, t2).andAssert(true);
        callBySignature("<=(NN)", t2, t1).andAssert(false);
    }

    @Test
    public void testMixedMicrosAndNanos() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampNano(timestampType.getTimestampType()));
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select " +
                    "timestamp_sequence(1000000, 1000000) as ts, " +
                    "timestamp_sequence_ns(0, 2000000000) as ts_ns " +
                    "from long_sequence(4)" +
                    ") timestamp(ts)");
            assertQuery("select ts, ts_ns, ts > ts_ns from x")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tts_ns\tcolumn
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:00.000000000Z\ttrue
                            1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\tfalse
                            1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000000Z\tfalse
                            1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:06.000000000Z\tfalse
                            """);
            assertQuery("select ts, ts_ns, ts >= ts_ns from x")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tts_ns\tcolumn
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:00.000000000Z\ttrue
                            1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\ttrue
                            1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000000Z\tfalse
                            1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:06.000000000Z\tfalse
                            """);

            assertQuery("select ts, ts_ns, ts < ts_ns from x")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tts_ns\tcolumn
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:00.000000000Z\tfalse
                            1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\tfalse
                            1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000000Z\ttrue
                            1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:06.000000000Z\ttrue
                            """);
            assertQuery("select ts, ts_ns, ts <= ts_ns from x")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tts_ns\tcolumn
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:00.000000000Z\tfalse
                            1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\ttrue
                            1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000000Z\ttrue
                            1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:06.000000000Z\ttrue
                            """);

            assertQuery("select ts, ts_ns, ts_ns > ts from x")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tts_ns\tcolumn
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:00.000000000Z\tfalse
                            1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\tfalse
                            1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000000Z\ttrue
                            1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:06.000000000Z\ttrue
                            """);
            assertQuery("select ts, ts_ns, ts_ns >= ts from x")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tts_ns\tcolumn
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:00.000000000Z\tfalse
                            1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\ttrue
                            1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000000Z\ttrue
                            1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:06.000000000Z\ttrue
                            """);

            assertQuery("select ts, ts_ns, ts_ns < ts from x")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tts_ns\tcolumn
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:00.000000000Z\ttrue
                            1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\tfalse
                            1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000000Z\tfalse
                            1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:06.000000000Z\tfalse
                            """);
            assertQuery("select ts, ts_ns, ts_ns <= ts from x")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tts_ns\tcolumn
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:00.000000000Z\ttrue
                            1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\ttrue
                            1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000000Z\tfalse
                            1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:06.000000000Z\tfalse
                            """);
        });
    }

    @Test
    public void testRuntimeConstantTextParsesOnce() throws Exception {
        final long value = timestampType.getDriver().parseFloorLiteral("2020-12-31T23:59:59.000000Z");
        for (int textIndex = 0; textIndex < 2; textIndex++) {
            final CountingTextFunction text = new CountingTextFunction("2021-01-01T00:00:00.000000Z");
            final ObjList<Function> args = new ObjList<>();
            args.add(textIndex == 0 ? text : new ColumnFunction(timestampType.getTimestampType(), value));
            args.add(textIndex == 0 ? new ColumnFunction(timestampType.getTimestampType(), value) : text);
            try (Function function = getFunctionFactory().newInstance(0, args, null, configuration, sqlExecutionContext)) {
                function.init(null, sqlExecutionContext);
                for (int i = 0; i < 1_000; i++) {
                    Assert.assertEquals(textIndex == 1, function.getBool(null));
                }
                Assert.assertEquals(1, text.getStrACalls);
            }
        }
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LtTimestampFunctionFactory();
    }

    private static class ColumnFunction extends TimestampFunction {
        private final long value;

        private ColumnFunction(int timestampType, long value) {
            super(timestampType);
            this.value = value;
        }

        @Override
        public long getTimestamp(Record rec) {
            return value;
        }
    }

    private static class CountingTextFunction extends StrFunction {
        private final String value;
        private int getStrACalls;

        private CountingTextFunction(String value) {
            this.value = value;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            getStrACalls++;
            return value;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStrA(rec);
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }
    }
}
