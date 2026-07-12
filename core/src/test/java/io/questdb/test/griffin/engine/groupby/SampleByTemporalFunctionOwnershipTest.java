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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.test.TestCloseCounterFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Exact-once lifetime tests for the SAMPLE BY temporal parameter functions (timezone, offset,
 * FROM, TO). The generator accepts runtime-constant expressions for all four, so they may own
 * child functions and must be closed exactly once - by the factory that adopted them, at factory
 * teardown, not by the per-execution cursors of a cached factory. {@code test_close_counter()}
 * produces a runtime-constant STRING function that counts its close() calls.
 */
public class SampleByTemporalFunctionOwnershipTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // enables the test_close_counter() instrumentation
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testFillLinearClosesTemporalFunctionsOnce() throws Exception {
        // interpolation path: SampleByInterpolateRecordCursorFactory
        assertMemoryLeak(() -> {
            createPriceTable();
            assertTemporalFunctionsClosedOnce(
                    "select ts, sum(price) s from t sample by 1h fill(linear) " +
                            "align to calendar time zone test_close_counter('UTC') " +
                            "with offset test_close_counter('00:00')",
                    "fill: linear",
                    "ts\ts\n1970-01-01T00:00:00.000000Z\t6\n"
            );
        });
    }

    @Test
    public void testFillLinearFromToClosesTemporalFunctionsOnce() throws Exception {
        // interpolation path with all four temporal parameters: the linear-fill guard in the
        // optimiser keeps FROM/TO queries on SampleByInterpolateRecordCursorFactory, which owns
        // sampleFromFunc and sampleToFunc independently of timezone and offset; a whole-day
        // stride avoids the compile-time timezone normalization that would replace the counted
        // FROM/TO functions with constants before the factory adopts them
        assertMemoryLeak(() -> {
            createPriceTable();
            assertTemporalFunctionsClosedOnce(
                    "select ts, sum(price) s from t sample by 1d " +
                            "from test_close_counter('1970-01-01') to test_close_counter('1970-01-02') " +
                            "fill(linear) " +
                            "align to calendar time zone test_close_counter('UTC') " +
                            "with offset test_close_counter('00:00')",
                    "fill: linear",
                    "ts\ts\n1970-01-01T00:00:00.000000Z\t6\n"
            );
        });
    }

    @Test
    public void testFirstLastClosesTemporalFunctionsOnce() throws Exception {
        // index-backed first/last fast path: SampleByFirstLastRecordCursorFactory
        assertMemoryLeak(() -> {
            execute("create table tfl (sym symbol index, price long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into tfl values ('a', 1, 0), ('b', 5, 30000000), ('a', 3, 60000000)");
            // the expression stride with a separate unit keeps the optimiser from rewriting the
            // query into GROUP BY timestamp_floor, so it reaches SampleByFirstLastRecordCursorFactory
            assertTemporalFunctionsClosedOnce(
                    "select ts, first(price) f, last(price) l from tfl where sym = 'a' sample by (1+0) h " +
                            "align to calendar time zone test_close_counter('UTC') " +
                            "with offset test_close_counter('00:00')",
                    "SampleByFirstLast",
                    "ts\tf\tl\n1970-01-01T00:00:00.000000Z\t1\t3\n"
            );
        });
    }

    @Test
    public void testGenerateFillResidualCleanupClosesOnceAndContinuesRollback() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price LONG, sym STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            TestCloseCounterFunctionFactory.reset();
            TestFaultFunctionFactory.armCloseFailures();
            try {
                select(
                        "SELECT test_close_counter('8') || sym key, sum(price) total, avg(price) value, ts " +
                                "FROM t SAMPLE BY 1d " +
                                "FILL(0, 0, test_fault()) ALIGN TO CALENDAR " +
                                "TIME ZONE test_close_counter('residual-timezone-owner') " +
                                "WITH OFFSET test_close_counter('residual-offset-owner')"
                ).close();
                Assert.fail("residual fill close failure expected");
            } catch (Throwable failure) {
                Assert.assertSame(TestFaultFunctionFactory.closeFailure(0), failure);
                Assert.assertEquals(0, failure.getSuppressed().length);
                Assert.assertEquals(1, TestFaultFunctionFactory.created());
                Assert.assertEquals(1, TestFaultFunctionFactory.closeCalls());
                Assert.assertTrue(TestCloseCounterFunctionFactory.created("8") > 0);
                Assert.assertEquals(
                        TestCloseCounterFunctionFactory.created("8"),
                        TestCloseCounterFunctionFactory.closeCalls("8")
                );
                Assert.assertTrue(TestCloseCounterFunctionFactory.created("residual-timezone-owner") > 0);
                Assert.assertEquals(
                        TestCloseCounterFunctionFactory.created("residual-timezone-owner"),
                        TestCloseCounterFunctionFactory.closeCalls("residual-timezone-owner")
                );
                Assert.assertTrue(TestCloseCounterFunctionFactory.created("residual-offset-owner") > 0);
                Assert.assertEquals(
                        TestCloseCounterFunctionFactory.created("residual-offset-owner"),
                        TestCloseCounterFunctionFactory.closeCalls("residual-offset-owner")
                );
                Assert.assertEquals(0, TestCloseCounterFunctionFactory.multiClosed());
            } finally {
                TestFaultFunctionFactory.disarm();
            }
        });
    }

    @Test
    public void testGenerateFillRollbackPreservesPrimaryAndContinuesCleanup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price LONG, sym STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            TestCloseCounterFunctionFactory.reset();
            TestFaultFunctionFactory.armCloseFailures();
            try {
                select(
                        "SELECT test_close_counter('7') || sym key, sum(price) total, avg(price) value, ts " +
                                "FROM t SAMPLE BY 1d " +
                                "FILL(0, PREV(no_such_column), test_fault()) ALIGN TO CALENDAR " +
                                "TIME ZONE test_close_counter('timezone-owner') " +
                                "WITH OFFSET test_close_counter('offset-owner')"
                ).close();
                Assert.fail("invalid PREV source failure expected");
            } catch (Throwable failure) {
                Assert.assertTrue(failure.toString(), failure instanceof io.questdb.griffin.SqlException);
                TestUtils.assertContains(failure.getMessage(), "PREV(col): column not found in output");
                Assert.assertArrayEquals(
                        new Throwable[]{TestFaultFunctionFactory.closeFailure(0)},
                        failure.getSuppressed()
                );
                Assert.assertEquals(1, TestFaultFunctionFactory.created());
                Assert.assertEquals(1, TestFaultFunctionFactory.closeCalls());
                Assert.assertTrue(TestCloseCounterFunctionFactory.created("7") > 0);
                Assert.assertEquals(
                        TestCloseCounterFunctionFactory.created("7"),
                        TestCloseCounterFunctionFactory.closeCalls("7")
                );
                Assert.assertTrue(TestCloseCounterFunctionFactory.created("timezone-owner") > 0);
                Assert.assertEquals(
                        TestCloseCounterFunctionFactory.created("timezone-owner"),
                        TestCloseCounterFunctionFactory.closeCalls("timezone-owner")
                );
                Assert.assertTrue(TestCloseCounterFunctionFactory.created("offset-owner") > 0);
                Assert.assertEquals(
                        TestCloseCounterFunctionFactory.created("offset-owner"),
                        TestCloseCounterFunctionFactory.closeCalls("offset-owner")
                );
                Assert.assertEquals(0, TestCloseCounterFunctionFactory.multiClosed());
            } finally {
                TestFaultFunctionFactory.disarm();
            }
        });
    }

    @Test
    public void testKeyedSampleByClosesTemporalFunctionsOnce() throws Exception {
        // ordinary keyed path: SampleByFillNoneRecordCursorFactory
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "  select x price, cast(case when x % 2 = 0 then 'a' else 'b' end as symbol) sym," +
                    "         timestamp_sequence(0, 60000000) ts" +
                    "  from long_sequence(3)" +
                    ") timestamp(ts) partition by day");
            // the expression stride with a separate unit keeps the optimiser from rewriting the
            // query into GROUP BY timestamp_floor, so it reaches the keyed SampleBy factory
            assertTemporalFunctionsClosedOnce(
                    "select ts, sym, sum(price) s from t sample by (1+0) h " +
                            "align to calendar time zone test_close_counter('UTC') " +
                            "with offset test_close_counter('00:00')",
                    "Sample By",
                    """
                            ts\tsym\ts
                            1970-01-01T00:00:00.000000Z\tb\t4
                            1970-01-01T00:00:00.000000Z\ta\t2
                            """
            );
        });
    }

    @Test
    public void testNotKeyedFromToClosesTemporalFunctionsOnce() throws Exception {
        // ordinary not-keyed path with all four parameters: FROM/TO accept runtime-constant
        // expressions convertible to TIMESTAMP; a whole-day stride avoids the sub-day timezone
        // normalization, which is covered separately
        assertMemoryLeak(() -> {
            createPriceTable();
            assertTemporalFunctionsClosedOnce(
                    "select ts, sum(price) s from t sample by 1d " +
                            "from test_close_counter('1970-01-01') to test_close_counter('1970-01-02') " +
                            "align to calendar time zone test_close_counter('UTC') " +
                            "with offset test_close_counter('00:00')",
                    "Sample By",
                    "ts\ts\n1970-01-01T00:00:00.000000Z\t6\n"
            );
        });
    }

    @Test
    public void testTimezoneNormalizationClosesReplacedBounds() throws Exception {
        // Sub-day calendar sampling with a timezone converts FROM/TO to UTC at compile time by
        // swapping in TimestampConstant replacements. The replaced original functions must be
        // closed exactly once at the swap; the replacements then live and die with the factory.
        assertMemoryLeak(() -> {
            createPriceTable();
            TestCloseCounterFunctionFactory.reset();
            try (RecordCursorFactory factory = select(
                    "select ts, sum(price) s from t sample by 1h " +
                            "from test_close_counter('1970-01-01') to test_close_counter('1970-01-02') " +
                            "align to calendar time zone test_close_counter('UTC') " +
                            "with offset test_close_counter('00:00')"
            )) {
                Assert.assertTrue(TestCloseCounterFunctionFactory.created() > 0);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor("ts\ts\n1970-01-01T00:00:00.000000Z\t6\n", cursor, factory.getMetadata(), true, sink);
                }
            }
            Assert.assertEquals(
                    "every temporal function must close exactly once, including the originals " +
                            "replaced by the timezone normalization",
                    TestCloseCounterFunctionFactory.created(),
                    TestCloseCounterFunctionFactory.closeCalls()
            );
            Assert.assertEquals(0, TestCloseCounterFunctionFactory.multiClosed());
        });
    }

    @Test
    public void testTimezoneNormalizationFailureClosesAllBounds() throws Exception {
        // When the FROM bound has already been replaced and the TO bound's compile-time
        // conversion then throws, every function - the replaced original, the replacement, the
        // still-current TO original, timezone and offset - must be closed exactly once.
        assertMemoryLeak(() -> {
            createPriceTable();
            TestCloseCounterFunctionFactory.reset();
            try {
                select(
                        "select ts, sum(price) s from t sample by 1h " +
                                "from test_close_counter('1970-01-01') to test_close_counter('nope') " +
                                "align to calendar time zone test_close_counter('UTC') " +
                                "with offset test_close_counter('00:00')"
                ).close();
                Assert.fail("conversion failure expected");
            } catch (Throwable e) {
                TestUtils.assertContains(e.getMessage(), "inconvertible value");
            }
            Assert.assertEquals(
                    "every temporal function must close exactly once when the TO conversion " +
                            "fails after FROM was replaced",
                    TestCloseCounterFunctionFactory.created(),
                    TestCloseCounterFunctionFactory.closeCalls()
            );
            Assert.assertEquals(0, TestCloseCounterFunctionFactory.multiClosed());
        });
    }

    private void assertTemporalFunctionsClosedOnce(String query, String expectedPlanFragment, String expected) throws Exception {
        // Pin the factory type: whole-day strides and plain-literal periods are rewritten into
        // GROUP BY timestamp_floor, which would put the counted functions under a different
        // owner and make the closure assertions vacuous for the SampleBy factories.
        assertQuery(query)
                .noLeakCheck()
                .assertsPlanContaining(expectedPlanFragment);
        TestCloseCounterFunctionFactory.reset();
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertTrue("query must parse at least one counted temporal function", TestCloseCounterFunctionFactory.created() > 0);
            // two executions of the cached factory: per-execution cursor teardown must not close
            // the borrowed temporal functions, or the second execution reads closed functions
            for (int i = 0; i < 2; i++) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
                }
            }
            Assert.assertEquals(
                    "no temporal function may close before the owning factory closes",
                    0,
                    TestCloseCounterFunctionFactory.closeCalls()
            );
        }
        Assert.assertEquals(
                "every temporal function must close exactly once at factory teardown",
                TestCloseCounterFunctionFactory.created(),
                TestCloseCounterFunctionFactory.closeCalls()
        );
        Assert.assertEquals(
                "no temporal function may close more than once",
                0,
                TestCloseCounterFunctionFactory.multiClosed()
        );
    }

    private void createPriceTable() throws Exception {
        execute("create table t as (" +
                "  select x price, timestamp_sequence(0, 60000000) ts" +
                "  from long_sequence(3)" +
                ") timestamp(ts) partition by day");
    }
}
