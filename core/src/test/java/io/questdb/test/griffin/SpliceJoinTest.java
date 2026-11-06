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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SpliceJoinTest extends AbstractCairoTest {
    private static final ObjList<TrackingFactory> inputs = new ObjList<>();

    @Test
    public void testConstructorFailureConsumesBothInputsAndKeepsPrimary() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException failure = new RuntimeException("splice map preparation");
            final RuntimeException closeFailure = new RuntimeException("master close");
            final boolean[] isArmed = {false};
            try (
                    CairoEngine failingEngine = new CairoEngine(new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath()) {
                        @Override
                        public int getSqlSmallMapKeyCapacity() {
                            if (isArmed[0]) {
                                throw failure;
                            }
                            return super.getSqlSmallMapKeyCapacity();
                        }
                    });
                    SqlExecutionContextImpl context = new SqlExecutionContextImpl(failingEngine, 1).with(AllowAllSecurityContext.INSTANCE)
            ) {
                failingEngine.load();
                registerSources(failingEngine.getFunctionFactoryCache());
                final TrackingFactory master = new TrackingFactory(metadata(ColumnType.INT));
                final TrackingFactory slave = new TrackingFactory(metadata(ColumnType.INT));
                master.closeFailure = closeFailure;
                setInputs(master, slave);
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(failingEngine)) {
                    isArmed[0] = true;
                    final RuntimeException actual = Assert.assertThrows(RuntimeException.class,
                            () -> compiler.compile("SELECT * FROM lp_join_m() m SPLICE JOIN lp_join_s() s ON k", context));
                    isArmed[0] = false;
                    Assert.assertSame(failure, actual);
                    Assert.assertEquals(1, actual.getSuppressed().length);
                    Assert.assertSame(closeFailure, actual.getSuppressed()[0]);
                    Assert.assertEquals(1, master.closeCount);
                    Assert.assertEquals(1, slave.closeCount);
                }
            }
        });
    }

    @Test
    public void testFactoryOwnsInputsAndCopiedKeyDiagnostic() throws Exception {
        assertMemoryLeak(() -> {
            registerSources(engine.getFunctionFactoryCache());
            try {
                for (int key = 0; key < 3; key++) {
                    final String on = key == 0 ? "" : key == 1 ? " ON k" : " ON ts";
                    final TrackingFactory master = new TrackingFactory(metadata(ColumnType.INT));
                    final TrackingFactory slave = new TrackingFactory(metadata(ColumnType.INT));
                    setInputs(master, slave);
                    try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                         RecordCursorFactory factory = compiler.compile(
                                 "SELECT * FROM lp_join_m() m SPLICE JOIN lp_join_s() s" + on, sqlExecutionContext
                         ).getRecordCursorFactory()) {
                        // Runtime diagnostics must survive compilation-pool reuse.
                        compiler.compile("SELECT 1 AS other FROM long_sequence(1)", sqlExecutionContext).getRecordCursorFactory().close();
                        Assert.assertEquals(0, master.closeCount);
                        Assert.assertEquals(0, slave.closeCount);
                        Assert.assertEquals(-1, factory.getMetadata().getTimestampIndex());
                        Assert.assertFalse(factory.recordCursorSupportsRandomAccess());
                        final TextPlanSink plan = new TextPlanSink();
                        plan.of(factory, sqlExecutionContext);
                        TestUtils.assertContains(plan.getSink(), "Splice Join");
                        if (key == 0) {
                            Assert.assertFalse(plan.getSink().toString().contains("condition:"));
                        } else {
                            final String keyName = key == 1 ? "k" : "ts";
                            TestUtils.assertContains(plan.getSink(), "condition: s." + keyName + "=m." + keyName);
                        }
                    }
                    Assert.assertEquals(1, master.closeCount);
                    Assert.assertEquals(1, slave.closeCount);
                }
            } finally {
                unregisterSources(engine.getFunctionFactoryCache());
            }
        });
    }

    @Test
    public void testOrderInsensitiveAggregateOverDescendingInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, qty LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO trades VALUES
                        ('2024-01-01T00:00:01.000000Z', 'A', 1),
                        ('2024-01-01T00:00:02.000000Z', 'B', 2),
                        ('2024-01-01T00:00:03.000000Z', 'A', 3),
                        ('2024-01-01T00:00:04.000000Z', 'B', 4)
                    """);
            execute("""
                    INSERT INTO quotes VALUES
                        ('2024-01-01T00:00:00.500000Z', 'A', 10),
                        ('2024-01-01T00:00:01.500000Z', 'B', 20),
                        ('2024-01-01T00:00:02.000000Z', 'B', 25),
                        ('2024-01-01T00:00:02.500000Z', 'A', 30),
                        ('2024-01-01T00:00:03.500000Z', 'B', 40),
                        ('2024-01-01T00:00:03.800000Z', 'A', 50)
                    """);
            final String unkeyed = """
                    count\tsum\tsum1
                    9\t19\t265
                    """;
            assertQuery("SELECT count(), sum(t.qty), sum(q.bid) FROM (SELECT * FROM trades ORDER BY ts DESC) t SPLICE JOIN quotes q")
                    .noRandomAccess()
                    .expectSize()
                    .returns(unkeyed);
            assertQuery("SELECT count(), sum(t.qty), sum(q.bid) FROM trades t SPLICE JOIN (SELECT * FROM quotes ORDER BY ts DESC) q")
                    .noRandomAccess()
                    .expectSize()
                    .returns(unkeyed);
            assertQuery("SELECT count(), sum(t.qty), sum(q.bid) FROM trades t SPLICE JOIN quotes q")
                    .noRandomAccess()
                    .expectSize()
                    .returns(unkeyed);
            final String keyed = """
                    count\tsum\tsum1
                    9\t14\t225
                    """;
            assertQuery("WITH d AS (SELECT * FROM trades ORDER BY ts DESC) SELECT count(), sum(t.qty), sum(q.bid) FROM d t SPLICE JOIN quotes q ON (sym)")
                    .noRandomAccess()
                    .expectSize()
                    .returns(keyed);
            assertQuery("SELECT count(), sum(t.qty), sum(q.bid) FROM trades t SPLICE JOIN quotes q ON (sym)")
                    .noRandomAccess()
                    .expectSize()
                    .returns(keyed);
            assertQuery("SELECT t.qty, q.bid FROM (SELECT * FROM trades ORDER BY ts DESC) t SPLICE JOIN quotes q")
                    .fails(67, "left side of time series join doesn't have ASC timestamp order");
            assertQuery("SELECT count(), sum(t.qty), sum(q.bid) FROM (SELECT * FROM trades ORDER BY qty DESC) t SPLICE JOIN quotes q")
                    .noRandomAccess()
                    .expectSize()
                    .returns(unkeyed);
            assertQuery("SELECT t.qty, q.bid FROM (SELECT * FROM trades ORDER BY qty DESC) t SPLICE JOIN quotes q")
                    .fails(68, "left side of time series join has no timestamp");
            final String resorted = """
                    qty\tbid
                    null\t10
                    1\t10
                    1\t20
                    2\t25
                    2\t30
                    3\t30
                    3\t40
                    3\t50
                    4\t50
                    """;
            assertQuery("SELECT t.qty, q.bid FROM (SELECT * FROM trades ORDER BY qty DESC) t SPLICE JOIN quotes q ORDER BY t.qty, q.bid")
                    .returns(resorted);
            assertQuery("SELECT t.qty, q.bid FROM trades t SPLICE JOIN quotes q ORDER BY t.qty, q.bid")
                    .returns(resorted);
        });
    }

    @Test
    public void testTimestampEndpointSlave() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, '2024-01-01T00:00:00'), (2, '2024-01-01T05:00:00'), (3, '2024-01-02T01:00:00')");
            assertQuery("SELECT t.l, x.r, x.k FROM t SPLICE JOIN ((SELECT min(ts) r, 42 k FROM t) TIMESTAMP(r)) x")
                    .noRandomAccess()
                    .withPlan("""
                            SelectedRecord
                                Splice Join
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                    SelectedRecord
                                        VirtualRecord
                                          functions: [r,42]
                                            Timestamp Endpoint
                                              value: min(ts)
                                                Limit value: 1 skip-rows: 0 take-rows: 1
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: t
                            """)
                    .returns("""
                            l	r	k
                            1	2024-01-01T00:00:00.000000Z	42
                            2	2024-01-01T00:00:00.000000Z	42
                            3	2024-01-01T00:00:00.000000Z	42
                            """);
            assertQuery("SELECT t.l, x.r FROM t SPLICE JOIN ((SELECT first(ts) r FROM t WHERE l >= 2) TIMESTAMP(r)) x")
                    .noRandomAccess()
                    .returns("""
                            l	r
                            1\t
                            2	2024-01-01T05:00:00.000000Z
                            3	2024-01-01T05:00:00.000000Z
                            """);
            assertQuery("SELECT t.l, x.r, x.k FROM t SPLICE JOIN ((SELECT max(ts) r, 42 k FROM t) TIMESTAMP(r)) x")
                    .noRandomAccess()
                    .returns("""
                            l	r	k
                            1		null
                            2		null
                            3	2024-01-02T01:00:00.000000Z	42
                            """);
            assertQuery("SELECT t.l, x.r, x.k FROM t SPLICE JOIN ((SELECT last(ts) r, 42 k FROM t WHERE l < 3) TIMESTAMP(r)) x")
                    .noRandomAccess()
                    .returns("""
                            l	r	k
                            1		null
                            2	2024-01-01T05:00:00.000000Z	42
                            3	2024-01-01T05:00:00.000000Z	42
                            """);
            assertQuery("SELECT t.l, x.r, x.k FROM t SPLICE JOIN ((SELECT min(ts) r, 42 k FROM t WHERE l > 1000) TIMESTAMP(r)) x")
                    .noRandomAccess()
                    .returns("""
                            l	r	k
                            null		42
                            1		42
                            2		42
                            3		42
                            """);
        });
    }

    @Test
    public void testValidationOrderAndFailureOwnership() throws Exception {
        assertMemoryLeak(() -> {
            registerSources(engine.getFunctionFactoryCache());
            try {
                for (int scenario = 0; scenario < 9; scenario++) {
                    final TrackingFactory master = new TrackingFactory(metadata(ColumnType.INT));
                    final TrackingFactory slave = new TrackingFactory(metadata(scenario == 5 ? ColumnType.LONG : ColumnType.INT));
                    String on = " ON k";
                    final String expected;
                    int expectedPosition = 28;
                    switch (scenario) {
                        case 0 -> {
                            master.metadata.setTimestampIndex(-1);
                            master.direction = RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
                            expected = "left side of time series join has no timestamp";
                        }
                        case 1 -> {
                            slave.metadata.setTimestampIndex(-1);
                            master.direction = RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
                            expected = "right side of time series join has no timestamp";
                        }
                        case 2 -> {
                            master.direction = RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
                            slave.direction = RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
                            expected = "left side of time series join doesn't have ASC timestamp order";
                        }
                        case 3 -> {
                            slave.direction = RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
                            expected = "right side of time series join doesn't have ASC timestamp order";
                        }
                        case 4 -> {
                            on = " ON s.k > m.k";
                            expected = "unsupported SPLICE join expression [expr='s.k > m.k']";
                            expectedPosition = 61;
                        }
                        case 5 -> {
                            expected = "join column type mismatch";
                            expectedPosition = 57;
                        }
                        case 6 -> {
                            master.isRandomAccess = false;
                            slave.isRandomAccess = false;
                            expected = "left side of splice join doesn't support random access";
                        }
                        case 7 -> {
                            slave.isRandomAccess = false;
                            expected = "right side of splice join doesn't support random access";
                        }
                        default -> expected = "splice join doesn't support full fat mode";
                    }
                    setInputs(master, slave);
                    try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                        // Every earlier rejection must win even when full-fat mode is also invalid.
                        compiler.setFullFatJoins(true);
                        final String sql = "SELECT * FROM lp_join_m() m SPLICE JOIN lp_join_s() s" + on;
                        final SqlException error = Assert.assertThrows(SqlException.class, () -> compiler.compile(sql, sqlExecutionContext));
                        Assert.assertEquals(expected, error.getFlyweightMessage().toString());
                        Assert.assertEquals(expectedPosition, error.getPosition());
                        Assert.assertEquals(1, master.closeCount);
                        Assert.assertEquals(1, slave.closeCount);
                    }
                }
            } finally {
                unregisterSources(engine.getFunctionFactoryCache());
            }
        });
    }

    static GenericRecordMetadata metadata(int keyType) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("k", keyType));
        metadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP_MICRO));
        metadata.setTimestampIndex(1);
        return metadata;
    }

    private static void registerSource(FunctionFactoryCache cache, String name, int inputIndex) throws SqlException {
        final ObjList<FunctionFactoryDescriptor> descriptors = new ObjList<>();
        descriptors.add(new FunctionFactoryDescriptor(new FunctionFactory() {
            @Override
            public String getSignature() {
                return name + "()";
            }

            @Override
            public boolean isCursor() {
                return true;
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                        CairoConfiguration configuration, SqlExecutionContext executionContext) {
                return new CursorFunction(inputs.getQuick(inputIndex));
            }
        }));
        cache.getFactories().put(name, descriptors);
    }

    static void registerSources(FunctionFactoryCache cache) throws SqlException {
        registerSource(cache, "lp_join_m", 0);
        registerSource(cache, "lp_join_s", 1);
    }

    static void setInputs(TrackingFactory master, TrackingFactory slave) {
        inputs.clear();
        inputs.add(master);
        inputs.add(slave);
    }

    static void unregisterSources(FunctionFactoryCache cache) {
        cache.getFactories().remove("lp_join_m");
        cache.getFactories().remove("lp_join_s");
    }

    static final class TrackingFactory implements RecordCursorFactory {
        final GenericRecordMetadata metadata;
        int closeCount;
        RuntimeException closeFailure;
        int direction = SCAN_DIRECTION_FORWARD;
        boolean isRandomAccess = true;

        TrackingFactory(GenericRecordMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public void close() {
            Assert.assertEquals("input closed twice", 1, ++closeCount);
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RecordMetadata getMetadata() {
            return metadata;
        }

        @Override
        public int getScanDirection() {
            return direction;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return isRandomAccess;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("input");
        }
    }
}
