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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.groupby.SampleByInterpolateRecordCursorFactory;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

public class SampleByInterpolateRewindTest extends AbstractCairoTest {
    private static final String EXPECTED = """
            ts\tk\tv
            2024-01-01T00:00:00.000000Z\ta\t1
            2024-01-01T00:00:00.000000Z\tlate-key\t2
            2024-01-01T01:00:00.000000Z\ta\t3
            2024-01-01T01:00:00.000000Z\tlate-key\t6
            2024-01-01T02:00:00.000000Z\tlate-key\t10
            2024-01-01T02:00:00.000000Z\ta\t5
            2024-01-01T03:00:00.000000Z\tlate-key\t14
            2024-01-01T03:00:00.000000Z\ta\t7
            """;
    private static final String SQL = "SELECT ts, k, sum(v) v FROM prices "
            + "SAMPLE BY 1h FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'UTC'";

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "false");
        super.setUp();
    }

    @Test
    public void testCrossJoinLimitedLinearRhs() throws Exception {
        assertMemoryLeak(() -> {
            createPrices("SYMBOL", "'late-key'");
            assertQuery("SELECT m.x, s.k, s.v FROM long_sequence(4) m CROSS JOIN (" + SQL + " LIMIT 1) s")
                    .noLeakCheck().noRandomAccess()
                    .withPlanContaining("Cross Join", "Limit", "Sample By", "Stream Group By")
                    .withPlanNotContaining("Materialize", "Sort")
                    .returns("""
                            x\tk\tv
                            1\ta\t1
                            2\ta\t1
                            3\ta\t1
                            4\ta\t1
                            """);
        });
    }

    @Test
    public void testLateGapCancellationThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            createPrices("VARCHAR", "'late-key'");
            assertQuery(SQL).noLeakCheck().assertsPlanContaining("Stream Group By");
            try (
                    NetworkSqlExecutionCircuitBreaker breaker = new NetworkSqlExecutionCircuitBreaker(engine,
                            new DefaultSqlExecutionCircuitBreakerConfiguration() {
                                @Override
                                public int getCircuitBreakerThrottle() {
                                    return 0;
                                }
                            });
                    SqlExecutionContextImpl cancellable = new SqlExecutionContextImpl(engine, 1)
                            .with(sqlExecutionContext.getSecurityContext(), null, null, -1, breaker);
                    RecordCursorFactory factory = select(SQL);
                    MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                            sqlExecutionContext.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)
            ) {
                cancellable.setMemoryTracker(tracker);
                try {
                    for (int iteration = 0; iteration < 2; iteration++) {
                        try (RecordCursor cursor = factory.getCursor(cancellable)) {
                            breaker.cancel();
                            try {
                                cursor.hasNext();
                                Assert.fail("expected cancellation before the first build");
                            } catch (CairoException e) {
                                Assert.assertTrue(e.getFlyweightMessage().toString(), e.isCancellation());
                                Assert.assertTrue(e.isInterruption());
                                Assert.assertEquals(false, cursorField(factory, "isMapBuilt"));
                            }
                        } finally {
                            breaker.clear();
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                        try (RecordCursor cursor = factory.getCursor(cancellable)) {
                            for (int pass = 0; pass < 2; pass++) {
                                final StringSink result = new StringSink();
                                CursorPrinter.println(cursor, factory.getMetadata(), result);
                                TestUtils.assertEquals(EXPECTED, result);
                                Assert.assertTrue((long) cursorField(factory, "inOrderEntryCount") > 0);
                                Assert.assertEquals(true, cursorField(factory, "isMapBuilt"));
                                cursor.toTop();
                            }
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                } finally {
                    cancellable.setMemoryTracker(null);
                }
            }
            assertQuery(SQL).noLeakCheck().timestamp("ts").expectSize().returns(EXPECTED);
        });
    }

    @Test
    public void testLateGapErrorThenReuse() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 1024);
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 128);
        setProperty(PropertyKey.CAIRO_SQL_MAP_MAX_RESIZES, 0);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE prices (ts TIMESTAMP, k SYMBOL, v LONG) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO prices VALUES
                    (0, 'a', 1), (28_000, 'a', 2), (28_000, 'late-key', 3), (29_000, 'late-key', 4)
                    """);
            bindVariableService.setLong("max", 4);
            final String sql = "SELECT ts, k, sum(v) v FROM prices WHERE v <= :max "
                    + "SAMPLE BY 1T FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'UTC'";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("Stream Group By");
            try (RecordCursorFactory factory = select(sql);
                 MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                         sqlExecutionContext.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)) {
                sqlExecutionContext.setMemoryTracker(tracker);
                try {
                    for (int iteration = 0; iteration < 2; iteration++) {
                        bindVariableService.setLong("max", 4);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            try {
                                cursor.hasNext();
                                Assert.fail("expected suffix-fill resize limit");
                            } catch (CairoException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), "limit of 0 resizes exceeded in FastMap");
                                Assert.assertTrue((long) cursorField(factory, "inOrderEntryCount") > 0);
                                Assert.assertEquals(false, cursorField(factory, "isMapFilled"));
                                Assert.assertEquals(false, cursorField(factory, "isMapBuilt"));
                            }
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                        bindVariableService.setLong("max", 1);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(1, cursor.getRecord().getLong(2));
                            Assert.assertFalse(cursor.hasNext());
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                } finally {
                    sqlExecutionContext.setMemoryTracker(null);
                }
            }
            assertQuery(sql).noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tk\tv\n1970-01-01T00:00:00.000000Z\ta\t1\n");
        });
    }

    @Test
    public void testLateGapFactoryReuseAfterGrowth() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 256);
        assertMemoryLeak(() -> {
            final String lateKey = "long-key-".repeat(64);
            execute("CREATE TABLE prices (ts TIMESTAMP, k VARCHAR, v LONG) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO prices VALUES
                    (0, 'a', 1), (32_000, 'a', 1), (32_000, '%s', 2), (33_000, '%s', 2)
                    """.formatted(lateKey, lateKey));
            bindVariableService.setLong("max", 2);
            final String sql = "SELECT ts, k, sum(v) v FROM prices WHERE v <= :max "
                    + "SAMPLE BY 1T FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'UTC'";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("Stream Group By");
            try (RecordCursorFactory factory = select(sql)) {
                for (int iteration = 0; iteration < 5; iteration++) {
                    final int max = switch (iteration) {
                        case 1 -> 1;
                        case 2 -> 0;
                        default -> 2;
                    };
                    if (iteration == 4) {
                        execute("INSERT INTO prices VALUES (34_000, '" + lateKey + "', 2)");
                    }
                    bindVariableService.setLong("max", max);
                    final int expectedCount = max == 0 ? 0 : max == 1 ? 33 : iteration == 4 ? 70 : 68;
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        cursor.toTop();
                        for (int pass = 0; pass < 2; pass++) {
                            int count = 0;
                            boolean hasFirstKey = false;
                            while (cursor.hasNext()) {
                                Assert.assertEquals((count / (max == 2 ? 2 : 1)) * 1000L, cursor.getRecord().getTimestamp(0));
                                final String key = cursor.getRecord().getVarcharA(1).toString();
                                Assert.assertTrue(key.equals("a") || key.equals(lateKey));
                                final boolean isFirstKey = key.equals("a");
                                if (max == 2 && count % 2 == 1) {
                                    Assert.assertNotEquals(hasFirstKey, isFirstKey);
                                }
                                hasFirstKey = isFirstKey;
                                Assert.assertEquals(isFirstKey ? 1 : 2, cursor.getRecord().getLong(2));
                                count++;
                            }
                            Assert.assertEquals(expectedCount, count);
                            Assert.assertEquals(expectedCount, cursor.size());
                            if (max == 2) {
                                Assert.assertTrue(((Map) cursorField(factory, "dataMap")).getHeapSize() > 256);
                            }
                            cursor.toTop();
                        }
                        Assert.assertEquals(expectedCount > 0, cursor.hasNext()); // Close a partially read result before reuse.
                    }
                }
            }
            assertQuery("SELECT count() n FROM (" + sql + ")").noLeakCheck().noRandomAccess().expectSize().returns("n\n70\n");
        });
    }

    @Test
    public void testLateGapInactiveParquet() throws Exception {
        assertMemoryLeak(() -> {
            createPrices("VARCHAR", "'late-key'");
            execute("INSERT INTO prices VALUES ('2024-01-02', 'active-partition', 100)");
            execute("ALTER TABLE prices CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            try (TableReader reader = getReader("prices")) {
                Assert.assertTrue(reader.getTxFile().isPartitionParquet(0));
                Assert.assertFalse(reader.getTxFile().isPartitionParquet(1));
            }
            final String sql = SQL.replace("FROM prices", "FROM prices WHERE ts < '2024-01-02'");
            assertQuery(sql).noLeakCheck().timestamp("ts").expectSize()
                    .withPlanContaining("Sample By", "Stream Group By")
                    .withPlanNotContaining("Sort", "Materialize").returns(EXPECTED);
        });
    }

    @Test
    public void testLateGapRewindPreservesCursorContracts() throws Exception {
        assertMemoryLeak(() -> {
            for (int type = 0; type < 2; type++) {
                createPrices(type == 0 ? "SYMBOL" : "VARCHAR", type == 0 ? "'late-key'" : "null");
                assertQuery(SQL).noLeakCheck().timestamp("ts").expectSize()
                        .withPlanContaining("Sample By", "Stream Group By")
                        .withPlanNotContaining("Sort", "Materialize")
                        .returns(type == 0 ? EXPECTED : EXPECTED.replace("late-key", ""));
                try (RecordCursorFactory factory = select(SQL);
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(factory.recordCursorSupportsRandomAccess());
                    cursor.toTop(); // Before the first build.
                    for (int consumed : new int[]{0, 1, 3, 8}) {
                        cursor.toTop();
                        for (int i = 0; i < consumed; i++) {
                            Assert.assertTrue(cursor.hasNext());
                        }
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                        Assert.assertEquals(8 - consumed, counter.get());
                        Assert.assertFalse(cursor.hasNext());
                        Assert.assertEquals(8, cursor.size());
                        cursor.toTop();
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(1, cursor.getRecord().getLong(2));
                    }
                    // A rewind replays the built map: it neither rebuilds nor moves the heap.
                    final long firstRowId = cursor.getRecord().getRowId();
                    for (int i = 0; i < 10; i++) {
                        cursor.toTop();
                        Assert.assertEquals(1, cursor.preComputedStateSize());
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(firstRowId, cursor.getRecord().getRowId());
                    }
                }
                execute("DROP TABLE prices");
            }
        });
    }

    @Test
    public void testTemporalJoinsKeepInterpolatedRhsRandomAccess() throws Exception {
        assertMemoryLeak(() -> {
            createPrices("VARCHAR", "'late-key'");
            execute("CREATE TABLE ticks (ts TIMESTAMP, k VARCHAR) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO ticks VALUES
                    ('2024-01-01', 'a'), ('2024-01-01T01:00', 'late-key'),
                    ('2024-01-01T02:30', 'a'), ('2024-01-01T03:00', 'late-key')
                    """);
            try (RecordCursorFactory rhs = select(SQL)) {
                Assert.assertTrue(rhs.recordCursorSupportsRandomAccess());
            }
            for (int join = 0; join < 2; join++) {
                final boolean isAsOf = join == 0;
                assertQuery("SELECT t.ts, t.k, s.v FROM ticks t " + (isAsOf ? "ASOF" : "LT")
                        + " JOIN (" + SQL + ") s ON (k)")
                        .noLeakCheck().timestamp("ts").noRandomAccess().expectSize()
                        .withPlanContaining(isAsOf ? "AsOf Join Light" : "Lt Join Light", "Stream Group By")
                        .withPlanNotContaining("Materialize")
                        .returns(isAsOf ? """
                                ts\tk\tv
                                2024-01-01T00:00:00.000000Z\ta\t1
                                2024-01-01T01:00:00.000000Z\tlate-key\t6
                                2024-01-01T02:30:00.000000Z\ta\t5
                                2024-01-01T03:00:00.000000Z\tlate-key\t14
                                """ : """
                                ts\tk\tv
                                2024-01-01T00:00:00.000000Z\ta\tnull
                                2024-01-01T01:00:00.000000Z\tlate-key\t2
                                2024-01-01T02:30:00.000000Z\ta\t5
                                2024-01-01T03:00:00.000000Z\tlate-key\t10
                                """);
            }
        });
    }

    private static void createPrices(String type, String lateKey) throws Exception {
        execute("CREATE TABLE prices (ts TIMESTAMP, k " + type + ", v LONG) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO prices VALUES
                ('2024-01-01', 'a', 1), ('2024-01-01T01:00', 'a', 3),
                ('2024-01-01T02:00', %s, 10), ('2024-01-01T03:00', %s, 14)
                """.formatted(lateKey, lateKey));
    }

    // Read-only diagnostics prove growth and failure phase; never replace cursor/map fields.
    private static Object cursorField(RecordCursorFactory factory, String name) throws Exception {
        while (!(factory instanceof SampleByInterpolateRecordCursorFactory)) {
            factory = factory.getBaseFactory();
            Assert.assertNotNull("missing interpolation factory", factory);
        }
        final Field cursorField = factory.getClass().getDeclaredField("cursor");
        cursorField.setAccessible(true);
        final Object cursor = cursorField.get(factory);
        final Field field = cursor.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(cursor);
    }
}
