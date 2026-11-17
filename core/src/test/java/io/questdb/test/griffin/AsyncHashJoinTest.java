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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.griffin.HashJoinGroupByKeys;
import io.questdb.griffin.HashJoinKeyLayout;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.join.HashOuterJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.griffin.engine.table.AsyncHashJoinSpec;
import io.questdb.griffin.engine.table.HashJoinInput;
import io.questdb.griffin.engine.table.HashJoinStageSpec;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import io.questdb.cairo.CursorPrinter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The parallel hash join over unfiltered page frame scans: its rows, their order, types and symbol
 * tables against the ordinary plan of the same SQL, for every key type the operator stages, every
 * payload type, NULL keys, duplicate keys, scan directions, data shapes, build modes and the cursor's
 * lifecycle, and the plans of the shapes it takes and of the shapes it leaves to the ordinary plan.
 * <p>
 * The INNER joins here scan an interval of the left input, whose cursor reports no size, so that
 * neither plan swaps the inputs; {@code AsyncHashJoinSwapTest} covers the swap.
 */
public class AsyncHashJoinTest extends AbstractCairoTest {
    // The key types that HashJoinGroupByKeys stages, each with five values: an INT and a SYMBOL key
    // read their keys straight off the frames, every other type stages its key. There is no LONG128
    // column type, and UUID covers a 128-bit key.
    private static final String[][] KEY_TYPES = {
            {"BOOLEAN", "true", "false", "true", "false", "true"},
            {"BYTE", "1", "2", "3", "-4", "127"},
            {"SHORT", "1", "2", "3", "-4", "32767"},
            {"CHAR", "'a'", "'b'", "'c'", "'d'", "'é'"},
            {"INT", "1", "2", "3", "-2147483647", "2147483647"},
            {"LONG", "1", "2", "3", "-9223372036854775807", "9223372036854775807"},
            {"DATE", "'2024-01-01'", "'1969-12-31'", "'2024-02-29'", "'1900-01-01'", "'2999-12-31'"},
            {"TIMESTAMP", "'2024-01-01T00:00:00.000001'", "'1969-12-31T23:59:59.999999'", "'2024-02-29'", "'1900-01-01'", "'2999-12-31'"},
            {"TIMESTAMP_NS", "'2024-01-01T00:00:00.000000001'", "'1969-12-31T23:59:59.999999999'", "'2024-02-29'", "'1900-01-01'", "'2200-12-31'"},
            {"FLOAT", "1.5", "-0.0", "0.0", "-3.25", "3.4e38"},
            {"DOUBLE", "1.5", "-0.0", "0.0", "-3.25", "1.7e308"},
            {"SYMBOL", "'a'", "''", "'é😀'", "'b'", "'c'"},
            {"STRING", "'a'", "''", "'é😀'", "'b'", "'c'"},
            {"VARCHAR", "'a'", "''", "'é😀'", "'b'", "'a long value that does not fit inline'"},
            {"LONG256", "'0x01'", "'0x02'", "'0x03'", "'0x04ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'", "'0x05'"},
            {"UUID", "'11111111-1111-1111-1111-111111111111'", "'11111111-1111-1111-1111-111111111112'", "'00000000-0000-0000-0000-000000000001'", "'ffffffff-ffff-ffff-ffff-ffffffffffff'", "'11111111-1111-1111-1111-111111111115'"},
            {"IPv4", "'1.1.1.1'", "'1.1.1.2'", "'0.0.0.1'", "'255.255.255.255'", "'10.0.0.1'"},
            {"GEOHASH(1c)", "'u'", "'v'", "'w'", "'x'", "'y'"},
            {"GEOHASH(3c)", "'u33'", "'u34'", "'u35'", "'u36'", "'u37'"},
            {"GEOHASH(5c)", "'u33dc'", "'u33dd'", "'u33de'", "'u33df'", "'u33dg'"},
            {"GEOHASH(8c)", "'u33dc0cp'", "'u33dc0cq'", "'u33dc0cr'", "'u33dc0cs'", "'u33dc0ct'"},
            {"DECIMAL(2,1)", "'1.1'", "'2.2'", "'-3.3'", "'9.9'", "'0.0'"},
            {"DECIMAL(4,2)", "'1.1'", "'2.2'", "'-3.3'", "'99.99'", "'0.0'"},
            {"DECIMAL(9,2)", "'1.1'", "'2.2'", "'-3.3'", "'9999999.99'", "'0.0'"},
            {"DECIMAL(18,3)", "'1.1'", "'2.2'", "'-3.3'", "'999999999999999.999'", "'0.0'"},
            {"DECIMAL(38,5)", "'1.1'", "'2.2'", "'-3.3'", "'123456789012345678901234567890.12345'", "'0.0'"},
            {"DECIMAL(76,10)", "'1.1'", "'2.2'", "'-3.3'", "'123456789012345678901234567890123456789012345678901234567890.1234567890'", "'0.0'"},
    };

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // read_parquet() reads files under the input root.
        inputRoot = root;
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testBackwardBuildScanSerialAndInRounds() throws Exception {
        assertMemoryLeak(() -> {
            createProbeAndBuild();
            // Pairs of consecutive rows share a key, so a frame of the build holds duplicates, whose
            // order in the frame the build's row order decides.
            execute("""
                    CREATE TABLE b AS (
                        SELECT (x / 2 % 5)::INT k, x w, ('b' || (x % 3))::SYMBOL s, timestamp_sequence('2023-12-31', 600_000_000) dts
                        FROM long_sequence(30)
                    ) TIMESTAMP(dts) PARTITION BY HOUR
                    """);
            // A sub-query's ORDER BY sorts its rows, so no SQL hashes a backward scan yet. The test plans
            // the join itself, over a build input that scans b backward, and compares it with the
            // ordinary join over the sorted sub-query: b's timestamps are unique, so the sort yields
            // the rows in the backward scan's order, and the serial join chains them in that order.
            final String expected = rows(ordinary(
                    "SELECT t.k, t.v, t.sym, t.ts, b.k, b.w, b.s, b.dts FROM t LEFT JOIN (SELECT * FROM b ORDER BY dts DESC) b ON t.k = b.k",
                    sqlExecutionContext
            ));
            sqlExecutionContext.changePageFrameSizes(2, 3);
            for (long minRows : new long[]{Long.MAX_VALUE, 0}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 1);
                try (
                        RecordCursorFactory probe = select("SELECT * FROM t");
                        RecordCursorFactory build = select("SELECT * FROM b ORDER BY dts DESC");
                        AsyncHashJoinRecordCursorFactory factory = newLeftJoin(probe, build)
                ) {
                    final String plan = plan(factory, sqlExecutionContext);
                    TestUtils.assertContains(plan, "Frame backward scan on: b");
                    Assert.assertEquals(expected, rows(result(factory, sqlExecutionContext)));
                    final HashJoinBuildStage stage = factory.getAtom().getStage(0);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(minRows == 0, stage.isBuiltInRounds());
                        Assert.assertEquals(minRows == 0, stage.getPartitionCount() > 1);
                    }
                }
            }
        });
    }

    @Test
    public void testBackwardProbeScan() throws Exception {
        assertMemoryLeak(() -> {
            createProbeAndBuild();
            sqlExecutionContext.changePageFrameSizes(2, 3);
            // ORDER BY the designated timestamp descending runs the left input backward, and the join
            // keeps that order, so the plan needs no sort.
            final String sql = "SELECT t.k, t.v, t.ts, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k ORDER BY t.ts DESC";
            final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
            TestUtils.assertContains(plan, "Frame backward scan on: t");
            Assert.assertFalse(plan, plan.contains("Sort"));
            assertParallelHashJoin(
                    "SELECT t.k, t.v, t.ts, d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01T01' ORDER BY t.ts DESC",
                    sqlExecutionContext
            );
        });
    }

    @Test
    public void testBinaryAndArrayKeysKeepOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x::INT k, rnd_bin(1, 4, 1) b, ARRAY[x::DOUBLE, 1.0] a, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(10)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d AS (SELECT x::INT k, rnd_bin(1, 4, 1) b, ARRAY[x::DOUBLE, 1.0] a FROM long_sequence(5))");
            assertOrdinaryPlan("SELECT t.k, d.k FROM t LEFT JOIN d ON t.b = d.b", sqlExecutionContext);
            assertOrdinaryPlan("SELECT t.k, d.k FROM t LEFT JOIN d ON t.a = d.a", sqlExecutionContext);
            assertOrdinaryPlan("SELECT t.k, d.k FROM t LEFT JOIN d ON t.k = d.k AND t.a = d.a", sqlExecutionContext);
        });
    }

    @Test
    public void testCancellationAtFrameBoundaryAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            // Every key of the build is unique, so no row leaves a chain for the consumer to walk, and
            // the cancellation surfaces where the next frame's task observes it.
            execute("CREATE TABLE t AS (SELECT (x % 5)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(20)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d AS (SELECT x::INT k, x w FROM long_sequence(4))");
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k";
            final String expected = ordinary(sql, sqlExecutionContext);
            sqlExecutionContext.changePageFrameSizes(1, 1);
            // One frame in flight at a time, so the frames after the first dispatch after the cancel.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
            final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine, 0);
            try (RecordCursorFactory factory = select(sql)) {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(1, find(factory).getAtom().getRing().getDispatchLimit());
                    breaker.cancel();
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.fail("expected cancellation");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCancellation());
                    TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                }
                breaker.reset();
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
            }
        });
    }

    @Test
    public void testConstructionFailureAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createProbeAndBuild();
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k";
            // The constructor fails once it created its frame sequences: it releases them, and the
            // compile releases the inputs and the filter contexts it made, as assertMemoryLeak checks.
            AsyncHashJoinRecordCursorFactory.setConstructorFailureForTesting(CairoException.nonCritical().put("injected construction failure"));
            try {
                assertExceptionNoLeakCheck(sql, 0, "injected construction failure");
            } finally {
                AsyncHashJoinRecordCursorFactory.setConstructorFailureForTesting(null);
            }
            assertParallelHashJoin(sql, sqlExecutionContext);
        });
    }

    @Test
    public void testInnerJoinEndsWithFramesWithoutMatches() throws Exception {
        assertMemoryLeak(() -> {
            // The probe's last frames hold keys that the build lacks, so the INNER join reads their
            // tasks, which carry no tuple, and ends.
            execute("CREATE TABLE t AS (SELECT (CASE WHEN x < 10 THEN x % 3 ELSE 100 + x END)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(30)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d AS (SELECT (x % 3)::INT k, x w FROM long_sequence(5))");
            sqlExecutionContext.changePageFrameSizes(2, 2);
            assertParallelHashJoin("SELECT t.k, t.v, d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01'", sqlExecutionContext);
            // So do frames without matches between frames with them.
            execute("INSERT INTO t SELECT (x % 3)::INT, x, timestamp_sequence('2024-01-01T01', 60_000_000) FROM long_sequence(4)");
            assertParallelHashJoin("SELECT t.k, t.v, d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01'", sqlExecutionContext);
        });
    }

    @Test
    public void testSharedStepperTemplate() throws Exception {
        assertMemoryLeak(() -> {
            // A JVM that cannot define hidden classes hands every stage the shared stepper template.
            AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(false);
            try {
                execute("CREATE TABLE t AS (SELECT (x % 7)::INT k, (x % 7)::LONG l, ('s' || (x % 7))::SYMBOL s, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(40)) TIMESTAMP(ts) PARTITION BY HOUR");
                // u holds one row per key, d repeats its keys, and m repeats some of them only.
                execute("CREATE TABLE u AS (SELECT (x % 5)::INT k, (x % 5)::LONG l, ('s' || (x % 5))::SYMBOL s, x w FROM long_sequence(5))");
                execute("CREATE TABLE d AS (SELECT (x % 5)::INT k, (x % 5)::LONG l, ('s' || (x % 5))::SYMBOL s, x w FROM long_sequence(15))");
                execute("CREATE TABLE m AS (SELECT (x % 5)::INT k, (x % 5)::LONG l, ('s' || (x % 5))::SYMBOL s, x w FROM long_sequence(8))");
                sqlExecutionContext.changePageFrameSizes(3, 3);
                for (String build : new String[]{"u", "d", "m"}) {
                    for (String key : new String[]{"k", "s", "l"}) {
                        final String on = " ON t." + key + " = b." + key;
                        assertParallelHashJoin("SELECT t.v, b.w FROM t LEFT JOIN " + build + " b" + on, sqlExecutionContext);
                        assertParallelHashJoin("SELECT t.v, b.w FROM t JOIN " + build + " b" + on + " WHERE t.ts IN '2024-01-01'", sqlExecutionContext);
                        assertParallelHashJoin("SELECT t.v, b.w FROM t LEFT JOIN " + build + " b" + on + " ORDER BY t.ts DESC", sqlExecutionContext);
                    }
                }
                try (RecordCursorFactory factory = select("SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k")) {
                    Assert.assertFalse(find(factory).getStepperClass(0).isHidden());
                }
            } finally {
                AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(true);
            }
        });
    }

    @Test
    public void testColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d (k INT, dts TIMESTAMP) TIMESTAMP(dts) PARTITION BY HOUR");
            execute("INSERT INTO t SELECT (x % 5)::INT, timestamp_sequence('2024-01-01', 600_000_000) FROM long_sequence(12)");
            execute("INSERT INTO d SELECT (x % 4)::INT, timestamp_sequence('2024-01-01', 900_000_000) FROM long_sequence(8)");
            // Columns added after the first rows have column tops in the first partitions, on both sides,
            // and a key column added late reads NULL there.
            execute("ALTER TABLE t ADD COLUMN tv VARCHAR");
            execute("ALTER TABLE t ADD COLUMN tk INT");
            execute("ALTER TABLE d ADD COLUMN dv VARCHAR");
            execute("ALTER TABLE d ADD COLUMN dl LONG");
            execute("ALTER TABLE d ADD COLUMN dk INT");
            execute("INSERT INTO t SELECT (x % 5)::INT, timestamp_sequence('2024-01-01T03', 600_000_000), 'tv' || x, (x % 3)::INT FROM long_sequence(12)");
            execute("INSERT INTO d SELECT (x % 4)::INT, timestamp_sequence('2024-01-01T03', 900_000_000), 'dv' || x, x, (x % 3)::INT FROM long_sequence(8)");
            sqlExecutionContext.changePageFrameSizes(2, 4);
            assertParallelHashJoin("SELECT * FROM t LEFT JOIN d ON t.k = d.k", sqlExecutionContext);
            assertParallelHashJoin("SELECT * FROM t LEFT JOIN d ON t.tk = d.dk", sqlExecutionContext);
            assertParallelHashJoin("SELECT * FROM t LEFT JOIN d ON t.k = d.dk AND t.tk = d.k", sqlExecutionContext);
            assertParallelHashJoin("SELECT * FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01'", sqlExecutionContext);
        });
    }

    @Test
    public void testDeferredFanOut() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x::INT k, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(5)) TIMESTAMP(ts) PARTITION BY HOUR");
            // Key 1 has one row, so the worker hands its locator over; key 2 has three, whose chain the
            // consumer walks, newest first; keys 3 to 5 miss.
            execute("CREATE TABLE d (k INT, s STRING)");
            execute("INSERT INTO d VALUES (1, 'one'), (2, 'two-a'), (2, 'two-b'), (2, 'two-c')");
            final String sql = "SELECT t.k, d.s FROM t LEFT JOIN d ON t.k = d.k";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            k\ts
                            1\tone
                            2\ttwo-c
                            2\ttwo-b
                            2\ttwo-a
                            3\t
                            4\t
                            5\t
                            """);
            assertParallelHashJoin(sql, sqlExecutionContext);
            assertQuery("SELECT t.k, d.s FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            k\ts
                            1\tone
                            2\ttwo-c
                            2\ttwo-b
                            2\ttwo-a
                            """);
        });
    }

    @Test
    public void testDuplicateBuildKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT (x % 7)::INT k, x v, ('s' || (x % 7))::SYMBOL sk, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(60)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d AS (SELECT (x % 5)::INT k, x w, ('s' || (x % 5))::SYMBOL sk, x::LONG l FROM long_sequence(40))");
            for (int frameRows : new int[]{1, 3, 1_000_000}) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                assertParallelHashJoin("SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k", sqlExecutionContext);
                assertParallelHashJoin("SELECT t.v, d.w FROM t LEFT JOIN d ON t.sk = d.sk", sqlExecutionContext);
                assertParallelHashJoin("SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND t.sk = d.sk", sqlExecutionContext);
                assertParallelHashJoin("SELECT t.v, d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01'", sqlExecutionContext);
            }
        });
    }

    @Test
    public void testEmptyBuild() throws Exception {
        assertMemoryLeak(() -> {
            createProbeAndBuild();
            execute("TRUNCATE TABLE d");
            // An INNER join over an empty build returns no row and dispatches no probe task.
            final String inner = "SELECT t.k, d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01'";
            assertParallelHashJoin(inner, sqlExecutionContext);
            try (RecordCursorFactory factory = select(inner)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse(cursor.hasNext());
                    Assert.assertEquals(0, join.getProbeFrameCount());
                    Assert.assertEquals(0, join.getAtom().getStage(0).getFrozenBuild().getRowCount());
                }
            }
            // A LEFT join reads every probe row with NULL build columns.
            final String left = "SELECT t.k, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k";
            assertParallelHashJoin(left, sqlExecutionContext);
            try (RecordCursorFactory factory = select(left)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(join.getProbeFrameCount() > 0);
                }
            }
        });
    }

    @Test
    public void testEmptyProbe() throws Exception {
        assertMemoryLeak(() -> {
            createProbeAndBuild();
            execute("TRUNCATE TABLE t");
            assertParallelHashJoin("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k", sqlExecutionContext);
            assertParallelHashJoin("SELECT t.k, d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01'", sqlExecutionContext);
            execute("TRUNCATE TABLE d");
            assertParallelHashJoin("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k", sqlExecutionContext);
        });
    }

    @Test
    public void testKeyPairs() throws Exception {
        assertMemoryLeak(() -> {
            // d inserts its symbols in another order, so equal SYMBOL text has different keys in t and d.
            execute("""
                    CREATE TABLE t AS (
                        SELECT
                            CASE WHEN x % 6 = 0 THEN NULL ELSE ('k' || (x % 5)) END::SYMBOL sym,
                            CASE WHEN x % 6 = 0 THEN NULL ELSE ('k' || (x % 5)) END str,
                            CASE WHEN x % 6 = 0 THEN NULL ELSE ('k' || (x % 5)) END::VARCHAR vch,
                            (x % 4)::INT a,
                            (x % 3)::INT b,
                            CASE WHEN x % 6 = 0 THEN NULL ELSE (x % 5)::TIMESTAMP END us,
                            CASE WHEN x % 6 = 0 THEN NULL ELSE (x % 5)::TIMESTAMP_NS END ns,
                            timestamp_sequence('2024-01-01', 60_000_000) ts
                        FROM long_sequence(40)
                    ) TIMESTAMP(ts) PARTITION BY HOUR
                    """);
            execute("""
                    CREATE TABLE d AS (
                        SELECT
                            CASE WHEN x % 7 = 0 THEN NULL ELSE ('k' || (8 - x % 8)) END::SYMBOL sym,
                            CASE WHEN x % 7 = 0 THEN NULL ELSE ('k' || (8 - x % 8)) END str,
                            CASE WHEN x % 7 = 0 THEN NULL ELSE ('k' || (8 - x % 8)) END::VARCHAR vch,
                            (x % 4)::INT a,
                            CASE WHEN x % 7 = 0 THEN NULL ELSE ((8 - x % 8) * 1000)::TIMESTAMP_NS END ns,
                            CASE WHEN x % 7 = 0 THEN NULL ELSE (8 - x % 8)::TIMESTAMP END us,
                            x w
                        FROM long_sequence(30)
                    )
                    """);
            try (TableReader t = getReader("t"); TableReader d = getReader("d")) {
                Assert.assertNotEquals(t.getSymbolMapReader(0).keyOf("k1"), d.getSymbolMapReader(0).keyOf("k1"));
            }
            sqlExecutionContext.changePageFrameSizes(3, 5);
            final String[] conditions = {
                    "t.sym = d.sym",
                    "t.sym = d.str",
                    "t.sym = d.vch",
                    "t.str = d.sym",
                    "t.vch = d.sym",
                    "t.str = d.vch",
                    "t.vch = d.str",
                    "t.str = d.str",
                    "t.us = d.ns",
                    "t.ns = d.us",
                    "t.a = d.a AND t.sym = d.sym",
                    "t.sym = d.sym AND t.str = d.str AND t.a = d.a",
            };
            for (String condition : conditions) {
                assertParallelHashJoin("SELECT t.*, d.w FROM t LEFT JOIN d ON " + condition, sqlExecutionContext);
                assertParallelHashJoin("SELECT t.*, d.w FROM t JOIN d ON " + condition + " WHERE t.ts IN '2024-01-01'", sqlExecutionContext);
            }
            // A SYMBOL pair compares the probe's key translated into the build's dictionary.
            TestUtils.assertContains(assertParallelHashJoin("SELECT t.*, d.w FROM t LEFT JOIN d ON t.sym = d.sym", sqlExecutionContext), "symbolKeyJoin: true");
            // A self-join over one SYMBOL column.
            assertParallelHashJoin("SELECT t1.a, t2.a FROM t t1 LEFT JOIN t t2 ON t1.sym = t2.sym", sqlExecutionContext);
            // A build column that two keys read: the optimiser turns ON t.a = d.a AND t.b = d.a into a
            // filter of t, so the build's projection names the column twice.
            for (String projection : new String[]{"a a1, a a2, w", "str s1, str s2, w", "sym s1, sym s2, w"}) {
                final String build = "(SELECT " + projection + " FROM d) d2";
                final String on = projection.startsWith("a") ? "t.a = d2.a1 AND t.b = d2.a2" : "t.sym = d2.s1 AND t.vch = d2.s2";
                assertParallelHashJoin("SELECT t.*, d2.* FROM t LEFT JOIN " + build + " ON " + on, sqlExecutionContext);
                assertParallelHashJoin("SELECT t.*, d2.* FROM t JOIN " + build + " ON " + on + " WHERE t.ts IN '2024-01-01'", sqlExecutionContext);
            }
        });
    }

    @Test
    public void testKeyTypes() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(3, 5);
            for (String[] type : KEY_TYPES) {
                execute("CREATE TABLE t (k " + type[0] + ", v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR");
                execute("CREATE TABLE d (k " + type[0] + ", w LONG)");
                // The probe holds every value and NULL; the build holds three of them, the first three
                // times, and NULL twice, so its keys repeat, NULL matches NULL and two values miss.
                execute("INSERT INTO t SELECT " + keyValue(type, "x % 6") + ", x, timestamp_sequence('2024-01-01', 600_000_000) FROM long_sequence(40)");
                execute("INSERT INTO d SELECT " + keyValue(type, "x % 4") + ", x FROM long_sequence(9)");
                final String plan = assertParallelHashJoin("SELECT * FROM t LEFT JOIN d ON t.k = d.k", sqlExecutionContext);
                assertParallelHashJoin("SELECT * FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01T03'", sqlExecutionContext);
                Assert.assertEquals(type[0], type[0].equals("SYMBOL"), plan.contains("symbolKeyJoin: true"));
                execute("DROP TABLE t");
                execute("DROP TABLE d");
            }
        });
    }

    @Test
    public void testNullKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT
                            CASE WHEN x % 3 = 0 THEN NULL ELSE (x % 4)::INT END k,
                            NULL::INT nk,
                            CASE WHEN x % 3 = 0 THEN NULL ELSE ('s' || (x % 4)) END::SYMBOL sk,
                            CASE WHEN x % 3 = 0 THEN NULL ELSE (x % 4)::LONG END lk,
                            x v,
                            timestamp_sequence('2024-01-01', 60_000_000) ts
                        FROM long_sequence(20)
                    ) TIMESTAMP(ts) PARTITION BY HOUR
                    """);
            execute("""
                    CREATE TABLE d AS (
                        SELECT
                            CASE WHEN x % 2 = 0 THEN NULL ELSE (x % 4)::INT END k,
                            NULL::INT nk,
                            CASE WHEN x % 2 = 0 THEN NULL ELSE ('s' || (x % 4)) END::SYMBOL sk,
                            CASE WHEN x % 2 = 0 THEN NULL ELSE (x % 4)::LONG END lk,
                            x w
                        FROM long_sequence(8)
                    )
                    """);
            for (String condition : new String[]{"t.k = d.k", "t.sk = d.sk", "t.lk = d.lk", "t.nk = d.nk", "t.k = d.nk", "t.nk = d.k", "t.k = d.k AND t.sk = d.sk"}) {
                assertParallelHashJoin("SELECT t.v, t.k, d.w, d.k, d.sk FROM t LEFT JOIN d ON " + condition, sqlExecutionContext);
                assertParallelHashJoin("SELECT t.v, t.k, d.w, d.k, d.sk FROM t JOIN d ON " + condition + " WHERE t.ts IN '2024-01-01'", sqlExecutionContext);
            }
            // NULL keys match NULL keys, as in the ordinary join.
            assertQuery("SELECT t.v, d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01' AND t.v < 4")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v\tw
                            1\t5
                            1\t1
                            3\t8
                            3\t6
                            3\t4
                            3\t2
                            """);
        });
    }

    @Test
    public void testPayloadTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT
                            (x % 5)::INT k,
                            CASE WHEN x % 4 = 0 THEN NULL ELSE ('t' || (x % 3)) END::SYMBOL tsym,
                            x v,
                            timestamp_sequence('2024-01-01', 600_000_000) ts
                        FROM long_sequence(30)
                    ) TIMESTAMP(ts) PARTITION BY HOUR
                    """);
            final StringBuilder columns = new StringBuilder("(x % 4)::INT k");
            for (int i = 0; i < KEY_TYPES.length; i++) {
                columns.append(", ").append(keyValue(KEY_TYPES[i], "x % 6")).append(" c").append(i);
            }
            columns.append("""
                    , CASE x % 5 WHEN 0 THEN NULL WHEN 1 THEN '' WHEN 2 THEN 'é😀' ELSE 'str' || x END str
                    , CASE x % 5 WHEN 0 THEN NULL WHEN 1 THEN '' WHEN 2 THEN 'é😀' ELSE 'a varchar that does not fit inline ' || x END::VARCHAR vch
                    , CASE x % 4 WHEN 0 THEN NULL ELSE rnd_bin(1, 8, 0) END bin
                    , CASE x % 4 WHEN 0 THEN NULL WHEN 1 THEN ARRAY[]::DOUBLE[] ELSE ARRAY[x::DOUBLE, -x, 1.5] END arr
                    , CASE x % 4 WHEN 0 THEN NULL ELSE ARRAY[[x::DOUBLE, 2.0], [3.0, 4.0]] END arr2
                    """);
            execute("CREATE TABLE d AS (SELECT " + columns + " FROM long_sequence(12))");
            sqlExecutionContext.changePageFrameSizes(2, 3);
            for (long copyMaxSize : new long[]{0, Long.MAX_VALUE}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, copyMaxSize);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0");
                assertParallelHashJoin("SELECT * FROM t LEFT JOIN d ON t.k = d.k", sqlExecutionContext);
                assertParallelHashJoin("SELECT * FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01T01'", sqlExecutionContext);
                // The build's SYMBOL payload, and the probe's, read their own dictionaries.
                assertParallelHashJoin("SELECT t.tsym, d.c11, t.k FROM t LEFT JOIN d ON t.k = d.k", sqlExecutionContext);
            }
        });
    }

    @Test
    public void testPayloadCopy() throws Exception {
        assertMemoryLeak(() -> {
            // Fixed-size payload columns alone, which the copy holds.
            execute("CREATE TABLE t AS (SELECT (x % 50)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(200)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d AS (SELECT (x % 40)::INT k, x w, ('s' || (x % 3))::SYMBOL s, x::DOUBLE dbl FROM long_sequence(80))");
            final String sql = "SELECT t.v, d.w, d.s, d.dbl FROM t LEFT JOIN d ON t.k = d.k";
            sqlExecutionContext.changePageFrameSizes(16, 16);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 8);
            for (long copyMaxSize : new long[]{0, Long.MAX_VALUE}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, copyMaxSize);
                for (long minRows : new long[]{Long.MAX_VALUE, 0}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                    final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
                    Assert.assertEquals(plan, copyMaxSize > 0, plan.contains("buildPayload: copied when the probe is large enough"));
                    try (RecordCursorFactory factory = select(sql)) {
                        final HashJoinBuildStage stage = find(factory).getAtom().getStage(0);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(copyMaxSize > 0, stage.isPayloadCopied());
                            Assert.assertEquals(minRows == 0, stage.isBuiltInRounds());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSingleRowAndTinyFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT 1 k, 10L v, '2024-01-01'::TIMESTAMP ts FROM long_sequence(1)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d AS (SELECT 1 k, 'one' s FROM long_sequence(1))");
            assertParallelHashJoin("SELECT * FROM t LEFT JOIN d ON t.k = d.k", sqlExecutionContext);
            assertParallelHashJoin("SELECT * FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024'", sqlExecutionContext);
            execute("INSERT INTO t SELECT (x % 4)::INT, x, timestamp_sequence('2024-01-02', 1_000_000) FROM long_sequence(30)");
            execute("INSERT INTO d SELECT (x % 3)::INT, 'v' || x FROM long_sequence(7)");
            for (int frameRows = 1; frameRows < 4; frameRows++) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                assertParallelHashJoin("SELECT * FROM t LEFT JOIN d ON t.k = d.k", sqlExecutionContext);
                assertParallelHashJoin("SELECT * FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024'", sqlExecutionContext);
            }
        });
    }

    @Test
    public void testBuildMemoryLimitAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createProbeAndBuild();
            final String sql = "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k";
            final String expected = ordinary(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                // A limit of four bytes admits the open cursor, which charges nothing, and fails the
                // build that the first read starts.
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("expected the build to breach the memory limit");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            }
        });
    }

    @Test
    public void testCancellationAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createProbeAndBuild();
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k";
            final String expected = ordinary(sql, sqlExecutionContext);
            sqlExecutionContext.changePageFrameSizes(1, 1);
            final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine, 0);
            try (RecordCursorFactory factory = select(sql)) {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                // Cancelled before the build: the build checks the breaker first.
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    breaker.cancel();
                    cursor.hasNext();
                    Assert.fail("expected cancellation");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCancellation());
                }
                breaker.reset();
                // Cancelled mid-stream: the next frame's task observes it.
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    breaker.cancel();
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.fail("expected cancellation");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCancellation());
                }
                breaker.reset();
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
            }
        });
    }

    @Test
    public void testDownstreamAsOfJoin() throws Exception {
        assertMemoryLeak(() -> {
            createProbeAndBuild();
            execute("CREATE TABLE q AS (SELECT ('s' || (x % 4))::SYMBOL sym, x qv, timestamp_sequence('2024-01-01', 450_000_000) qts FROM long_sequence(40)) TIMESTAMP(qts) PARTITION BY DAY");
            sqlExecutionContext.changePageFrameSizes(2, 3);
            // The join keeps the left input's designated timestamp and its order, which the ASOF join
            // on top reads.
            for (String sql : new String[]{
                    "SELECT t.ts, t.k, d.w, q.qv FROM t LEFT JOIN d ON t.k = d.k ASOF JOIN q ON (sym)",
                    "SELECT t.ts, t.k, d.w, q.qv FROM t JOIN d ON t.k = d.k ASOF JOIN q WHERE t.ts IN '2024-01-01T02'",
                    "SELECT t.ts, t.k, d.w, q.qv FROM t LEFT JOIN d ON t.k = d.k LT JOIN q ON (sym)",
            }) {
                final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
                Assert.assertTrue(plan, plan.contains("AsOf Join") || plan.contains("Lt Join"));
            }
        });
    }

    @Test
    public void testFactoryProperties() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT (x % 5)::INT tk, x tv, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(20)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d AS (SELECT (x % 4)::INT dk, x dw FROM long_sequence(6))");
            final String sql = "SELECT * FROM t LEFT JOIN d ON t.tk = d.dk";
            assertParallelHashJoin(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // The metadata is the ordinary join's: the left input's columns, then the right one's,
                // and the left input's designated timestamp.
                final RecordMetadata metadata = join.getMetadata();
                sqlExecutionContext.setParallelHashJoinEnabled(false);
                try (RecordCursorFactory ordinary = select(sql)) {
                    Assert.assertNull(find(ordinary));
                    RecordCursorFactory serialJoin = ordinary;
                    while (!(serialJoin instanceof HashOuterJoinLightRecordCursorFactory)) {
                        serialJoin = serialJoin.getBaseFactory();
                    }
                    final RecordMetadata expected = serialJoin.getMetadata();
                    Assert.assertEquals(5, expected.getColumnCount());
                    Assert.assertEquals(expected.getColumnCount(), metadata.getColumnCount());
                    for (int i = 0, n = expected.getColumnCount(); i < n; i++) {
                        Assert.assertEquals(expected.getColumnName(i), metadata.getColumnName(i));
                        Assert.assertEquals(expected.getColumnType(i), metadata.getColumnType(i));
                    }
                    Assert.assertEquals(2, expected.getTimestampIndex());
                    Assert.assertEquals(2, metadata.getTimestampIndex());
                } finally {
                    sqlExecutionContext.setParallelHashJoinEnabled(true);
                }
                Assert.assertFalse(join.recordCursorSupportsRandomAccess());
                Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_FORWARD, join.getScanDirection());
                Assert.assertFalse(join.followedOrderByAdvice());
                Assert.assertFalse(join.usesExternalDataSource());
                Assert.assertNull(join.getBaseFactory());
                Assert.assertNull(join.getTableToken());
                Assert.assertFalse(join.supportsUpdateRowId(engine.verifyTableName("t")));
                try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(-1, cursor.size());
                    Assert.assertEquals(0, cursor.preComputedStateSize());
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(1, cursor.preComputedStateSize());
                    Assert.assertEquals(-1, cursor.size());
                    Assert.assertThrows(UnsupportedOperationException.class, cursor::getRecordB);
                }
            }
            // A left input that scans backward, as ORDER BY its timestamp descending asks, followed the
            // order advice, and the join answers as it does.
            try (RecordCursorFactory factory = select("SELECT * FROM (SELECT * FROM t ORDER BY ts DESC) t LEFT JOIN d ON t.tk = d.dk")) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_BACKWARD, join.getScanDirection());
                Assert.assertTrue(join.followedOrderByAdvice());
            }
        });
    }

    @Test
    public void testIntervalScans() throws Exception {
        assertMemoryLeak(() -> {
            createProbeAndBuild();
            sqlExecutionContext.changePageFrameSizes(2, 3);
            for (String sql : new String[]{
                    // The probe scans an interval.
                    "SELECT t.k, t.v, b.w FROM t JOIN d b ON t.k = b.k WHERE t.ts IN '2024-01-01T01'",
                    "SELECT t.k, t.v, b.w FROM t LEFT JOIN d b ON t.k = b.k WHERE t.ts IN '2024-01-01T01'",
                    // The build scans an interval.
                    "SELECT t.k, t.v, b.w FROM t JOIN (SELECT * FROM d WHERE dts IN '2023-12-31T01') b ON t.k = b.k",
                    "SELECT t.k, t.v, b.w FROM t LEFT JOIN (SELECT * FROM d WHERE dts IN '2023-12-31T01') b ON t.k = b.k",
                    // Both do.
                    "SELECT t.k, t.v, b.w FROM t JOIN (SELECT * FROM d WHERE dts IN '2023-12-31') b ON t.k = b.k WHERE t.ts IN '2024-01-01T02'",
                    // An interval that holds no row.
                    "SELECT t.k, t.v, b.w FROM t LEFT JOIN d b ON t.k = b.k WHERE t.ts IN '2030'",
                    "SELECT t.k, t.v, b.w FROM t JOIN (SELECT * FROM d WHERE dts IN '2030') b ON t.k = b.k",
            }) {
                final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
                TestUtils.assertContains(plan, "Interval forward scan");
            }
        });
    }

    @Test
    public void testKeyPresizingPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            // Serial builds, so that the key table is the one table of the build.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, Long.MAX_VALUE);
            execute("CREATE TABLE t AS (SELECT x::INT k, timestamp_sequence('2024-01-01', 1_000_000) ts FROM long_sequence(200)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d (k INT, w LONG)");
            final String sql = "SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k";
            try (RecordCursorFactory factory = select(sql)) {
                final HashJoinBuildStage stage = find(factory).getAtom().getStage(0);
                // The key table starts at 64 slots and doubles once a new key would fill half of it,
                // so 100 keys grow it twice, to 256 slots, 200 keys three times and 300 keys four times.
                // A build whose table holds no more rows than the probe's presizes the table for its
                // rows instead, and never grows it.
                execute("INSERT INTO d SELECT x::INT, x FROM long_sequence(100)");
                assertPresizing(factory, stage, true, 0);
                execute("INSERT INTO d SELECT (100 + x)::INT, x FROM long_sequence(100)");
                assertPresizing(factory, stage, true, 0);
                // The build outgrows the probe between two executions of one factory: the next one lets
                // its key table grow, and needs no new compile.
                execute("INSERT INTO d SELECT (200 + x)::INT, x FROM long_sequence(100)");
                assertPresizing(factory, stage, false, 4);
            }
            // A staged key decides the same way; its map's growth goes uncounted.
            execute("CREATE TABLE ld AS (SELECT x l FROM long_sequence(300))");
            execute("CREATE TABLE lt AS (SELECT x l, timestamp_sequence('2024-01-01', 1_000_000) ts FROM long_sequence(200)) TIMESTAMP(ts) PARTITION BY DAY");
            try (RecordCursorFactory factory = select("SELECT lt.l, ld.l FROM lt LEFT JOIN ld ON lt.l = ld.l")) {
                assertPresizing(factory, find(factory).getAtom().getStage(0), false, 0);
            }
            assertParallelHashJoin(sql, sqlExecutionContext);
        });
    }

    @Test
    public void testLimitZeroAndExplainBuildNothing() throws Exception {
        assertMemoryLeak(() -> {
            createProbeAndBuild();
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k";
            // A limit of four bytes fails any build, since a build allocates its key table.
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4L);
            try (RecordCursorFactory factory = select(sql + " LIMIT 0")) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse(cursor.hasNext());
                    Assert.assertNull(join.getAtom().getStage(0).getFrozenBuild());
                    Assert.assertEquals(0, join.getAtom().getStage(0).getBuildFrameCount());
                }
            }
            try (RecordCursorFactory factory = select("EXPLAIN " + sql)) {
                final StringSink sink = new StringSink();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), sink);
                }
                TestUtils.assertContains(sink, "Async Hash Join");
            }
            // Whereas reading a row builds, which the limit fails.
            try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                cursor.hasNext();
                Assert.fail("expected the build to breach the memory limit");
            } catch (CairoException e) {
                Assert.assertTrue(e.isOutOfMemory());
            }
        });
    }

    @Test
    public void testLimits() throws Exception {
        assertMemoryLeak(() -> {
            createProbeAndBuild();
            sqlExecutionContext.changePageFrameSizes(2, 3);
            for (String limit : new String[]{" LIMIT 3", " LIMIT 0", " LIMIT -4", " LIMIT 2, 6", " LIMIT -6, -2", " LIMIT 1000", " LIMIT -1000"}) {
                assertParallelHashJoin("SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k" + limit, sqlExecutionContext);
                assertParallelHashJoin("SELECT t.k, t.v, d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01'" + limit, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testProbeMemoryLimitAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            // One large probe frame over a two-row build: the frame's tuples take 50_000 * 2 * 8 bytes,
            // far more than the build.
            execute("CREATE TABLE t AS (SELECT (x % 3)::INT k, timestamp_sequence('2024-01-01', 1_000_000) ts FROM long_sequence(50_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d AS (SELECT x::INT k, x w FROM long_sequence(2))");
            sqlExecutionContext.changePageFrameSizes(50_000, 50_000);
            final String sql = "SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k";
            final String expected = ordinary(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 200_000L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("expected the probe task to breach the memory limit");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            }
        });
    }

    @Test
    public void testProbeScanFailureClosesBuildScan() throws Exception {
        assertMemoryLeak(() -> {
            createProbeAndBuild();
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k";
            try (RecordCursorFactory factory = select(sql)) {
                // The build scan opens first and waits for the stage to adopt it; the probe scan of a
                // dropped table fails to open, and the open build scan closes with the cursor.
                execute("DROP TABLE t");
                try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("expected the probe scan to fail");
                } catch (CairoException | SqlException e) {
                    TestUtils.assertContains(e.getMessage(), "t");
                }
                Assert.assertEquals(0, engine.getBusyReaderCount());
            }
            // The build table still works.
            assertQuery("SELECT count() FROM d").noLeakCheck().noRandomAccess().expectSize().returns("count\n14\n");
        });
    }

    @Test
    public void testReadParquetBuild() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelReadParquetEnabled(true);
            execute("CREATE TABLE t AS (SELECT x::INT k, timestamp_sequence('2024-01-01', 1_000_000) ts FROM long_sequence(200)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d AS (SELECT (x % 300)::INT k, x w, ('s' || (x % 3))::SYMBOL s FROM long_sequence(300))");
            writeParquet("d", "d.parquet");
            for (String sql : new String[]{
                    "SELECT t.k, p.w, p.s FROM t LEFT JOIN read_parquet('d.parquet') p ON t.k = p.k",
                    "SELECT t.k, p.w, p.s FROM t JOIN read_parquet('d.parquet') p ON t.k = p.k WHERE t.ts IN '2024-01-01'",
                    "SELECT p.k, t.k FROM read_parquet('d.parquet') p LEFT JOIN t ON t.k = p.k",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
                // The join reads externally, as its ordinary plan does.
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    Assert.assertTrue(join.usesExternalDataSource());
                    Assert.assertTrue(factory.usesExternalDataSource());
                }
            }
            // A build over read_parquet() has no table reader to size its key table by, so it grows.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, Long.MAX_VALUE);
            try (RecordCursorFactory factory = select("SELECT t.k, p.w FROM t LEFT JOIN read_parquet('d.parquet') p ON t.k = p.k")) {
                assertPresizing(factory, find(factory).getAtom().getStage(0), false, 4);
            }
        });
    }

    @Test
    public void testStepperClassPerFactory() throws Exception {
        assertMemoryLeak(() -> {
            createProbeAndBuild();
            final String sql = "SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k";
            // The factories run their copies from the first task, so they define them as they compile.
            useSteppers(true);
            try (RecordCursorFactory first = select(sql); RecordCursorFactory second = select(sql)) {
                final Class<?> firstClass = find(first).getStepperClass(0);
                final Class<?> secondClass = find(second).getStepperClass(0);
                // Each factory's stage probes through a hidden copy of the stepper template of its own.
                Assert.assertNotEquals(firstClass, secondClass);
                Assert.assertTrue(firstClass.isHidden());
                Assert.assertTrue(secondClass.isHidden());
            } finally {
                restoreSteppers();
            }
        });
    }

    @Test
    public void testToTopMidStream() throws Exception {
        assertMemoryLeak(() -> {
            createProbeAndBuild();
            sqlExecutionContext.changePageFrameSizes(2, 3);
            for (String sql : new String[]{
                    "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k",
                    "SELECT t.k, t.v, d.w, d.s FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01'",
            }) {
                sqlExecutionContext.setParallelHashJoinEnabled(false);
                final String expected;
                try (RecordCursorFactory factory = select(sql)) {
                    Assert.assertNull(find(factory));
                    expected = printRows(factory);
                }
                sqlExecutionContext.setParallelHashJoinEnabled(true);
                try (RecordCursorFactory factory = select(sql)) {
                    Assert.assertNotNull(find(factory));
                    final RecordMetadata metadata = factory.getMetadata();
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        // toTop() before the first read, after a few rows, inside a chain of matches and
                        // after the last row each read the whole result again.
                        for (int stop : new int[]{0, 1, 5, 9, Integer.MAX_VALUE}) {
                            final StringSink sink = new StringSink();
                            int rows = 0;
                            while (rows++ < stop && cursor.hasNext()) {
                                CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
                            }
                            TestUtils.assertContains(expected, sink);
                            Assert.assertTrue(expected.startsWith(sink.toString()));
                            cursor.toTop();
                        }
                        final StringSink sink = new StringSink();
                        while (cursor.hasNext()) {
                            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
                        }
                        Assert.assertEquals(expected, sink.toString());
                    }
                }
            }
        });
    }

    private static void assertPresizing(RecordCursorFactory factory, HashJoinBuildStage stage, boolean isPresized, int growCount) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(isPresized, stage.isKeyCapacityPresized());
            Assert.assertEquals(growCount, stage.getKeyTableGrowCount());
        }
    }

    private static void createProbeAndBuild() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT
                        CASE WHEN x % 9 = 0 THEN NULL ELSE (x % 7)::INT END k,
                        x v,
                        ('s' || (x % 4))::SYMBOL sym,
                        timestamp_sequence('2024-01-01', 600_000_000) ts
                    FROM long_sequence(30)
                ) TIMESTAMP(ts) PARTITION BY HOUR
                """);
        execute("""
                CREATE TABLE d AS (
                    SELECT
                        CASE WHEN x % 6 = 0 THEN NULL ELSE (x % 5)::INT END k,
                        x w,
                        ('d' || (x % 3))::SYMBOL s,
                        timestamp_sequence('2023-12-31', 600_000_000) dts
                    FROM long_sequence(14)
                ) TIMESTAMP(dts) PARTITION BY HOUR
                """);
    }

    // A LEFT join of two compiled scans on their first columns, planned by hand. The factory takes the
    // scans the two QueryProgress factories wrap, and the wrappers close them again, which is a no-op.
    private static AsyncHashJoinRecordCursorFactory newLeftJoin(RecordCursorFactory probeQuery, RecordCursorFactory buildQuery) throws SqlException {
        final RecordCursorFactory probe = probeQuery.getBaseFactory();
        final RecordCursorFactory build = buildQuery.getBaseFactory();
        final HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
        Assert.assertTrue(keys.add(0, probe.getMetadata().getColumnType(0), 0, build.getMetadata().getColumnType(0)));
        final IntList probeColumns = identity(probe.getMetadata().getColumnCount());
        final IntList buildColumns = identity(build.getMetadata().getColumnCount());
        final HashJoinKeyLayout keyLayout = new HashJoinKeyLayout(configuration, new BytecodeAssembler(), keys,
                "t", probe.getMetadata(), probeColumns, "b", build.getMetadata(), buildColumns);
        final JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, probeColumns.size() + buildColumns.size());
        metadata.copyColumnMetadataFrom("t", probe.getMetadata());
        metadata.copyColumnMetadataFrom("b", build.getMetadata());
        metadata.setTimestampIndex(probe.getMetadata().getTimestampIndex());
        final AsyncHashJoinSpec spec = new AsyncHashJoinSpec(
                new HashJoinInput(probe, probeColumns, new AsyncFilterContext(configuration, null, null, null, null, null, null, 1, 0, 0, 0)),
                new HashJoinStageSpec(
                        new HashJoinInput(build, buildColumns, new AsyncFilterContext(configuration, null, null, null, null, null, null, 1, 0, 0, 0)),
                        keyLayout,
                        true,
                        "b.k=t.k",
                        null,
                        null,
                        null
                ),
                metadata,
                probe.followedOrderByAdvice(),
                probe.getScanDirection()
        );
        return new AsyncHashJoinRecordCursorFactory(
                engine,
                configuration,
                engine.getMessageBus(),
                spec,
                () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                1
        );
    }

    private static IntList identity(int columnCount) {
        final IntList columns = new IntList();
        for (int i = 0; i < columnCount; i++) {
            columns.add(i);
        }
        return columns;
    }

    private static String printRows(RecordCursorFactory factory) throws SqlException {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            while (cursor.hasNext()) {
                CursorPrinter.println(cursor.getRecord(), factory.getMetadata(), sink, true);
            }
        }
        return sink.toString();
    }

    // A result's rows, without its metadata lines.
    private static String rows(String result) {
        return result.substring(result.indexOf('\n', result.indexOf('\n') + 1) + 1);
    }

    // A key column's value of a SQL type for each remainder of the given expression: NULL for zero, the
    // type's values otherwise.
    private static String keyValue(String[] type, String remainder) {
        final StringBuilder sb = new StringBuilder("CAST(CASE ").append(remainder).append(" WHEN 0 THEN NULL");
        for (int i = 1; i < type.length; i++) {
            sb.append(" WHEN ").append(i).append(" THEN ").append(type[i]);
        }
        return sb.append(" END AS ").append(type[0]).append(')').toString();
    }

    // Writes the table's first partition to a Parquet file under the input root, for read_parquet().
    private static void writeParquet(String table, String file) {
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader(table)
        ) {
            path.of(root).concat(file);
            engine.getConfiguration().getFilesFacade().remove(path.$());
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encode(descriptor, path);
            Assert.assertTrue(Files.exists(path.$()));
        }
    }
}
