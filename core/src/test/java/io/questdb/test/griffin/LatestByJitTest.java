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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.jit.JitUtil;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class LatestByJitTest extends AbstractCairoTest {

    @Test
    public void testBatchesAcrossManySmallFrames() throws Exception {
        assertJitBatchesAfterSmallFrames(16, 32);
    }

    @Test
    public void testBatchesAcrossMultipleSmallFrames() throws Exception {
        assertJitBatchesAfterSmallFrames(4, 32);
    }

    @Test
    public void testBatchesAfterSmallNewestFrame() throws Exception {
        assertJitBatchesAfterSmallFrames(1, 32);
    }

    @Test
    public void testBatchesForLargeFirstParquetFrame() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
            setProperty(PropertyKey.CAIRO_PAGE_FRAME_ROWID_LIST_CAPACITY, 262_144);
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4096);
            execute("""
                    CREATE TABLE jit_large_parquet AS (
                      SELECT (x % 2)::STRING::SYMBOL s, 'a'::SYMBOL t, 'b'::SYMBOL u, x v, timestamp_sequence(0, 1) ts
                      FROM long_sequence(4096)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("INSERT INTO jit_large_parquet VALUES ('0', 'a', 'b', 0, '1970-01-02')");
            execute("ALTER TABLE jit_large_parquet CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            try {
                // Exclude the active native partition so the first frame is a Parquet row group.
                // Its newest two rows complete every query, even when maxRows is only one or two.
                for (int maxRows : new int[]{1, 2}) {
                    sqlExecutionContext.changePageFrameSizes(1, maxRows);
                    for (String interval : new String[]{"ts < '1970-01-02'",
                            "ts >= '1970-01-01T00:00:00.002048' AND ts < '1970-01-02'"}) {
                        String prefix = "SELECT v FROM jit_large_parquet WHERE v > 0 AND " + interval;
                        ObjList<String> queries = new ObjList<>();
                        queries.add(prefix + " LATEST ON ts PARTITION BY s");
                        queries.add(prefix + " LATEST ON ts PARTITION BY s,t");
                        queries.add(prefix + " LATEST ON ts PARTITION BY s,t,u");
                        queries.add(prefix + " AND s IN ('0','1') LATEST ON ts PARTITION BY s");
                        queries.add(prefix + " AND s = '1' LATEST ON ts PARTITION BY s");
                        queries.add(prefix + " AND s != '0' LATEST ON ts PARTITION BY s");
                        queries.add(prefix + " AND s IN (SELECT '1'::STRING) LATEST ON ts PARTITION BY s");
                        for (int mode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_ENABLED}) {
                            sqlExecutionContext.setJitMode(mode);
                            for (int queryIndex = 0; queryIndex < queries.size(); queryIndex++) {
                                try (RecordCursorFactory factory = select(queries.getQuick(queryIndex))) {
                                    Assert.assertEquals(mode != SqlJitMode.JIT_MODE_DISABLED, factory.usesCompiledFilter());
                                    for (int attempt = 0; attempt < 3; attempt++) {
                                        assertFactory(factory).withContext(sqlExecutionContext).sizeMayVary()
                                                .returns(queryIndex < 4 ? "v\n4095\n4096\n" : "v\n4095\n");
                                        Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                                    }
                                }
                            }
                        }
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testBatchesWhenNewestFrameCannotCoverTargets() throws Exception {
        assertJitBatchesAfterSmallFrames(1, 1);
    }

    @Test
    public void testBindVarMemoryFollowsFactoryLifetime() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_JIT_BIND_VARS_MEMORY_PAGE_SIZE, 256 * 1024);
            execute("""
                    CREATE TABLE jit_open AS (
                      SELECT (x % 3)::STRING::SYMBOL s, (x % 5)::STRING::SYMBOL t, x v, (x * 1_000_000)::TIMESTAMP ts
                      FROM long_sequence(100)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            final String[] clauses = {"", " AND s='1'", " AND s=:key", " AND s IN ('1','2')"};
            final long[] mins = {0, 99, 0};
            final String[] keys = {"1", "2", "2"};
            final String[][] expected = {
                    {"v\n98\n99\n100\n", "v\n100\n", "v\n98\n99\n100\n"},
                    {"v\n100\n", "v\n100\n", "v\n100\n"},
                    {"v\n100\n", "v\n", "v\n98\n"},
                    {"v\n98\n100\n", "v\n100\n", "v\n98\n100\n"},
            };
            for (int clause = 0; clause < clauses.length; clause++) {
                final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
                bindVariableService.setStr("key", keys[0]);
                bindVariableService.setLong("min", mins[0]);
                try (RecordCursorFactory factory = select("SELECT v FROM jit_open WHERE v > :min" + clauses[clause]
                        + " LATEST ON ts PARTITION BY s")) {
                    Assert.assertTrue(factory.usesCompiledFilter());
                    for (int i = 0; i < mins.length; i++) {
                        bindVariableService.setLong("min", mins[i]);
                        bindVariableService.setStr("key", keys[i]);
                        assertFactory(factory).withContext(sqlExecutionContext).sizeMayVary().returns(expected[clause][i]);
                        Assert.assertTrue(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT) >= before + 256 * 1024L);
                        Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                    }
                }
                Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
            }
        });
    }

    @Test
    public void testBuffersAllocatedOnCursorOpen() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(1, 32);
            try {
                setProperty(PropertyKey.CAIRO_PAGE_FRAME_COLUMN_LIST_CAPACITY, 32_768);
                execute("""
                        CREATE TABLE jit_lazy AS (
                          SELECT (x % 3)::STRING::SYMBOL s, x v, (x * 1_000_000)::TIMESTAMP ts
                          FROM long_sequence(100)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                for (String predicate : new String[]{"v > 0", "v < 50"}) {
                    final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
                    try (RecordCursorFactory factory = select("SELECT v FROM jit_lazy WHERE " + predicate
                            + " LATEST ON ts PARTITION BY s")) {
                        Assert.assertTrue(factory.usesCompiledFilter());
                        Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD));
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertNotNull(cursor);
                            Assert.assertTrue(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD) > before);
                        }
                        final long allocated = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
                        Assert.assertTrue(allocated > before);
                        for (int attempt = 0; attempt < 3; attempt++) {
                            assertFactory(factory).withContext(sqlExecutionContext).sizeMayVary()
                                    .returns(predicate.equals("v > 0") ? "v\n98\n99\n100\n" : "v\n47\n48\n49\n");
                            Assert.assertEquals(allocated, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD));
                            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                        }
                    }
                    Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD));
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testBuffersRetainedAcrossBreachAndCancel() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(1, 512);
            final SqlExecutionCircuitBreaker original = sqlExecutionContext.getCircuitBreaker();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
            ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
            try {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4 * 1024 * 1024L);
                execute("""
                        CREATE TABLE jit_memory AS (
                          SELECT (x % 11)::STRING::SYMBOL s, (x % 13)::STRING::SYMBOL t, (x % 17)::STRING::SYMBOL u,
                            x v, (x * 1_000_000)::TIMESTAMP ts
                          FROM long_sequence(10_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
                bindVariableService.setLong("min", 100);
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                for (String keys : new String[]{"s", "s,t", "s,t,u"}) {
                    try (RecordCursorFactory factory = select("SELECT v FROM jit_memory WHERE v > :min AND v < 9000 LATEST ON ts PARTITION BY " + keys)) {
                        Assert.assertTrue(factory.usesCompiledFilter());
                        for (int iteration = 0; iteration < 6; iteration++) {
                            final long retained = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
                            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, iteration == 3 ? 1 : 4 * 1024 * 1024L);
                            MemoryTracker tracker = null;
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                tracker = sqlExecutionContext.getMemoryTracker();
                                Assert.assertNotNull(tracker);
                                if (iteration == 3) {
                                    Assert.fail("expected a per-query memory breach during cursor open");
                                }
                                if (iteration == 4) {
                                    breaker.cancel();
                                }
                                if (iteration % 3 == 1) {
                                    Assert.assertTrue(cursor.hasNext());
                                    Assert.assertNotEquals("expected query cancellation", 4, iteration);
                                } else if (iteration % 3 == 2) {
                                    while (cursor.hasNext()) {
                                    }
                                }
                                Assert.assertTrue(tracker.getUsed() > 0);
                            } catch (CairoException e) {
                                if (iteration == 3) {
                                    Assert.assertTrue(e.isOutOfMemory());
                                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                                } else if (iteration == 4) {
                                    Assert.assertTrue(e.isCancellation());
                                } else {
                                    throw e;
                                }
                            } finally {
                                breaker.reset();
                            }
                            if (tracker != null) {
                                Assert.assertEquals(0, tracker.getUsed());
                            }
                            if (iteration == 3 || iteration == 4) {
                                Assert.assertEquals(retained, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD));
                            }
                            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                        }
                    }
                }
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(original);
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testIndexedSubQueryUsesJavaFilter() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
            execute("CREATE TABLE t (s SYMBOL INDEX, f SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE k (s SYMBOL)");
            execute("""
                    INSERT INTO t VALUES
                    ('a', 'nope', 10, 0), ('b', 'nope', 20, 1), (NULL, 'nope', 30, 2),
                    ('z', 'nope', 40, 3), ('a', NULL, NULL, 4)
                    """);
            execute("INSERT INTO k VALUES ('a'), ('b'), (NULL), ('z')");
            bindVariableService.setInt("min", 0);
            final ObjList<String> predicates = new ObjList<>();
            predicates.add("s NOT IN ('z') AND s IN (SELECT s FROM k) AND v > :min");
            predicates.add("s != 'z' AND s IN (SELECT s FROM k) AND v > :min");
            predicates.add("s NOT IN ('z') AND s IN (SELECT s FROM k) AND f = 'nope'");
            predicates.add("s NOT IN ('z', NULL) AND s IN (SELECT s FROM k) AND v > :min");
            for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(mode);
                for (int i = 0; i < predicates.size(); i++) {
                    try (RecordCursorFactory factory = select("SELECT s, v FROM t WHERE " + predicates.getQuick(i)
                            + " LATEST ON ts PARTITION BY s")) {
                        Assert.assertFalse(factory.usesCompiledFilter());
                        for (int attempt = 0; attempt < 3; attempt++) {
                            assertFactory(factory).withContext(sqlExecutionContext).sizeMayVary()
                                    .returns(i == 3 ? "s\tv\na\t10\nb\t20\n" : "s\tv\na\t10\nb\t20\n\t30\n");
                            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testResultBufferBoundedAcrossLargeFrames() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(65_537, 65_537);
            try {
                setProperty(PropertyKey.CAIRO_PAGE_FRAME_ROWID_LIST_CAPACITY, 131_072);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
                execute("""
                        CREATE TABLE jit_bound AS (
                          SELECT (x % 2)::STRING::SYMBOL s, (x % 2)::INT k, x v, timestamp_sequence(0, 1) ts
                          FROM long_sequence(65_537)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                for (String keys : new String[]{"k", "s"}) {
                    long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
                    try (RecordCursorFactory factory = select("SELECT v FROM jit_bound WHERE v > 0"
                            + " LATEST ON ts PARTITION BY " + keys)) {
                        Assert.assertTrue(factory.usesCompiledFilter());
                        for (int attempt = 0; attempt < 3; attempt++) {
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                long count = 0;
                                long sum = 0;
                                while (cursor.hasNext()) {
                                    count++;
                                    sum += cursor.getRecord().getLong(0);
                                }
                                Assert.assertEquals(2, count);
                                Assert.assertEquals(131_073, sum);
                                long allocated = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD) - before;
                                long pointerBytes = 2L * configuration.getPageFrameReduceColumnListCapacity() * Long.BYTES;
                                Assert.assertTrue("native batch buffers: " + allocated,
                                        allocated > pointerBytes && allocated <= 2048L * Long.BYTES + pointerBytes);
                                cursor.toTop();
                                Assert.assertTrue(cursor.hasNext());
                            }
                            Assert.assertTrue(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD) > before);
                            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                        }
                    }
                    Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD));
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    private void assertJitBatchesAfterSmallFrames(int smallPartitionCount, int smallPartitionRows) throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(1, 512);
            try {
                setProperty(PropertyKey.CAIRO_PAGE_FRAME_ROWID_LIST_CAPACITY, 32_768);
                execute("""
                        CREATE TABLE jit_small_frames AS (
                          SELECT (x % 2)::STRING::SYMBOL s, 'a'::SYMBOL t, 'b'::SYMBOL u, x v, timestamp_sequence(0, 1) ts
                          FROM long_sequence(4096)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
                for (int i = 0; i < smallPartitionCount; i++) {
                    execute("INSERT INTO jit_small_frames SELECT '0', 'a', 'b', x + "
                            + (4096 + i * smallPartitionRows) + ", timestamp_sequence("
                            + (i + 1) * 86_400_000_000L + ", 1) FROM long_sequence(" + smallPartitionRows + ")");
                }
                final String expected = "v\n4095\n" + (4096 + smallPartitionCount * smallPartitionRows) + "\n";
                ObjList<String> queries = new ObjList<>();
                queries.add("SELECT v FROM jit_small_frames WHERE v > 0 LATEST ON ts PARTITION BY s");
                queries.add("SELECT v FROM jit_small_frames WHERE v > 0 LATEST ON ts PARTITION BY s,t");
                queries.add("SELECT v FROM jit_small_frames WHERE v > 0 LATEST ON ts PARTITION BY s,t,u");
                queries.add("SELECT v FROM jit_small_frames WHERE v > 0 AND s IN ('0','1') LATEST ON ts PARTITION BY s");
                queries.add("SELECT v FROM jit_small_frames WHERE v > 0 AND s = '1' LATEST ON ts PARTITION BY s");
                queries.add("SELECT v FROM jit_small_frames WHERE v > 0 AND s != '0' LATEST ON ts PARTITION BY s");
                queries.add("SELECT v FROM jit_small_frames WHERE v > 0 AND s IN (SELECT '1'::STRING) LATEST ON ts PARTITION BY s");
                for (int mode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_ENABLED}) {
                    sqlExecutionContext.setJitMode(mode);
                    for (int queryIndex = 0; queryIndex < queries.size(); queryIndex++) {
                        try (RecordCursorFactory factory = select(queries.getQuick(queryIndex))) {
                            Assert.assertEquals(mode != SqlJitMode.JIT_MODE_DISABLED, factory.usesCompiledFilter());
                            for (int attempt = 0; attempt < 3; attempt++) {
                                assertFactory(factory).withContext(sqlExecutionContext).sizeMayVary()
                                        .returns(queryIndex < 4 ? expected : "v\n4095\n");
                                Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                            }
                        }
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }
}
