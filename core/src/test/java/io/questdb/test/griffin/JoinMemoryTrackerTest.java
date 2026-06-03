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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.join.AsOfJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopFullJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.SpliceJoinLightRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the
 * tracker-aware secondary join cursors in {@link io.questdb.griffin.engine.join}.
 * Light outer joins grow a join-key {@link io.questdb.cairo.map.Map} plus a
 * {@link io.questdb.griffin.engine.join.LongChain} of every slave rowid; the
 * keyed merge joins (ASOF / LT / SPLICE) grow a per-key map that never evicts
 * without TOLERANCE; the nested-loop full join grows a matched-rowid map. All
 * scale with join-key cardinality (or slave row count) and bind the active
 * workload's MemoryTracker in the cursor's of() before the first reopen(), so a
 * runaway join breaches the limit at the offending allocation.
 * <p>
 * The per-query limit is set to 512 KiB per test in {@link #setUp()} (so it
 * survives the per-test override reset; the provider reads it live on each
 * tracker acquisition), enough to clear the LongChain's fixed 128 KiB initial
 * page (plus the small join maps) for the small-input / leak-loop cases while a
 * high-cardinality join breaches.
 */
public class JoinMemoryTrackerTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512 * 1024L);
    }

    @Test
    public void testAsOfJoinLightFailsOnLargeInput() throws Exception {
        // The asof_linear hint forces the AsOfJoinLight fallback (skipping the
        // Fast/Indexed paths); its join-key map never evicts without TOLERANCE.
        // The result is iterated (not count(*), which would short-circuit via
        // calculateSize without building the map) so the map actually grows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k, (x * 1000000L)::timestamp ts FROM long_sequence(100_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, (x * 1000000L)::timestamp ts FROM long_sequence(100_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            final String sql = "SELECT /*+ asof_linear(m s) */ m.k FROM m ASOF JOIN s ON k";
            assertUsesFactory(sql, AsOfJoinLightRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testHashOuterJoinLightFailsOnLargeInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k FROM long_sequence(100_000))");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, x AS v FROM long_sequence(100_000))");
            drainWalQueue();
            final String sql = "SELECT m.k, s.v FROM m LEFT JOIN s ON k";
            assertUsesFactory(sql, HashOuterJoinLightRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testHashOuterJoinLightRepeatedCursorRunsReleaseAllocations() throws Exception {
        // The leak loop cycles both the join-key map and the LongChain slave
        // chain; the assertMemoryLeak around it is the load-bearing check that
        // the new LongChain tracker binding keeps malloc/free symmetric.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k FROM long_sequence(50))");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, x AS v FROM long_sequence(50))");
            drainWalQueue();
            final String sql = "SELECT m.k, s.v FROM m LEFT JOIN s ON k";
            assertUsesFactory(sql, HashOuterJoinLightRecordCursorFactory.class);
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                for (int i = 0; i < 20; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(50, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testHashOuterJoinLightSucceedsOnSmallInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k FROM long_sequence(20))");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, x AS v FROM long_sequence(20))");
            drainWalQueue();
            assertSql("count\n20\n", "SELECT count(*) FROM (SELECT m.k, s.v FROM m LEFT JOIN s ON k)");
        });
    }

    @Test
    public void testNestedLoopFullJoinFailsOnLargeInput() throws Exception {
        // A one-row master keeps the nested loop cheap while the non-equi FULL
        // OUTER condition matches nearly every slave row, growing the matched-id
        // map past the limit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT 1L AS k FROM long_sequence(1))");
            execute("CREATE TABLE s AS (SELECT x AS k FROM long_sequence(100_000))");
            drainWalQueue();
            final String sql = "SELECT count(*) FROM m FULL OUTER JOIN s ON m.k <> s.k";
            assertUsesFactory(sql, NestedLoopFullJoinRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testSpliceJoinFailsOnLargeInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT cast(x AS SYMBOL) k, (x * 1000000L)::timestamp ts FROM long_sequence(100_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s AS (SELECT cast(x AS SYMBOL) k, (x * 1000000L)::timestamp ts FROM long_sequence(100_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            final String sql = "SELECT count(*) FROM m SPLICE JOIN s ON k";
            assertUsesFactory(sql, SpliceJoinLightRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    private void assertQueryBreaches(String sql) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                    // drain until breach
                }
                Assert.fail("expected per-query memory breach");
            } catch (CairoException e) {
                Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
            }
        }
    }

    private void assertUsesFactory(String sql, Class<?> factoryClass) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            RecordCursorFactory cur = factory;
            while (cur != null) {
                if (factoryClass.isInstance(cur)) {
                    return;
                }
                RecordCursorFactory next = cur.getBaseFactory();
                if (next == cur) {
                    break;
                }
                cur = next;
            }
            Assert.fail("expected " + factoryClass.getSimpleName() + " in base chain of " + factory.getClass().getSimpleName());
        }
    }
}
