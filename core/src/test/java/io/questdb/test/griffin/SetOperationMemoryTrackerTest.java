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
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.union.ExceptAllRecordCursorFactory;
import io.questdb.griffin.engine.union.ExceptRecordCursorFactory;
import io.questdb.griffin.engine.union.IntersectAllRecordCursorFactory;
import io.questdb.griffin.engine.union.IntersectRecordCursorFactory;
import io.questdb.griffin.engine.union.UnionRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the
 * tracker-aware set-operation cursors in {@link io.questdb.griffin.engine.union}.
 * UNION, INTERSECT, EXCEPT, INTERSECT ALL and EXCEPT ALL hash whole rows into
 * one (or two) {@link io.questdb.cairo.map.OrderedMap}(s) that grow with
 * distinct row count. The cursors construct those maps with deferred allocation
 * (openOnInit=false) and bind the active workload's MemoryTracker in of() before
 * the first reopen(), so the malloc/free pairs that follow charge the per-query
 * counter symmetrically and a runaway set operation breaches the limit at the
 * offending allocation site.
 * <p>
 * UNION ALL streams without a hash map and stays out of scope: a large input
 * runs to completion under the same limit.
 * <p>
 * The per-query limit is applied per test in {@link #setUp()} via
 * {@code setProperty} so it survives the per-test override reset; the provider
 * reads it live on each tracker acquisition.
 */
public class SetOperationMemoryTrackerTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        // The two-map operators (INTERSECT, EXCEPT) reopen two 32 KiB small maps
        // up front, so the limit must clear ~64 KiB for the small-input cases
        // while still tripping once a map grows past it on a large input.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 128 * 1024L);
    }

    @Test
    public void testExceptAllFailsOnLargeInput() throws Exception {
        assertMemoryLeak(() -> {
            createLargeTables();
            final String sql = "SELECT k FROM a EXCEPT ALL SELECT k FROM b";
            assertUsesFactory(sql, ExceptAllRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testExceptAllOpenFailureReleasesAllocations() throws Exception {
        // EXCEPT ALL open-failure check: an inflated map makes of() breach on the
        // first reopen(), and the failed open must free the cursor so reopen() runs
        // (and breaches) again on every reuse.
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 50_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT cast(x AS varchar) k FROM long_sequence(20))");
            execute("CREATE TABLE b AS (SELECT cast(x AS varchar) k FROM long_sequence(10))");
            drainWalQueue();
            assertOpenFailureReleasesAllocations("SELECT k FROM a EXCEPT ALL SELECT k FROM b", ExceptAllRecordCursorFactory.class);
        });
    }

    @Test
    public void testExceptFailsOnLargeInput() throws Exception {
        assertMemoryLeak(() -> {
            createLargeTables();
            final String sql = "SELECT k FROM a EXCEPT SELECT k FROM b";
            assertUsesFactory(sql, ExceptRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testExceptOpenFailureReleasesAllocations() throws Exception {
        // Two-map EXCEPT variant of the open-failure check: the failed open must free
        // the cursor so reopen() runs (and breaches) again on every reuse.
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 50_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT cast(x AS varchar) k FROM long_sequence(20))");
            execute("CREATE TABLE b AS (SELECT cast(x AS varchar) k FROM long_sequence(10))");
            drainWalQueue();
            assertOpenFailureReleasesAllocations("SELECT k FROM a EXCEPT SELECT k FROM b", ExceptRecordCursorFactory.class);
        });
    }

    @Test
    public void testIntersectAllFailsOnLargeInput() throws Exception {
        assertMemoryLeak(() -> {
            createLargeTables();
            final String sql = "SELECT k FROM a INTERSECT ALL SELECT k FROM b";
            assertUsesFactory(sql, IntersectAllRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testIntersectAllOpenFailureReleasesAllocations() throws Exception {
        // INTERSECT ALL open-failure check: an inflated map makes of() breach on the
        // first reopen(), and the failed open must free the cursor so reopen() runs
        // (and breaches) again on every reuse.
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 50_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT cast(x AS varchar) k FROM long_sequence(20))");
            execute("CREATE TABLE b AS (SELECT cast(x AS varchar) k FROM long_sequence(10))");
            drainWalQueue();
            assertOpenFailureReleasesAllocations("SELECT k FROM a INTERSECT ALL SELECT k FROM b", IntersectAllRecordCursorFactory.class);
        });
    }

    @Test
    public void testIntersectFailsOnLargeInput() throws Exception {
        assertMemoryLeak(() -> {
            createLargeTables();
            final String sql = "SELECT k FROM a INTERSECT SELECT k FROM b";
            assertUsesFactory(sql, IntersectRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testIntersectOpenFailureReleasesAllocations() throws Exception {
        // Inflate the maps above the limit so INTERSECT's of() breaches on the first
        // reopen() with isOpen set; reusing the factory catches a failed open that
        // leaves isOpen stuck (a later open would skip reopen() and stop breaching).
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 50_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT cast(x AS varchar) k FROM long_sequence(20))");
            execute("CREATE TABLE b AS (SELECT cast(x AS varchar) k FROM long_sequence(10))");
            drainWalQueue();
            assertOpenFailureReleasesAllocations("SELECT k FROM a INTERSECT SELECT k FROM b", IntersectRecordCursorFactory.class);
        });
    }

    @Test
    public void testIntersectSucceedsOnSmallInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT cast(x AS varchar) k FROM long_sequence(20))");
            execute("CREATE TABLE b AS (SELECT cast(x AS varchar) k FROM long_sequence(10))");
            drainWalQueue();
            assertQuery("SELECT count(*) FROM (SELECT k FROM a INTERSECT SELECT k FROM b)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n10\n");
        });
    }

    @Test
    public void testIntersectWithCastsFailsOnLargeInput() throws Exception {
        // Mismatched column types unify to a common type and wrap one arm in cast
        // functions, exercising IntersectCastRecordCursor's lazy map pair.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT x::int k FROM long_sequence(100_000))");
            execute("CREATE TABLE b AS (SELECT x::long k FROM long_sequence(100_000))");
            drainWalQueue();
            final String sql = "SELECT k FROM a INTERSECT SELECT k FROM b";
            assertUsesFactory(sql, IntersectRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testRepeatedCursorRunsReleaseAllocations() throws Exception {
        // Repeat the same UNION many times to verify close/reopen cycles through
        // the set-operation map release every byte they allocate. The
        // assertMemoryLeak around the loop is the load-bearing check; a
        // malloc/free asymmetry shows up as a residual native allocation count.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT cast(x AS varchar) k FROM long_sequence(50))");
            execute("CREATE TABLE b AS (SELECT cast(x + 25 AS varchar) k FROM long_sequence(50))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile("SELECT k FROM a UNION SELECT k FROM b", sqlExecutionContext).getRecordCursorFactory()) {
                for (int i = 0; i < 20; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(75, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testUnionAllSucceedsOnLargeInput() throws Exception {
        // UNION ALL streams with no hash map, so it is out of scope for the
        // per-query limit: a large input runs to completion.
        assertMemoryLeak(() -> {
            createLargeTables();
            assertQuery("SELECT count(*) FROM (SELECT k FROM a UNION ALL SELECT k FROM b)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n200000\n");
        });
    }

    @Test
    public void testUnionFailsOnLargeInput() throws Exception {
        assertMemoryLeak(() -> {
            createLargeTables();
            final String sql = "SELECT k FROM a UNION SELECT k FROM b";
            assertUsesFactory(sql, UnionRecordCursorFactory.class);
            assertQueryBreaches(sql);
        });
    }

    @Test
    public void testUnionOpenFailureReleasesAllocations() throws Exception {
        // Single-map UNION variant of the open-failure check: the failed open must
        // free the cursor so reopen() runs (and breaches) again on every reuse.
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 50_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT cast(x AS varchar) k FROM long_sequence(20))");
            execute("CREATE TABLE b AS (SELECT cast(x + 10 AS varchar) k FROM long_sequence(10))");
            drainWalQueue();
            assertOpenFailureReleasesAllocations("SELECT k FROM a UNION SELECT k FROM b", UnionRecordCursorFactory.class);
        });
    }

    @Test
    public void testUnionSucceedsOnSmallInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT cast(x AS varchar) k FROM long_sequence(10))");
            execute("CREATE TABLE b AS (SELECT cast(x + 5 AS varchar) k FROM long_sequence(10))");
            drainWalQueue();
            assertQuery("SELECT count(*) FROM (SELECT k FROM a UNION SELECT k FROM b)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n15\n");
        });
    }

    private void assertOpenFailureReleasesAllocations(String sql, Class<?> factoryClass) throws Exception {
        // Reuse one factory across opens, each expected to breach during the set-op
        // cursor's of(). Without freeing the cursor on the failed open, isOpen stays
        // set, a later open skips reopen() and stops breaching, tripping Assert.fail.
        assertUsesFactory(sql, factoryClass);
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            for (int i = 0; i < 5; i++) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("expected a per-query memory breach during cursor open at iteration " + i);
                } catch (CairoException e) {
                    Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }
            }
        }
    }

    private void assertQueryBreaches(String sql) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = cq.getRecordCursorFactory();
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
        // Pin which set-operation factory the query routes to, so a later routing change
        // cannot silently redirect the breach to a different structure and pass for the
        // wrong reason.
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            if (!isFactoryInChain(factory, factoryClass)) {
                Assert.fail("expected " + factoryClass.getSimpleName() + " in base chain of " + factory.getClass().getSimpleName());
            }
        }
    }

    private void createLargeTables() throws Exception {
        execute("CREATE TABLE a AS (SELECT cast(x AS varchar) k FROM long_sequence(100_000))");
        execute("CREATE TABLE b AS (SELECT cast(x AS varchar) k FROM long_sequence(100_000))");
        drainWalQueue();
    }

    private boolean isFactoryInChain(RecordCursorFactory factory, Class<?> factoryClass) {
        RecordCursorFactory cur = factory;
        while (cur != null) {
            if (factoryClass.isInstance(cur)) {
                return true;
            }
            RecordCursorFactory next = cur.getBaseFactory();
            if (next == cur) {
                break;
            }
            cur = next;
        }
        return false;
    }
}
