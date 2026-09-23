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

package io.questdb.test.griffin.engine.table;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.SymbolTranslatingRecord;
import io.questdb.mp.WorkerPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Verifies that {@link SymbolTranslatingRecord} keeps its translation caches in native
 * memory only while a cursor is open: the owning cursor releases them on close, while the
 * factory stays alive (as it does in the query cache), and reopens them on the next execution.
 * <p>
 * The capped run limits each cache to fewer entries than a cache at initial capacity takes
 * before its first rehash, so the caches must stay small, while the uncached translations
 * must still produce the same results.
 * <p>
 * The caches are the only execution-time user of {@link MemoryTag#NATIVE_JOIN_MAP}, so the
 * tag's counter measures them precisely.
 */
@RunWith(Parameterized.class)
public class SymbolTranslatingRecordTest extends AbstractCairoTest {
    // A cache at initial capacity rehashes on its 16th entry.
    private static final int CAPPED_CACHE_CAPACITY = 10;
    private static final int MASTER_SYMBOL_COUNT = 2_000;
    private static final int SLAVE_SYMBOL_COUNT = 1_000;
    private final boolean isCacheCapped;

    public SymbolTranslatingRecordTest(boolean isCacheCapped) {
        this.isCacheCapped = isCacheCapped;
    }

    @Parameterized.Parameters(name = "capped={0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        if (isCacheCapped) {
            setProperty(PropertyKey.CAIRO_SQL_JOIN_SYMBOL_TRANSLATION_CACHE_CAPACITY, CAPPED_CACHE_CAPACITY);
        }
    }

    @Test
    public void testAsyncHorizonJoinReleasesCachesOnCursorClose() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTables(engine, sqlExecutionContext);
                        assertHorizonJoins(compiler, sqlExecutionContext);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testHashJoinsReleaseCachesOnCursorClose() throws Exception {
        assertMemoryLeak(() -> {
            createTables(engine, sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // INNER JOIN: only the master symbols present in the slave table match
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT count(), count(s.price), sum(s.price) FROM master m JOIN slave s ON (sym)",
                        """
                                count\tcount1\tsum
                                1000\t1000\t499500.0
                                """
                );
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT count(), count(s.price), sum(s.price) FROM master m LEFT JOIN slave s ON (sym)",
                        """
                                count\tcount1\tsum
                                2000\t1000\t499500.0
                                """
                );
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT count(), count(m.val), sum(s.price) FROM master m RIGHT JOIN slave s ON (sym)",
                        """
                                count\tcount1\tsum
                                1000\t1000\t499500.0
                                """
                );
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT count(), count(s.price), sum(s.price) FROM master m FULL JOIN slave s ON (sym)",
                        """
                                count\tcount1\tsum
                                2000\t1000\t499500.0
                                """
                );
                // extra join condition routes to the filtered hash outer join factories
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT count(), count(s.price), sum(s.price) FROM master m LEFT JOIN slave s ON m.sym = s.sym AND s.price < m.val",
                        """
                                count\tcount1\tsum
                                2000\t1000\t499500.0
                                """
                );
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT count(), count(s.price), sum(s.price) FROM master m FULL JOIN slave s ON m.sym = s.sym AND s.price < m.val",
                        """
                                count\tcount1\tsum
                                2000\t1000\t499500.0
                                """
                );
            }
        });
    }

    @Test
    public void testSyncHorizonJoinReleasesCachesOnCursorClose() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, "false");
        assertMemoryLeak(() -> {
            createTables(engine, sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertHorizonJoins(compiler, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testTimeSeriesJoinsReleaseCachesOnCursorClose() throws Exception {
        assertMemoryLeak(() -> {
            createTables(engine, sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT count(), count(s.price), sum(s.price) FROM master m ASOF JOIN slave s ON (sym, sym2)",
                        """
                                count\tcount1\tsum
                                2000\t1000\t499500.0
                                """
                );
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT count(), count(s.price), sum(s.price) FROM master m LT JOIN slave s ON (sym, sym2)",
                        """
                                count\tcount1\tsum
                                2000\t1000\t499500.0
                                """
                );
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT /*+ asof_linear(m s) */ count(), count(s.price), sum(s.price) FROM master m ASOF JOIN slave s ON (sym, sym2)",
                        """
                                count\tcount1\tsum
                                2000\t1000\t499500.0
                                """
                );
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT /*+ asof_dense(m s) */ count(), count(s.price), sum(s.price) FROM master m ASOF JOIN slave s ON (sym, sym2)",
                        """
                                count\tcount1\tsum
                                2000\t1000\t499500.0
                                """
                );
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT count(), count(s.price), sum(s.price) FROM master m ASOF JOIN (slave WHERE price >= 0) s ON (sym, sym2)",
                        """
                                count\tcount1\tsum
                                2000\t1000\t499500.0
                                """
                );
            }
        });
    }

    private static void createTables(CairoEngine engine, SqlExecutionContext sqlExecutionContext) throws Exception {
        // ASOF and LT joins take the SymbolTranslatingRecord path only for multi-column keys,
        // hence the constant sym2 column. The slave table inserts its symbols in reverse order, so that each master symbol key
        // maps to a different slave key, and holds only half of the master symbols.
        engine.execute(
                """
                        CREATE TABLE slave AS (
                            SELECT ('s' || (%d - x))::SYMBOL sym, 'k'::SYMBOL sym2, (%d - x)::DOUBLE price, (x * 1_000_000)::TIMESTAMP ts
                            FROM long_sequence(%d)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """.formatted(SLAVE_SYMBOL_COUNT, SLAVE_SYMBOL_COUNT, SLAVE_SYMBOL_COUNT),
                sqlExecutionContext
        );
        engine.execute(
                """
                        CREATE TABLE master AS (
                            SELECT ('s' || (x - 1))::SYMBOL sym, 'k'::SYMBOL sym2, (x * 10_000)::DOUBLE val, (2_000_000_000 + x * 1_000_000)::TIMESTAMP ts
                            FROM long_sequence(%d)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """.formatted(MASTER_SYMBOL_COUNT),
                sqlExecutionContext
        );
    }

    private void assertCachesReleased(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String query,
            String expected
    ) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
            // The second execution checks that the closed caches reopen and translate correctly.
            for (int i = 0; i < 2; i++) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    TestUtils.assertEquals(expected, sink);
                    final long used = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP) - baseline;
                    if (isCacheCapped) {
                        // each cache stays at its initial 256 bytes, since it never reaches the rehash threshold
                        Assert.assertTrue(query + ", used: " + used, used > 0 && used <= 4 * 1024);
                    } else {
                        // the sym cache takes at least 1,000 translations, which grow it to 16 KiB or more
                        Assert.assertTrue(query + ", used: " + used, used >= 16 * 1024);
                    }
                }
                Assert.assertEquals(query, baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP));
            }
        }
    }

    private void assertHorizonJoins(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws Exception {
        assertCachesReleased(
                compiler,
                sqlExecutionContext,
                "SELECT count(sym), sum(p) FROM (" +
                        "SELECT m.sym, sum(s.price) p FROM master m HORIZON JOIN slave s ON (m.sym = s.sym) RANGE FROM 0s TO 0s STEP 1s AS h" +
                        ")",
                """
                        count\tsum
                        2000\t499500.0
                        """
        );
        assertCachesReleased(
                compiler,
                sqlExecutionContext,
                "SELECT count(s.price), sum(s.price) FROM master m HORIZON JOIN slave s ON (m.sym = s.sym) RANGE FROM 0s TO 0s STEP 1s AS h",
                """
                        count\tsum
                        1000\t499500.0
                        """
        );
        assertCachesReleased(
                compiler,
                sqlExecutionContext,
                "SELECT count(sym), sum(p) FROM (" +
                        "SELECT m.sym, sum(s.price) p, count(s2.price) c FROM master m " +
                        "HORIZON JOIN slave s ON (m.sym = s.sym) HORIZON JOIN slave s2 ON (m.sym = s2.sym) " +
                        "RANGE FROM 0s TO 0s STEP 1s AS h" +
                        ")",
                """
                        count\tsum
                        2000\t499500.0
                        """
        );
        assertCachesReleased(
                compiler,
                sqlExecutionContext,
                "SELECT count(s.price), sum(s.price), count(s2.price) FROM master m " +
                        "HORIZON JOIN slave s ON (m.sym = s.sym) HORIZON JOIN slave s2 ON (m.sym = s2.sym) " +
                        "RANGE FROM 0s TO 0s STEP 1s AS h",
                """
                        count\tsum\tcount1
                        1000\t499500.0\t1000
                        """
        );
    }
}
