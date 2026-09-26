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
import io.questdb.griffin.engine.join.HashJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinFilteredLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinFilteredRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncMultiHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncMultiHorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.MultiHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.MultiHorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.SymbolTranslatingRecord;
import io.questdb.mp.WorkerPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
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
 * The capped run limits each cache to 10 entries, which take at most one page and a small
 * hash map, so the caches must stay small, while the uncached translations must still produce
 * the same results. The uncapped run caches dense keys, which the caches keep in pages rather
 * than in a hash map, so the caches must take much less memory than hash maps with the same
 * entries.
 * <p>
 * The caches are the only execution-time user of {@link MemoryTag#NATIVE_JOIN_MAP}, so the
 * tag's counter measures them precisely.
 */
@RunWith(Parameterized.class)
public class SymbolTranslatingRecordTest extends AbstractCairoTest {
    // 10 entries take at most one 1 KiB page and a 256-byte hash map, besides the 256-byte page table.
    private static final int CAPPED_CACHE_CAPACITY = 10;
    private static final int MASTER_SYMBOL_COUNT = 20_000;
    private static final int SLAVE_SYMBOL_COUNT = 10_000;
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
                        assertHorizonJoins(
                                compiler,
                                sqlExecutionContext,
                                AsyncHorizonJoinRecordCursorFactory.class,
                                AsyncHorizonJoinNotKeyedRecordCursorFactory.class,
                                AsyncMultiHorizonJoinRecordCursorFactory.class,
                                AsyncMultiHorizonJoinNotKeyedRecordCursorFactory.class
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testFullFatHashJoinsReleaseCachesOnCursorClose() throws Exception {
        assertMemoryLeak(() -> {
            createTables(engine, sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // the compiler pool resets the flag when the compiler returns to the pool
                compiler.setFullFatJoins(true);
                assertHashJoins(
                        compiler,
                        HashJoinRecordCursorFactory.class,
                        HashOuterJoinRecordCursorFactory.class,
                        HashOuterJoinFilteredRecordCursorFactory.class
                );
            }
        });
    }

    @Test
    public void testHashJoinsReleaseCachesOnCursorClose() throws Exception {
        assertMemoryLeak(() -> {
            createTables(engine, sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertHashJoins(
                        compiler,
                        HashJoinLightRecordCursorFactory.class,
                        HashOuterJoinLightRecordCursorFactory.class,
                        HashOuterJoinFilteredLightRecordCursorFactory.class
                );
            }
        });
    }

    @Test
    public void testSyncHorizonJoinReleasesCachesOnCursorClose() throws Exception {
        // setUp() has already copied the configuration flag into the context, and the code
        // generator reads the context, so the test switches the context itself. The next
        // setUp() restores the flag from the configuration.
        sqlExecutionContext.setParallelHorizonJoinEnabled(false);
        assertMemoryLeak(() -> {
            createTables(engine, sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertHorizonJoins(
                        compiler,
                        sqlExecutionContext,
                        HorizonJoinRecordCursorFactory.class,
                        HorizonJoinNotKeyedRecordCursorFactory.class,
                        MultiHorizonJoinRecordCursorFactory.class,
                        MultiHorizonJoinNotKeyedRecordCursorFactory.class
                );
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
                                20000\t10000\t4.9995E7
                                """
                );
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT count(), count(s.price), sum(s.price) FROM master m LT JOIN slave s ON (sym, sym2)",
                        """
                                count\tcount1\tsum
                                20000\t10000\t4.9995E7
                                """
                );
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT /*+ asof_linear(m s) */ count(), count(s.price), sum(s.price) FROM master m ASOF JOIN slave s ON (sym, sym2)",
                        """
                                count\tcount1\tsum
                                20000\t10000\t4.9995E7
                                """
                );
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT /*+ asof_dense(m s) */ count(), count(s.price), sum(s.price) FROM master m ASOF JOIN slave s ON (sym, sym2)",
                        """
                                count\tcount1\tsum
                                20000\t10000\t4.9995E7
                                """
                );
                assertCachesReleased(
                        compiler,
                        sqlExecutionContext,
                        "SELECT count(), count(s.price), sum(s.price) FROM master m ASOF JOIN (slave WHERE price >= 0) s ON (sym, sym2)",
                        """
                                count\tcount1\tsum
                                20000\t10000\t4.9995E7
                                """
                );
            }
        });
    }

    private static void createTables(CairoEngine engine, SqlExecutionContext sqlExecutionContext) throws Exception {
        // ASOF and LT joins take the SymbolTranslatingRecord path only for multi-column keys,
        // hence the constant sym2 column. The slave table inserts its symbols in reverse order, so that each master symbol key
        // maps to a different slave key, and holds only half of the master symbols. All slave rows precede the master rows.
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
                            SELECT ('s' || (x - 1))::SYMBOL sym, 'k'::SYMBOL sym2, (x * 10_000)::DOUBLE val, (20_000_000_000 + x * 1_000_000)::TIMESTAMP ts
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
        assertCachesReleased(compiler, sqlExecutionContext, query, expected, null);
    }

    private void assertCachesReleased(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String query,
            String expected,
            @Nullable Class<? extends RecordCursorFactory> expectedFactory
    ) throws Exception {
        assertCachesReleased(compiler, sqlExecutionContext, query, expected, expectedFactory, 1);
    }

    // slaveCount is the number of slave tables in the query; each translates the master keys with its own caches
    private void assertCachesReleased(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String query,
            String expected,
            @Nullable Class<? extends RecordCursorFactory> expectedFactory,
            int slaveCount
    ) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            if (expectedFactory != null) {
                // guards against a silent reroute to a factory that the test does not target
                TestUtils.assertFactoryInTree(factory, expectedFactory, query);
            }
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
            // The second execution checks that the closed caches reopen and translate correctly.
            for (int i = 0; i < 2; i++) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    TestUtils.assertEquals(expected, sink);
                    final long used = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP) - baseline;
                    if (isCacheCapped) {
                        // Each cache takes at most 1,536 bytes, and a query opens at most 10 caches:
                        // one per slave for the owner and for each of the 4 workers of async HORIZON.
                        Assert.assertTrue(query + ", used: " + used, used > 0 && used <= 16 * 1024);
                    } else {
                        // the sym cache takes at least 10,000 translations of dense keys, which fill 40 pages of 1 KiB
                        Assert.assertTrue(query + ", used: " + used, used >= 40 * 1024);
                        // Each slave's caches hold at most the 20,000 dense master keys, which fill 79 pages.
                        // Async HORIZON splits the keys between the caches of the owner and the 4 workers by
                        // page frames of 5,000 rows (the 20,000 master rows over the pool's 4 workers), which
                        // touch 82 pages when counted per frame, and each cache adds a page table and a hash map
                        // of a few KiB for the keys that come before their page.
                        // That stays below 128 KiB per slave, while hash maps at load factor 0.5 take at least
                        // 16 bytes per key, over 156 KiB for those 10,000 translations.
                        Assert.assertTrue(query + ", used: " + used, used <= slaveCount * 128 * 1024);
                    }
                }
                Assert.assertEquals(query, baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP));
            }
        }
    }

    private void assertHashJoins(
            SqlCompiler compiler,
            Class<? extends RecordCursorFactory> innerJoinFactory,
            Class<? extends RecordCursorFactory> outerJoinFactory,
            Class<? extends RecordCursorFactory> filteredOuterJoinFactory
    ) throws Exception {
        // INNER JOIN: only the master symbols present in the slave table match
        assertCachesReleased(
                compiler,
                sqlExecutionContext,
                "SELECT count(), count(s.price), sum(s.price) FROM master m JOIN slave s ON (sym)",
                """
                        count\tcount1\tsum
                        10000\t10000\t4.9995E7
                        """,
                innerJoinFactory
        );
        assertCachesReleased(
                compiler,
                sqlExecutionContext,
                "SELECT count(), count(s.price), sum(s.price) FROM master m LEFT JOIN slave s ON (sym)",
                """
                        count\tcount1\tsum
                        20000\t10000\t4.9995E7
                        """,
                outerJoinFactory
        );
        assertCachesReleased(
                compiler,
                sqlExecutionContext,
                "SELECT count(), count(m.val), sum(s.price) FROM master m RIGHT JOIN slave s ON (sym)",
                """
                        count\tcount1\tsum
                        10000\t10000\t4.9995E7
                        """,
                outerJoinFactory
        );
        assertCachesReleased(
                compiler,
                sqlExecutionContext,
                "SELECT count(), count(s.price), sum(s.price) FROM master m FULL JOIN slave s ON (sym)",
                """
                        count\tcount1\tsum
                        20000\t10000\t4.9995E7
                        """,
                outerJoinFactory
        );
        // extra join condition routes to the filtered hash outer join factories
        assertCachesReleased(
                compiler,
                sqlExecutionContext,
                "SELECT count(), count(s.price), sum(s.price) FROM master m LEFT JOIN slave s ON m.sym = s.sym AND s.price < m.val",
                """
                        count\tcount1\tsum
                        20000\t10000\t4.9995E7
                        """,
                filteredOuterJoinFactory
        );
        assertCachesReleased(
                compiler,
                sqlExecutionContext,
                "SELECT count(), count(s.price), sum(s.price) FROM master m FULL JOIN slave s ON m.sym = s.sym AND s.price < m.val",
                """
                        count\tcount1\tsum
                        20000\t10000\t4.9995E7
                        """,
                filteredOuterJoinFactory
        );
    }

    private void assertHorizonJoins(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            Class<? extends RecordCursorFactory> keyedFactory,
            Class<? extends RecordCursorFactory> notKeyedFactory,
            Class<? extends RecordCursorFactory> multiKeyedFactory,
            Class<? extends RecordCursorFactory> multiNotKeyedFactory
    ) throws Exception {
        assertCachesReleased(
                compiler,
                sqlExecutionContext,
                """
                        SELECT count(sym), sum(p) FROM (
                            SELECT m.sym, sum(s.price) p FROM master m HORIZON JOIN slave s ON (m.sym = s.sym) RANGE FROM 0s TO 0s STEP 1s AS h
                        )
                        """,
                """
                        count\tsum
                        20000\t4.9995E7
                        """,
                keyedFactory
        );
        assertCachesReleased(
                compiler,
                sqlExecutionContext,
                "SELECT count(s.price), sum(s.price) FROM master m HORIZON JOIN slave s ON (m.sym = s.sym) RANGE FROM 0s TO 0s STEP 1s AS h",
                """
                        count\tsum
                        10000\t4.9995E7
                        """,
                notKeyedFactory
        );
        assertCachesReleased(
                compiler,
                sqlExecutionContext,
                """
                        SELECT count(sym), sum(p) FROM (
                            SELECT m.sym, sum(s.price) p, count(s2.price) c FROM master m
                            HORIZON JOIN slave s ON (m.sym = s.sym) HORIZON JOIN slave s2 ON (m.sym = s2.sym)
                            RANGE FROM 0s TO 0s STEP 1s AS h
                        )
                        """,
                """
                        count\tsum
                        20000\t4.9995E7
                        """,
                multiKeyedFactory,
                2
        );
        assertCachesReleased(
                compiler,
                sqlExecutionContext,
                """
                        SELECT count(s.price), sum(s.price), count(s2.price) FROM master m
                        HORIZON JOIN slave s ON (m.sym = s.sym) HORIZON JOIN slave s2 ON (m.sym = s2.sym)
                        RANGE FROM 0s TO 0s STEP 1s AS h
                        """,
                """
                        count\tsum\tcount1
                        10000\t4.9995E7\t10000
                        """,
                multiNotKeyedFactory,
                2
        );
    }
}
