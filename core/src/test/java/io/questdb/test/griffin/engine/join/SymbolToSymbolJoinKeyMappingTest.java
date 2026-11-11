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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.join.AsOfJoinDenseSingleSymbolRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinIndexedRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinMemoizedRecordCursorFactory;
import io.questdb.griffin.engine.join.SymbolJoinKeyMapping;
import io.questdb.griffin.engine.join.SymbolToSymbolJoinKeyMapping;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Verifies that {@link SymbolToSymbolJoinKeyMapping} translates master symbol keys to slave
 * symbol keys correctly, and that it keeps its translation cache in native memory only while
 * a cursor is open: the owning cursor releases the cache on close, while the factory stays
 * alive (as it does in the query cache), and reopens it on the next execution.
 * <p>
 * The capped run limits the cache to 5 entries, which take a single page of the cache, so the
 * cache must stay at one page, while the uncached translations must still produce the same
 * results.
 * <p>
 * The translation caches are the only execution-time user of {@link MemoryTag#NATIVE_JOIN_MAP},
 * so the tag's counter measures them precisely.
 */
@RunWith(Parameterized.class)
public class SymbolToSymbolJoinKeyMappingTest extends AbstractCairoTest {
    // The first 5 master symbols have keys 0 to 4, which fit a single page.
    private static final int CAPPED_CACHE_CAPACITY = 5;
    // An open cache without entries takes its page table: 32 slots of 8 bytes.
    private static final long INITIAL_CACHE_SIZE = 256;
    // The number of master symbols s0..s19 that the slave table holds in the null key tests.
    private static final int NULL_TEST_FOUND_SYMBOL_COUNT = 10;
    // A page of the cache takes 256 slots of 4 bytes, for 256 consecutive master symbol keys.
    private static final long PAGE_SIZE = 1024;
    private final boolean isCacheCapped;

    public SymbolToSymbolJoinKeyMappingTest(boolean isCacheCapped) {
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
            setProperty(PropertyKey.CAIRO_SQL_ASOF_JOIN_SHORT_CIRCUIT_CACHE_CAPACITY, CAPPED_CACHE_CAPACITY);
        }
    }

    @Test
    public void testChainedShortCircuitReleasesCachesOnCursorClose() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Casting the master symbols makes their symbol tables non-static, so the join can't
            // compare symbol keys and falls back to a ChainedSymbolShortCircuit over two mappings.
            final String query = """
                    SELECT count(), count(s.price), sum(s.price)
                    FROM (SELECT (sym || '')::SYMBOL sym, (sym2 || '')::SYMBOL sym2, val, ts FROM master) m
                    ASOF JOIN slave s ON (sym, sym2)
                    """;
            // The symbolKeyJoin attribute would mean SymbolTranslatingRecord, not the short circuit.
            assertQuery(query).noLeakCheck().assertsPlanNotContaining("symbolKeyJoin");
            assertCachesReleased(
                    query,
                    AsOfJoinFastRecordCursorFactory.class,
                    """
                            count\tcount1\tsum
                            2000\t1000\t499500.0
                            """,
                    2
            );
        });
    }

    @Test
    public void testNullMasterKeysBypassCache() throws Exception {
        assertMemoryLeak(() -> {
            createNullKeyTables(true);
            assertSlaveKeys();
        });
    }

    @Test
    public void testNullMasterKeysNotFoundWhenSlaveHasNoNull() throws Exception {
        assertMemoryLeak(() -> {
            createNullKeyTables(false);
            assertSlaveKeys();
        });
    }

    @Test
    public void testOfShrinksGrownCache() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (
                    SymbolToSymbolJoinKeyMapping mapping = new SymbolToSymbolJoinKeyMapping(configuration, 0, 0);
                    RecordCursorFactory masterFactory = select("SELECT sym FROM master");
                    RecordCursorFactory slaveFactory = select("SELECT sym FROM slave");
                    RecordCursor masterCursor = masterFactory.getCursor(sqlExecutionContext);
                    RecordCursor slaveCursor = slaveFactory.getCursor(sqlExecutionContext)
            ) {
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
                mapping.reopen();
                mapping.of(slaveCursor);
                final Record masterRecord = masterCursor.getRecord();
                while (masterCursor.hasNext()) {
                    mapping.getSlaveKey(masterRecord);
                }
                final long used = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP) - baseline;
                if (isCacheCapped) {
                    // 5 translations of keys 0 to 4 take one page
                    Assert.assertEquals(CAPPED_CACHE_CAPACITY, mapping.getCacheSize());
                    Assert.assertEquals(INITIAL_CACHE_SIZE + PAGE_SIZE, used);
                } else {
                    // 1,000 translations of keys 0 to 999 take 4 pages, where a hash map takes 16 KiB
                    Assert.assertEquals(1_000, mapping.getCacheSize());
                    Assert.assertEquals(INITIAL_CACHE_SIZE + 4 * PAGE_SIZE, used);
                }

                // A re-initialization without close() shrinks the cache back to its initial capacity.
                mapping.of(slaveCursor);
                Assert.assertEquals(0, mapping.getCacheSize());
                Assert.assertEquals(INITIAL_CACHE_SIZE, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP) - baseline);

                mapping.close();
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP));
            }
        });
    }

    @Test
    public void testTimeSeriesJoinsReleaseCacheOnCursorClose() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String expected = """
                    count\tcount1\tsum
                    2000\t1000\t499500.0
                    """;
            final String query = "SELECT %s count(), count(s.price), sum(s.price) FROM master m ASOF JOIN slave s ON (sym)";

            assertCachesReleased(query.formatted(""), AsOfJoinFastRecordCursorFactory.class, expected, 1);
            assertCachesReleased(query.formatted("/*+ asof_linear(m s) */"), AsOfJoinLightRecordCursorFactory.class, expected, 1);
            assertCachesReleased(query.formatted("/*+ asof_dense(m s) */"), AsOfJoinDenseSingleSymbolRecordCursorFactory.class, expected, 1);
            assertCachesReleased(query.formatted("/*+ asof_index(m s) */"), AsOfJoinIndexedRecordCursorFactory.class, expected, 1);
            assertCachesReleased(query.formatted("/*+ asof_memoized(m s) */"), AsOfJoinMemoizedRecordCursorFactory.class, expected, 1);
            assertCachesReleased(query.formatted("/*+ asof_memoized_driveby(m s) */"), AsOfJoinMemoizedRecordCursorFactory.class, expected, 1);
        });
    }

    private static void createNullKeyTables(boolean hasSlaveNull) throws Exception {
        // The slave holds s0..s9, plus a NULL when hasSlaveNull is set.
        execute(
                """
                        CREATE TABLE slave AS (
                            SELECT (CASE WHEN x <= 10 THEN 's' || (x - 1) END)::SYMBOL sym
                            FROM long_sequence(%d)
                        )
                        """.formatted(hasSlaveNull ? 11 : 10)
        );
        // One master row in 1,000 holds a symbol from s0..s19, and the other 99,900 rows are NULL.
        execute(
                """
                        CREATE TABLE master AS (
                            SELECT (CASE WHEN x % 1_000 = 0 THEN 's' || (x / 1_000 % 20) END)::SYMBOL sym
                            FROM long_sequence(100_000)
                        )
                        """
        );
    }

    private static void createTables() throws Exception {
        // The slave table inserts its symbols in reverse order, so that each master symbol key
        // maps to a different slave key, and holds only half of the master symbols. The sym2
        // column serves the multi-column key join. The index serves the asof_index hint.
        execute(
                """
                        CREATE TABLE slave AS (
                            SELECT ('s' || (1_000 - x))::SYMBOL sym, 'k'::SYMBOL sym2, (1_000 - x)::DOUBLE price, (x * 1_000_000)::TIMESTAMP ts
                            FROM long_sequence(1_000)
                        ), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY
                        """
        );
        execute(
                """
                        CREATE TABLE master AS (
                            SELECT ('s' || (x - 1))::SYMBOL sym, 'k'::SYMBOL sym2, (x * 10_000)::DOUBLE val, (2_000_000_000 + x * 1_000_000)::TIMESTAMP ts
                            FROM long_sequence(2_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """
        );
    }

    private void assertCachesReleased(String query, Class<?> factoryClass, String expected, int cacheCount) throws Exception {
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()
        ) {
            TestUtils.assertFactoryInTree(factory, factoryClass, query);
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
            // The second execution checks that the closed caches reopen and translate correctly.
            for (int i = 0; i < 2; i++) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    TestUtils.assertEquals(expected, sink);
                    final long used = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP) - baseline;
                    if (isCacheCapped) {
                        // each cache holds at most 5 translations of keys below 256, which take one page
                        Assert.assertEquals(query, cacheCount * (INITIAL_CACHE_SIZE + PAGE_SIZE), used);
                    } else {
                        // the sym cache takes 1,000 translations of keys 0 to 999, which take 4 pages
                        Assert.assertTrue(query + ", used: " + used, used >= INITIAL_CACHE_SIZE + 4 * PAGE_SIZE);
                    }
                }
                Assert.assertEquals(query, baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP));
            }
        }
    }

    private void assertSlaveKeys() throws Exception {
        try (
                SymbolToSymbolJoinKeyMapping mapping = new SymbolToSymbolJoinKeyMapping(configuration, 0, 0);
                RecordCursorFactory masterFactory = select("master");
                RecordCursorFactory slaveFactory = select("slave");
                RecordCursor masterCursor = masterFactory.getCursor(sqlExecutionContext);
                RecordCursor slaveCursor = slaveFactory.getCursor(sqlExecutionContext)
        ) {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
            final StaticSymbolTable slaveSymbolTable = SymbolJoinKeyMapping.toStaticSymbolTable(slaveCursor.getSymbolTable(0));
            final Record masterRecord = masterCursor.getRecord();
            // The second execution checks that the closed cache reopens and translates correctly.
            for (int i = 0; i < 2; i++) {
                mapping.reopen();
                mapping.of(slaveCursor);
                masterCursor.toTop();
                long rowCount = 0;
                while (masterCursor.hasNext()) {
                    final CharSequence symbol = masterRecord.getSymA(0);
                    final int expectedSlaveKey;
                    if (symbol == null) {
                        expectedSlaveKey = slaveSymbolTable.containsNullValue()
                                ? SymbolTable.VALUE_IS_NULL
                                : StaticSymbolTable.VALUE_NOT_FOUND;
                    } else {
                        expectedSlaveKey = slaveSymbolTable.keyOf(symbol);
                    }
                    Assert.assertEquals("row " + rowCount + ", symbol " + symbol, expectedSlaveKey, mapping.getSlaveKey(masterRecord));
                    Assert.assertEquals(expectedSlaveKey == StaticSymbolTable.VALUE_NOT_FOUND, mapping.isShortCircuit(masterRecord));
                    rowCount++;
                }
                Assert.assertEquals(100_000, rowCount);
                // The cache holds only the found symbols: neither the NULL master keys,
                // nor the symbols missing from the slave table, take any entries.
                Assert.assertEquals(
                        isCacheCapped ? CAPPED_CACHE_CAPACITY : NULL_TEST_FOUND_SYMBOL_COUNT,
                        mapping.getCacheSize()
                );
                // The found master symbols s1..s9 and s0 have keys 0 to 8 and 19, which take one page.
                Assert.assertEquals(INITIAL_CACHE_SIZE + PAGE_SIZE, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP) - baseline);
                mapping.close();
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP));
            }
        }
    }
}
