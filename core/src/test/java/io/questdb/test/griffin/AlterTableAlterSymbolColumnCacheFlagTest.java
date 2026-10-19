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

import io.questdb.cairo.MapWriter;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class AlterTableAlterSymbolColumnCacheFlagTest extends AbstractCairoTest {

    @Test
    public void testAlterExpectColumnKeyword() throws Exception {
        assertFailure("alter table x alter", 19, "'column' expected");
    }

    @Test
    public void testAlterExpectColumnName() throws Exception {
        assertFailure("alter table x alter column", 26, "column name expected");
    }

    @Test
    public void testAlterFlagInNonSymbolColumn() throws Exception {
        assertFailure("alter table x alter column b cache", 27, "cache is only supported for symbol type");
    }

    @Test
    public void testAlterSymbolCacheFlagToFalseAndCheckOpenReaderWithCursor() throws Exception {
        String expectedOrdered = """
                sym
                googl
                googl
                googl
                googl
                googl
                googl
                ibm
                ibm
                msft
                msft
                """;

        String expectedChronological = """
                sym\tk
                msft\t1970-01-01T00:00:00.000000Z
                googl\t1970-01-01T00:16:40.000000Z
                googl\t1970-01-01T00:33:20.000000Z
                ibm\t1970-01-01T00:50:00.000000Z
                googl\t1970-01-01T01:06:40.000000Z
                ibm\t1970-01-01T01:23:20.000000Z
                googl\t1970-01-01T01:40:00.000000Z
                googl\t1970-01-01T01:56:40.000000Z
                googl\t1970-01-01T02:13:20.000000Z
                msft\t1970-01-01T02:30:00.000000Z
                """;

        assertMemoryLeak(() -> {
            createX();

            assertQuery("select sym from x order by sym")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expectedOrdered);

            assertQuery("select sym, k from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expectedChronological);

            try (TableWriter writer = getWriter("x")) {
                writer.changeCacheFlag(1, false);
            }

            assertQuery("select sym, k from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expectedChronological);

            assertQuery("select sym from x order by 1 asc")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expectedOrdered);
        });
    }

    @Test
    public void testAlterSymbolCacheFlagToTrueCheckOpenReaderWithCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (i int, sym symbol nocache) ;");
            execute("insert into x values (1, 'GBP')");
            execute("insert into x values (2, 'CHF')");
            execute("insert into x values (3, 'GBP')");
            execute("insert into x values (4, 'JPY')");
            execute("insert into x values (5, 'USD')");
            execute("insert into x values (6, 'GBP')");
            execute("insert into x values (7, 'GBP')");
            execute("insert into x values (8, 'GBP')");
            execute("insert into x values (9, 'GBP')");

            String expectedOrdered = """
                    sym
                    CHF
                    GBP
                    GBP
                    GBP
                    GBP
                    GBP
                    GBP
                    JPY
                    USD
                    """;

            String expectedChronological = """
                    i\tsym
                    1\tGBP
                    2\tCHF
                    3\tGBP
                    4\tJPY
                    5\tUSD
                    6\tGBP
                    7\tGBP
                    8\tGBP
                    9\tGBP
                    """;

            assertQuery("select sym from x order by sym")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expectedOrdered);

            assertQuery("select i, sym from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expectedChronological);

            try (TableWriter writer = getWriter("x")) {
                writer.changeCacheFlag(1, true);
            }

            assertQuery("select i, sym from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expectedChronological);

            assertQuery("select sym from x order by 1 asc")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expectedOrdered);
        });
    }

    @Test
    public void testAlterSymbolNocacheThenCacheOnANonWalTable() throws Exception {
        assertNocacheThenCacheRetargetsTheLiveWriter(false);
    }

    @Test
    public void testAlterSymbolNocacheThenCacheOnAWalTable() throws Exception {
        assertNocacheThenCacheRetargetsTheLiveWriter(true);
    }

    @Test
    public void testBadSyntax() throws Exception {
        assertFailure("alter table x alter column c", 28, "'add index' or 'drop index' or 'type' or 'cache' or 'nocache' or 'symbol' expected");
    }

    @Test
    public void testCacheAlterRestoresTheDroppedCacheOnANonWalTable() throws Exception {
        assertCacheAlterRestoresTheDroppedCache(false);
    }

    @Test
    public void testCacheAlterRestoresTheDroppedCacheOnAWalTable() throws Exception {
        assertCacheAlterRestoresTheDroppedCache(true);
    }

    @Test
    public void testInvalidColumn() throws Exception {
        assertFailure("alter table x alter column y cache", 27, "column 'y' does not exist in table 'x'");
    }

    @Test
    public void testRedundantCacheAlterKeepsTheMetadataVersionAndTheWarmCache() throws Exception {
        // The recovery the two exhaustion tests drive must not cost the common case
        // anything. A CACHE the column already carries has nothing to change, so it must not
        // rewrite the table metadata - which swaps _meta through _meta.swp and bumps the
        // metadata version every reader watches - and must not throw away the cache the
        // writer has been filling, which updateCacheFlag() would do because it replaces
        // unconditionally.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL CACHE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t SELECT 'warm-symbol-' || x, x::timestamp FROM long_sequence(5000)");

            final long metadataVersion;
            final long nativeBytes;
            try (TableWriter writer = getWriter("t")) {
                Assert.assertTrue(
                        "the column was declared CACHE, so its writer must hold a cache",
                        writer.getSymbolMapWriter(0).isCacheAllocated()
                );
                metadataVersion = writer.getMetadataVersion();
                // The cache is the writer's only NATIVE_TABLE_WRITER allocation - the mapped
                // symbol files are charged to MMAP_INDEX_WRITER - and five thousand distinct
                // values have grown it well past what a replacement would start at, so a
                // discard and rebuild shows up here as a drop.
                nativeBytes = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);
            }

            execute("ALTER TABLE t ALTER COLUMN s CACHE");

            try (TableWriter writer = getWriter("t")) {
                Assert.assertEquals(
                        "a CACHE the column already carries must not rewrite table metadata",
                        metadataVersion,
                        writer.getMetadataVersion()
                );
                Assert.assertTrue(writer.getSymbolMapWriter(0).isCacheAllocated());
                Assert.assertEquals(
                        "a CACHE the column already carries must not discard the warm cache",
                        nativeBytes,
                        Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER)
                );
            }
        });
    }

    @Test
    public void testWhenCacheOrNocacheAreNotInAlterStatement() throws Exception {
        assertFailure("alter table x alter column c ca", 29, "'cache' or 'nocache' expected");
    }

    /**
     * A writer whose cache ran its key buffer out drops the cache and keeps the flag, because
     * the drop is an internal fallback rather than a change the column asked for. That leaves
     * ALTER TABLE ... ALTER COLUMN ... CACHE as the obvious lever to get the acceleration back,
     * and it has to actually pull: the writer serving the column is pooled and nothing else
     * revisits the decision before it closes.
     * <p>
     * The lowered key-buffer ceiling is what makes the state reachable in a test - at the
     * production ceiling it takes eight gigabytes of distinct values in one column.
     */
    private void assertCacheAlterRestoresTheDroppedCache(boolean isWal) throws Exception {
        // Sixteen chars per value, so six fit the 256-byte key buffer and the seventh
        // trips the drop.
        final long previousLimit = SymbolMapWriter.setCacheKeyBufferLimit(256);
        try {
            assertMemoryLeak(() -> {
                execute(
                        "CREATE TABLE t (s SYMBOL CACHE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY "
                                + (isWal ? "WAL" : "BYPASS WAL")
                );
                execute("""
                        INSERT INTO t VALUES
                        ('cached-symbol-01', 1), ('cached-symbol-02', 2), ('cached-symbol-03', 3),
                        ('cached-symbol-04', 4), ('cached-symbol-05', 5), ('cached-symbol-06', 6),
                        ('cached-symbol-07', 7), ('cached-symbol-08', 8), ('cached-symbol-09', 9),
                        ('cached-symbol-10', 10), ('cached-symbol-11', 11), ('cached-symbol-12', 12)
                        """);
                if (isWal) {
                    drainWalQueue();
                }

                final TableWriter pooled;
                try (TableWriter writer = getWriter("t")) {
                    pooled = writer;
                    final MapWriter symbolMapWriter = writer.getSymbolMapWriter(0);
                    Assert.assertTrue("the column still asks for a cache", symbolMapWriter.isCached());
                    Assert.assertFalse(
                            "setup: the writer must have dropped its cache on key-buffer exhaustion",
                            symbolMapWriter.isCacheAllocated()
                    );
                }

                execute("ALTER TABLE t ALTER COLUMN s CACHE");
                if (isWal) {
                    drainWalQueue();
                }

                try (TableWriter writer = getWriter("t")) {
                    Assert.assertSame("the ALTER must have landed on this very writer", pooled, writer);
                    final MapWriter symbolMapWriter = writer.getSymbolMapWriter(0);
                    Assert.assertTrue(
                            "CACHE must hand the cache back to a writer that dropped it, rather than"
                                    + " leave the column on the on-disk index for the life of the writer",
                            symbolMapWriter.isCacheAllocated()
                    );
                    Assert.assertTrue(symbolMapWriter.isCached());
                }

                // The replacement cache starts empty over a non-empty column, so the values
                // keep the keys the on-disk index already holds and a repeat of one of them
                // is still a repeat.
                execute("INSERT INTO t VALUES ('cached-symbol-01', 13), ('cached-symbol-13', 14)");
                if (isWal) {
                    drainWalQueue();
                }
                assertQuery("SELECT count() rows, count_distinct(s) symbols FROM t")
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                rows\tsymbols
                                14\t13
                                """);
            });
        } finally {
            SymbolMapWriter.setCacheKeyBufferLimit(previousLimit);
        }
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            try {
                createX();
                execute(sql);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
        });
    }

    /**
     * Drives both cache-flag directions through real SQL and asserts they retarget the
     * writer that is already serving the column, rather than only the header a later
     * writer would read. WAL and non-WAL reach the same place by different routes: a
     * non-WAL ALTER applies to the table writer the compiler takes out of the pool, a WAL
     * one is a non-structural change that rides the WAL and is re-executed against the
     * table writer by the apply job. The identity check on the pooled writer is what keeps
     * the case honest - a writer the pool had discarded and rebuilt from the header would
     * satisfy every other assertion here without the ALTER having done anything.
     */
    private void assertNocacheThenCacheRetargetsTheLiveWriter(boolean isWal) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE t (s SYMBOL CACHE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY "
                            + (isWal ? "WAL" : "BYPASS WAL")
            );
            execute("INSERT INTO t VALUES ('a', 0), ('b', 1), ('c', 2), ('d', 3), ('e', 4)");
            if (isWal) {
                drainWalQueue();
            }

            final TableWriter pooled;
            try (TableWriter writer = getWriter("t")) {
                pooled = writer;
                Assert.assertTrue(
                        "the column was declared CACHE, so its writer must hold a cache",
                        writer.getSymbolMapWriter(0).isCacheAllocated()
                );
            }

            execute("ALTER TABLE t ALTER COLUMN s NOCACHE");
            if (isWal) {
                drainWalQueue();
            }

            try (TableWriter writer = getWriter("t")) {
                Assert.assertSame("the ALTER must have landed on this very writer", pooled, writer);
                final MapWriter symbolMapWriter = writer.getSymbolMapWriter(0);
                Assert.assertFalse(
                        "NOCACHE must release the cache of the writer already serving the column",
                        symbolMapWriter.isCacheAllocated()
                );
                Assert.assertFalse(symbolMapWriter.isCached());
            }

            execute("ALTER TABLE t ALTER COLUMN s CACHE");
            if (isWal) {
                drainWalQueue();
            }

            try (TableWriter writer = getWriter("t")) {
                Assert.assertSame("the ALTER must have landed on this very writer", pooled, writer);
                final MapWriter symbolMapWriter = writer.getSymbolMapWriter(0);
                Assert.assertTrue(
                        "CACHE must build a cache for the writer already serving the column",
                        symbolMapWriter.isCacheAllocated()
                );
                Assert.assertTrue(symbolMapWriter.isCached());
            }

            // Both flips left the column's values where they were.
            execute("INSERT INTO t VALUES ('a', 5), ('f', 6)");
            if (isWal) {
                drainWalQueue();
            }
            assertQuery("SELECT s, count() FROM t ORDER BY s")
                    .expectSize()
                    .returns("""
                            s\tcount
                            a\t2
                            b\t1
                            c\t1
                            d\t1
                            e\t1
                            f\t1
                            """);
        });
    }

    private void createX() throws SqlException {
        execute(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(10)" +
                        ") timestamp (timestamp);"
        );
    }
}
