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
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The stable LSD radix sort of one-word encoded sort keys (Vect.sortEncodedEntriesLsd): sorted as the
 * MSD sort sorts, (key, row id), and declined when the row ids do not ascend or the keys vary too widely.
 */
public class EncodedSortLsdTest extends AbstractCairoTest {

    @Test
    public void testLsdDeclinesDescendingRowIds() {
        final int n = 100_000;
        final long a = Unsafe.malloc(16L * n, MemoryTag.NATIVE_DEFAULT);
        final long scratch = Unsafe.malloc(16L * n, MemoryTag.NATIVE_DEFAULT);
        try {
            final Rnd rnd = new Rnd();
            for (int i = 0; i < n; i++) {
                Unsafe.putLong(a + 16L * i, rnd.nextLong() & 0xFFFFFL);
                Unsafe.putLong(a + 16L * i + 8, n - i);
            }
            final long before = checksum(a, n);
            Assert.assertFalse(Vect.sortEncodedEntriesLsd(a, n, scratch));
            Assert.assertEquals(before, checksum(a, n));
        } finally {
            Unsafe.free(a, 16L * n, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(scratch, 16L * n, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testLsdMatchesTheMsdSort() {
        // key widths: one bit, a byte, the 11-bit digit edge, two and three digits, a high-bit span,
        // and one beyond three digits that the LSD sort declines
        final long[] masks = {0x1L, 0xFFL, 0x7FFL, 0xFFFL, 0x3FFFFFL, 0x1FFFFFFFFL, 0xFFFF000000000000L, 0xFFFFFFFFFFL};
        final int n = 300_000;
        final long a = Unsafe.malloc(16L * n, MemoryTag.NATIVE_DEFAULT);
        final long b = Unsafe.malloc(16L * n, MemoryTag.NATIVE_DEFAULT);
        final long scratch = Unsafe.malloc(16L * n, MemoryTag.NATIVE_DEFAULT);
        try {
            final Rnd rnd = new Rnd();
            for (long mask : masks) {
                for (int i = 0; i < n; i++) {
                    final long k = (rnd.nextLong() & mask) | 0x0100000000000000L;
                    Unsafe.putLong(a + 16L * i, k);
                    Unsafe.putLong(a + 16L * i + 8, 3L * i + 7);
                }
                Vect.memcpy(b, a, 16L * n);
                final boolean applied = Vect.sortEncodedEntriesLsd(a, n, scratch);
                Assert.assertEquals("mask " + Long.toHexString(mask), mask != 0xFFFFFFFFFFL, applied);
                Vect.sortEncodedEntries(b, n, 1, 1_000);
                if (applied) {
                    for (int i = 0; i < n; i++) {
                        Assert.assertEquals("key at " + i, Unsafe.getLong(b + 16L * i), Unsafe.getLong(a + 16L * i));
                        Assert.assertEquals("row id at " + i, Unsafe.getLong(b + 16L * i + 8), Unsafe.getLong(a + 16L * i + 8));
                    }
                }
            }
        } finally {
            Unsafe.free(a, 16L * n, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(b, 16L * n, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(scratch, 16L * n, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testOrderByOneKeyIsStable() throws Exception {
        // a parallel threshold below the row count sends one-word keys to the LSD sort
        setProperty(PropertyKey.CAIRO_SQL_SORT_ENCODED_PARALLEL_THRESHOLD, 1000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT"
                    + " timestamp_sequence('2024-01-01', 1000) ts,"
                    + " (rnd_int(0, 400, 0) / 4.0)::FLOAT f,"
                    + " rnd_double(2) / 3 d,"
                    + " rnd_int(-50, 50, 2) i,"
                    + " rnd_long(-1_000_000, 1_000_000, 2) l,"
                    + " dateadd('s', rnd_int(0, 1000, 0), '2024-01-01'::TIMESTAMP) t2"
                    + " FROM long_sequence(60_000)) TIMESTAMP(ts) PARTITION BY HOUR");
            for (String k : new String[]{"f", "d", "i", "l", "t2", "(f * 2)", "(i + 1)"}) {
                for (String dir : new String[]{"", " DESC"}) {
                    // rows were inserted in ts order, so a stable sort on k is a sort on (k, ts);
                    // the two-word key takes the MSD sort
                    assertSame(
                            "SELECT ts, " + k + " k FROM t ORDER BY " + k + dir,
                            "SELECT ts, " + k + " k FROM t ORDER BY " + k + dir + ", ts"
                    );
                    // a sub-query materializes its rows: the record chain's cursor, not the light one
                    assertSame(
                            "SELECT ts, k FROM (SELECT ts, " + k + " k, i + 0 x FROM t) ORDER BY k" + dir,
                            "SELECT ts, k FROM (SELECT ts, " + k + " k, i + 0 x FROM t) ORDER BY k" + dir + ", ts"
                    );
                }
            }
        });
    }

    @Test
    public void testSortCopiesSymbolBlocks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tr AS (SELECT"
                    + " timestamp_sequence('2024-01-01', 1000) ts,"
                    + " rnd_symbol('A', 'B', 'C', NULL) ex,"
                    + " rnd_symbol(300, 2, 5, 2) sym,"
                    + " rnd_symbol('x', 'y') cond,"
                    + " rnd_int(1, 20, 0)::FLOAT size,"
                    + " rnd_boolean() b"
                    + " FROM long_sequence(80_000)) TIMESTAMP(ts) PARTITION BY HOUR");
            // the Min/Max Filter's rows reach the sort as row blocks, SYMBOL columns as keys; the sort
            // keeps their order (ts) among equal keys
            final String rows = "SELECT ts, ex, sym, cond, size, b FROM (SELECT ts, ex, sym, cond, size, b, min(size) OVER (PARTITION BY ex, sym) ms FROM tr) WHERE size = ms";
            final StringSink plan = new StringSink();
            printSql("EXPLAIN " + rows + " ORDER BY sym", plan);
            TestUtils.assertContains(plan, "Min/Max Filter");
            for (String k : new String[]{"sym", "ex", "cond", "sym DESC"}) {
                assertSame(rows + " ORDER BY " + k, rows + " ORDER BY " + k + ", ts");
            }
        });
    }

    private static void assertSame(String sql, String reference) throws Exception {
        final StringSink a = new StringSink();
        final StringSink b = new StringSink();
        printSql(sql, a);
        printSql(reference, b);
        TestUtils.assertEquals(b, a);
    }

    private static long checksum(long addr, int n) {
        long h = 0;
        for (int i = 0; i < 2 * n; i++) {
            h = h * 31 + Unsafe.getLong(addr + 8L * i);
        }
        return h;
    }
}
