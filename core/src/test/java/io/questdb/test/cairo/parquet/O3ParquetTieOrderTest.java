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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An O3 row whose timestamp ties the boundary of two parquet row groups lands after every stored row
 * of that timestamp, in every row group that holds it, as in a native partition. The parquet table
 * {@code pt} goes through the commit, its native twin {@code tw} takes the same SQL; both must hold
 * the rows in the same order, compared row by row, and in the order written out in each test.
 * <p>
 * Partition 2024-01-01 is converted with row groups of 4 rows; 2024-01-02 holds one row, so
 * 2024-01-01 is not the last partition. The O3 commit updates the parquet file in place or rewrites
 * it, see {@link #setProperties()}.
 */
@RunWith(Parameterized.class)
public class O3ParquetTieOrderTest extends AbstractCairoTest {
    private static final String DAY = "2024-01-01";
    private final boolean isRewrite;
    private final boolean isWal;
    private final TestTimestampType timestampType;

    public O3ParquetTieOrderTest(WalMode walMode, boolean isRewrite, TestTimestampType timestampType) {
        this.isWal = walMode == WalMode.WITH_WAL;
        this.isRewrite = isRewrite;
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}-rewrite={1}-{2}")
    public static Collection<Object[]> data() {
        final List<Object[]> params = new ArrayList<>();
        for (WalMode walMode : WalMode.values()) {
            for (boolean isRewrite : new boolean[]{false, true}) {
                for (TestTimestampType timestampType : TestTimestampType.values()) {
                    params.add(new Object[]{walMode, isRewrite, timestampType});
                }
            }
        }
        return params;
    }

    @Test
    public void testDedupSkipTieMatchesNative() throws Exception {
        // DEDUP SKIP KEYS(ts, v): the coalesced merge drops the resent row 5 and puts the new keys 0 and 50
        // after the stored rows at 03:00
        Assume.assumeTrue(isWal);
        setProperties();
        assertMemoryLeak(() -> {
            createTables(" DEDUP SKIP KEYS(ts, v)", 1, 2, 3, 4, 4, 4, 7, 8);
            insertRows("(50, 'a', 'v50', 50.5, '2024-01-01T03:00:00.000000Z'), (5, 'a', 'v5', 5.5, '2024-01-01T03:00:00.000000Z'), "
                    + "(0, 'a', 'v0', 0.5, '2024-01-01T03:00:00.000000Z')");
            assertDayOrder(1, 2, 3, 4, 5, 6, 0, 50, 7, 8);
        });
    }

    @Test
    public void testDedupUpsertTieMatchesNative() throws Exception {
        // DEDUP UPSERT KEYS(ts, v): the coalesced merge puts the new keys 0 and 50 after the stored rows at 03:00
        Assume.assumeTrue(isWal);
        setProperties();
        assertMemoryLeak(() -> {
            createTables(" DEDUP UPSERT KEYS(ts, v)", 1, 2, 3, 4, 4, 4, 7, 8);
            insertRows("(50, 'a', 'v50', 50.5, '2024-01-01T03:00:00.000000Z'), (0, 'a', 'v0', 0.5, '2024-01-01T03:00:00.000000Z')");
            assertDayOrder(1, 2, 3, 4, 5, 6, 0, 50, 7, 8);
        });
    }

    @Test
    public void testNonDedupTieAcrossThreeRowGroups() throws Exception {
        // row groups 00,01,02,03 | 03,03,03,03 | 03,05,06,07 (v 1..12): the row at 02:30 merges into row
        // group 0, the rows at 03:00 go after the nine stored rows at 03:00, into row group 2
        setProperties();
        assertMemoryLeak(() -> {
            createTables("", 1, 2, 3, 4, 4, 4, 4, 4, 4, 6, 7, 8);
            insertRows("(50, 'a', 'v50', 50.5, '2024-01-01T02:30:00.000000Z'), (51, 'a', 'v51', 51.5, '2024-01-01T03:00:00.000000Z'), "
                    + "(52, 'a', 'v52', 52.5, '2024-01-01T03:00:00.000000Z'), (53, 'a', 'v53', 53.5, '2024-01-01T04:00:00.000000Z')");
            assertDayOrder(1, 2, 3, 50, 4, 5, 6, 7, 8, 9, 51, 52, 53, 10, 11, 12);
        });
    }

    @Test
    public void testNonDedupTieAcrossTwoRowGroups() throws Exception {
        // row groups 00,01,02,03 | 03,03,06,07 (v 1..8): the row at 03:00 goes after rows 4, 5 and 6
        setProperties();
        assertMemoryLeak(() -> {
            createTables("", 1, 2, 3, 4, 4, 4, 7, 8);
            insertRows("(50, 'a', 'v50', 50.5, '2024-01-01T03:00:00.000000Z')");
            assertTwoRowGroupTieOrder();

            // the order survives a reopen of the table
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertTwoRowGroupTieOrder();

            // and the conversion back to native
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '" + DAY + "'");
            drainWalQueue();
            assertTwoRowGroupTieOrder();
        });
    }

    @Test
    public void testNonDedupTieAtPartitionMax() throws Exception {
        // row groups 00,01,02,03 | 03,03 (v 1..6): the row at 03:00 goes after rows 4, 5 and 6, at the
        // end of the partition
        setProperties();
        assertMemoryLeak(() -> {
            createTables("", 1, 2, 3, 4, 4, 4);
            insertRows("(50, 'a', 'v50', 50.5, '2024-01-01T03:00:00.000000Z')");
            assertDayOrder(1, 2, 3, 4, 5, 6, 50);
        });
    }

    @Test
    public void testNonDedupTieFailedCommitThenRetry() throws Exception {
        // the in-place update fails to open data.parquet for writing: the commit fails and leaves the
        // partition as it was, the retry puts the row at 03:00 after rows 4, 5 and 6
        Assume.assumeFalse(isRewrite);
        setProperties();
        final AtomicBoolean isFailing = new AtomicBoolean();
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isFailing.get() && Utf8s.endsWithAscii(name, TableUtils.PARQUET_PARTITION_NAME)
                        && Utf8s.containsAscii(name, "pt")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            createTables("", 1, 2, 3, 4, 4, 4, 7, 8);
            final String o3Row = "(50, 'a', 'v50', 50.5, '2024-01-01T03:00:00.000000Z')";
            isFailing.set(true);
            if (isWal) {
                execute("INSERT INTO pt (v, s, vc, d, ts) VALUES " + o3Row);
                drainWalQueue();
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
                assertPtDayOrder(1, 2, 3, 4, 5, 6, 7, 8);
                isFailing.set(false);
                execute("ALTER TABLE pt RESUME WAL");
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
            } else {
                assertExceptionNoLeakCheck("INSERT INTO pt (v, s, vc, d, ts) VALUES " + o3Row, -1, "bulk update failed and will be rolled back");
                isFailing.set(false);
                assertPtDayOrder(1, 2, 3, 4, 5, 6, 7, 8);
                execute("INSERT INTO pt (v, s, vc, d, ts) VALUES " + o3Row);
            }
            execute("INSERT INTO tw (v, s, vc, d, ts) VALUES " + o3Row);
            drainWalQueue();
            assertDayOrder(1, 2, 3, 4, 5, 6, 50, 7, 8);
        });
    }

    @Test
    public void testNonDedupTieIndexedSymbol() throws Exception {
        // the shape of testNonDedupTieAcrossTwoRowGroups with an indexed symbol: the index of the merged
        // partition gives the rows of 'a' in the new order
        setProperties();
        assertMemoryLeak(() -> {
            for (String t : tables()) {
                execute("CREATE TABLE " + t + " (v INT, s SYMBOL INDEX, vc VARCHAR, d DOUBLE, ts " + timestampType.getTypeName()
                        + ") TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
            }
            insertStoredRows(1, 2, 3, 4, 4, 4, 7, 8);
            insertRows("(50, 'a', 'v50', 50.5, '2024-01-01T03:00:00.000000Z')");
            assertDayOrder(1, 2, 3, 4, 5, 6, 50, 7, 8);
            for (String t : tables()) {
                assertQuery("SELECT v FROM " + t + " WHERE s = 'a'")
                        .noLeakCheck()
                        .returns("v\n1\n2\n3\n4\n5\n6\n50\n7\n8\n");
                assertQuery("SELECT v FROM " + t + " WHERE ts <= '2024-01-01T03:00:00.000000Z' LATEST ON ts PARTITION BY s")
                        .noLeakCheck()
                        .expectSize()
                        .returns("v\n50\n");
            }
        });
    }

    @Test
    public void testRepeatedTieCommits() throws Exception {
        // a second commit at the tied 03:00 goes after the first one
        setProperties();
        assertMemoryLeak(() -> {
            createTables("", 1, 2, 3, 4, 4, 4, 7, 8);
            insertRows("(50, 'a', 'v50', 50.5, '2024-01-01T03:00:00.000000Z')");
            assertDayOrder(1, 2, 3, 4, 5, 6, 50, 7, 8);
            insertRows("(51, 'a', 'v51', 51.5, '2024-01-01T03:00:00.000000Z')");
            assertDayOrder(1, 2, 3, 4, 5, 6, 50, 51, 7, 8);
        });
    }

    private static String rowsOf(String header, int... vs) {
        final StringBuilder sb = new StringBuilder(header).append('\n');
        for (int v : vs) {
            sb.append(v).append('\t').append('a').append('\t').append('v').append(v).append('\t').append(v).append(".5\n");
        }
        return sb.toString();
    }

    // pt and tw hold the rows of 2024-01-01 in the given order, and in the same order as each other
    private void assertDayOrder(int... vs) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.assertEqualsExactOrder(compiler, sqlExecutionContext, "tw", "pt");
        }
        for (String t : tables()) {
            assertQuery("SELECT v, s, vc, d FROM " + t + " WHERE ts IN '" + DAY + "'")
                    .noLeakCheck()
                    .returns(rowsOf("v\ts\tvc\td", vs));
        }
    }

    private void assertPtDayOrder(int... vs) throws Exception {
        assertQuery("SELECT v, s, vc, d FROM pt WHERE ts IN '" + DAY + "'")
                .noLeakCheck()
                .returns(rowsOf("v\ts\tvc\td", vs));
    }

    private void assertTwoRowGroupTieOrder() throws Exception {
        assertDayOrder(1, 2, 3, 4, 5, 6, 50, 7, 8);
        for (String t : tables()) {
            // the last row at or before 03:00 is the O3 row
            assertQuery("SELECT v FROM " + t + " WHERE ts <= '2024-01-01T03:00:00.000000Z' LATEST ON ts PARTITION BY s")
                    .noLeakCheck()
                    .expectSize()
                    .returns("v\n50\n");
            // in the 03:00 bucket, the first row is row 4 and the last the O3 row
            assertQuery("SELECT first(v) f, last(v) l FROM " + t + " WHERE ts IN '2024-01-01T03' SAMPLE BY 1h")
                    .noLeakCheck()
                    .expectSize()
                    .returns("f\tl\n4\t50\n");
        }
    }

    // pt and tw with the rows v = 1.. of 2024-01-01 at the given hours, and one row on 2024-01-02;
    // 2024-01-01 of pt converted to parquet
    private void createTables(String dedup, int... hours) throws Exception {
        for (String t : tables()) {
            execute("CREATE TABLE " + t + " (v INT, s SYMBOL, vc VARCHAR, d DOUBLE, ts " + timestampType.getTypeName()
                    + ") TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL") + dedup);
        }
        insertStoredRows(hours);
    }

    private void insertRows(String rows) throws Exception {
        for (String t : tables()) {
            execute("INSERT INTO " + t + " (v, s, vc, d, ts) VALUES " + rows);
        }
        drainWalQueue();
    }

    // row v (1-based) at hour hours[v - 1] - 1, s = 'a', vc = 'v<v>', d = v.5; converts 2024-01-01 of pt
    private void insertStoredRows(int... hours) throws Exception {
        final StringBuilder rows = new StringBuilder();
        for (int i = 0; i < hours.length; i++) {
            final int v = i + 1;
            rows.append(i > 0 ? ", " : "").append('(').append(v).append(", 'a', 'v").append(v).append("', ").append(v)
                    .append(".5, '").append(String.format("2024-01-01T%02d:00:00.000000Z", hours[i] - 1)).append("')");
        }
        rows.append(", (99, 'z', 'v99', 99.5, '2024-01-02T00:00:00.000000Z')");
        insertRows(rows.toString());
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '" + DAY + "'");
        drainWalQueue();
    }

    private void setProperties() {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        if (isRewrite) {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, -1);
        } else {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        }
    }

    private String[] tables() {
        return new String[]{"pt", "tw"};
    }
}
