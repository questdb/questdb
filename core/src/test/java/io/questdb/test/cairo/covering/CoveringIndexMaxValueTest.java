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

package io.questdb.test.cairo.covering;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A sealed covering-index sidecar block whose covered values are ALL the column
 * type's maximum -- {@code Integer.MAX_VALUE} for INT / IPv4, {@code Long.MAX_VALUE}
 * for LONG / TIMESTAMP / DATE -- used to read back as 0 (NULL for IPv4). The
 * Frame-of-Reference writer seeded its running minimum with the type maximum and
 * then reused that same constant as an "I saw no values" sentinel, resetting the
 * stored base to 0; the span of an all-max stride is 0, so no packed payload is
 * written and both reader paths reproduce the clobbered base for every row.
 * <p>
 * Every test here pins a sealed block. The active partition never seals its
 * sidecar, so each table carries a later partition that pushes the all-max
 * partition out of the active slot.
 */
public class CoveringIndexMaxValueTest extends AbstractCairoTest {

    private static final String COLUMNS = "v_ipv4, v_int, v_long, v_ts, v_date, v_short, v_byte";
    /**
     * Every column {@link #COLUMNS} names, in that order, at its type maximum.
     */
    private static final String MAX_VALUE_TUPLE = "'127.255.255.255'"
            + ", " + Integer.MAX_VALUE
            + ", " + Long.MAX_VALUE
            + ", " + Long.MAX_VALUE + "::TIMESTAMP"
            + ", " + Long.MAX_VALUE + "::DATE"
            + ", " + Short.MAX_VALUE
            + ", " + Byte.MAX_VALUE;

    @Test
    public void testCoveredMaxValuesBulkDecodePath() throws Exception {
        // The reader point-reads the first row of a block and bulk-decodes the
        // block on the second access (AbstractPostingIndexReader.ensureColumnDecoded).
        // Three all-max rows for one key in one sealed partition therefore walk
        // readIntAt/readLongAt once and decompressIntsToAddr/decompressLongsToAddr
        // for the rest, so both surfaces of the clobbered base are covered.
        assertMemoryLeak(() -> {
            createTable("t_max_bulk", "POSTING", "WAL");
            insertMaxRows("t_max_bulk", 3);
            drainWalQueue();
            assertCoveredMaxValues("t_max_bulk", 3);
        });
    }

    @Test
    public void testCoveredMaxValuesMixedBlockStillCorrect() throws Exception {
        // Negative control: a block that mixes the maximum with a smaller value
        // has a non-zero span, so it never hit the sentinel reset and already
        // round-tripped. Its bytes must not change.
        assertMemoryLeak(() -> {
            createTable("t_max_mixed", "POSTING", "WAL");
            execute("""
                    INSERT INTO t_max_mixed VALUES
                        ('a', %s, '2024-01-01T00:00:00.000000Z'),
                        ('a', '10.0.0.1', 7, 7, 7::TIMESTAMP, 7::DATE, 7, 7,
                         '2024-01-01T00:00:01.000000Z'),
                        ('b', '10.0.0.2', 1, 1, 1::TIMESTAMP, 1::DATE, 1, 1,
                         '2024-01-02T00:00:00.000000Z')
                    """.formatted(MAX_VALUE_TUPLE));
            drainWalQueue();
            assertQuery("SELECT " + COLUMNS + " FROM t_max_mixed WHERE sym = 'a'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            v_ipv4\tv_int\tv_long\tv_ts\tv_date\tv_short\tv_byte
                            127.255.255.255\t2147483647\t9223372036854775807\t294247-01-10T04:00:54.775807Z\t292278994-08-17T07:12:55.807Z\t32767\t127
                            10.0.0.1\t7\t7\t1970-01-01T00:00:00.000007Z\t1970-01-01T00:00:00.007Z\t7\t7
                            """);
        });
    }

    @Test
    public void testCoveredMaxValuesNonWalSealed() throws Exception {
        // A non-WAL partition only seals its sidecar once its generation count
        // exceeds cairo.posting.seal.gen.threshold (default 16), which
        // TableWriter.switchPartition checks via
        // PostingIndexWriter.sealIfMultiGen. One commit per row therefore has to
        // clear that threshold before the compressed all-max block exists at
        // all; below it the reader serves the raw, uncompressed sidecar and the
        // values are already correct (see
        // testCoveredMaxValuesNonWalUnsealedReadsRawSidecar). This runs at the
        // default threshold on purpose: non-WAL tables are reachable in
        // production, they just need more commits.
        assertMemoryLeak(() -> {
            createTable("t_max_nowal_sealed", "POSTING", "BYPASS WAL");
            final int maxRowCount = 20;
            for (int i = 0; i < maxRowCount; i++) {
                execute("INSERT INTO t_max_nowal_sealed VALUES"
                        + " ('a', " + MAX_VALUE_TUPLE + ", " + rowTimestamp(i) + ")");
            }
            // Crosses the partition boundary, which is what runs the seal.
            execute("INSERT INTO t_max_nowal_sealed VALUES"
                    + " ('b', '10.0.0.1', 1, 1, 1::TIMESTAMP, 1::DATE, 1, 1,"
                    + " '2024-01-02T00:00:00.000000Z')");
            engine.releaseAllWriters();
            assertCoveredMaxValues("t_max_nowal_sealed", maxRowCount);
        });
    }

    @Test
    public void testCoveredMaxValuesNonWalUnsealedReadsRawSidecar() throws Exception {
        // Negative control and scope statement: below the seal threshold the
        // covering reader takes AbstractPostingIndexReader's !isCurrentGenDense
        // branch and reads raw sidecar values, so no FoR base is involved and
        // the all-max row is correct even on the unfixed writer.
        assertMemoryLeak(() -> {
            createTable("t_max_nowal", "POSTING", "BYPASS WAL");
            insertMaxRows("t_max_nowal", 1);
            engine.releaseAllWriters();
            assertCoveredMaxValues("t_max_nowal", 1);
        });
    }

    @Test
    public void testCoveredMaxValuesParquet() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t_max_parquet", "POSTING", "WAL");
            insertMaxRows("t_max_parquet", 2);
            drainWalQueue();
            execute("ALTER TABLE t_max_parquet CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            assertCoveredMaxValues("t_max_parquet", 2);
        });
    }

    @Test
    public void testCoveredMaxValuesPostingDelta() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t_max_delta", "POSTING DELTA", "WAL");
            insertMaxRows("t_max_delta", 2);
            drainWalQueue();
            assertCoveredMaxValues("t_max_delta", 2);
        });
    }

    @Test
    public void testCoveredMaxValuesPostingEf() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t_max_ef", "POSTING EF", "WAL");
            insertMaxRows("t_max_ef", 2);
            drainWalQueue();
            assertCoveredMaxValues("t_max_ef", 2);
        });
    }

    @Test
    public void testCoveredMaxValuesWal() throws Exception {
        // One all-max row for key 'a' in a sealed partition: the covering read
        // resolves through the point-read path (readIntAt / readLongAt).
        assertMemoryLeak(() -> {
            createTable("t_max_wal", "POSTING", "WAL");
            insertMaxRows("t_max_wal", 1);
            drainWalQueue();
            assertCoveredMaxValues("t_max_wal", 1);
        });
    }

    private static void createTable(String name, String indexKind, String walClause) throws SqlException {
        execute("CREATE TABLE " + name + " ("
                + " sym SYMBOL INDEX TYPE " + indexKind + " INCLUDE (" + COLUMNS + "),"
                + " v_ipv4 IPv4,"
                + " v_int INT,"
                + " v_long LONG,"
                + " v_ts TIMESTAMP,"
                + " v_date DATE,"
                + " v_short SHORT,"
                + " v_byte BYTE,"
                + " ts TIMESTAMP"
                + ") TIMESTAMP(ts) PARTITION BY DAY " + walClause);
    }

    /**
     * Writes {@code maxRowCount} rows for key {@code 'a'} into 2024-01-01, every
     * covered column at its type maximum, plus one ordinary row for key
     * {@code 'b'} in 2024-01-02 so that 2024-01-01 leaves the active-partition
     * slot and its sidecar gets sealed.
     */
    private static void insertMaxRows(String name, int maxRowCount) throws SqlException {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(name).append(" VALUES");
        for (int i = 0; i < maxRowCount; i++) {
            sql.append(i == 0 ? "\n" : ",\n")
                    .append("    ('a', ").append(MAX_VALUE_TUPLE)
                    .append(", ").append(rowTimestamp(i)).append(")");
        }
        sql.append(",\n    ('b', '10.0.0.1', 1, 1, 1::TIMESTAMP, 1::DATE, 1, 1,")
                .append(" '2024-01-02T00:00:00.000000Z')");
        execute(sql);
    }

    /**
     * Renders row {@code i} of a 2024-01-01 batch as a timestamp literal, placing the row
     * {@code i} seconds past midnight. Carrying the index through the minute and hour fields,
     * instead of zero-padding the seconds field alone, keeps the literal well formed across the
     * whole range that can still land in the 2024-01-01 partition; padding the seconds alone
     * mints an invalid literal ("00:00:60") from row 60 onwards.
     */
    private static String rowTimestamp(int i) {
        Assert.assertTrue("row " + i + " falls outside the 2024-01-01 partition", i >= 0 && i < 86_400);
        return String.format("'2024-01-01T%02d:%02d:%02d.000000Z'", i / 3_600, i / 60 % 60, i % 60);
    }

    private void assertCoveredMaxValues(String table, int maxRowCount) throws Exception {
        StringBuilder expected = new StringBuilder("v_ipv4\tv_int\tv_long\tv_short\tv_byte\n");
        for (int i = 0; i < maxRowCount; i++) {
            expected.append("127.255.255.255\t2147483647\t9223372036854775807\t32767\t127\n");
        }
        assertQuery("SELECT v_ipv4, v_int, v_long, v_short, v_byte FROM " + table + " WHERE sym = 'a'")
                .noRandomAccess()
                .expectSize()
                .noLeakCheck()
                .withPlanContaining("CoveringIndex")
                .returns(expected);

        StringBuilder expectedTemporal = new StringBuilder("ts_long\tdate_long\n");
        for (int i = 0; i < maxRowCount; i++) {
            expectedTemporal.append("9223372036854775807\t9223372036854775807\n");
        }
        assertQuery("SELECT v_ts::long AS ts_long, v_date::long AS date_long FROM " + table + " WHERE sym = 'a'")
                .noRandomAccess()
                .expectSize()
                .noLeakCheck()
                .withPlanContaining("CoveringIndex")
                .returns(expectedTemporal);
    }
}
