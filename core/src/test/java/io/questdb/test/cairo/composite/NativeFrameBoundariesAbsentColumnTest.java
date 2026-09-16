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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A column added AFTER a partition went composite has no column version record there, so both the lazy
 * frame prediction ({@link io.questdb.griffin.engine.table.NativeFrameBoundaries}) and the open path
 * ({@code TableReader.reloadColumnAt}) take their "column absent from this partition" branch and invent a
 * top for it. A top is a FILE row: the frame split subtracts the piece shift from it before comparing it
 * against partition rows. The open path invents the partition's mapped extent, which no piece can ever
 * reach past, so it never cuts a frame there. A prediction that invents the LIVE row count instead cuts at
 * {@code liveRows - pieceShift}, a frame boundary the opened partition does not have, whenever that lands
 * strictly inside a relocated piece.
 * <p>
 * The fixture below is built for exactly that: 110 live rows over a single piece shifted by 100, so the
 * live count lands at partition row 10 - inside the piece, hence a predicted cut.
 */
public class NativeFrameBoundariesAbsentColumnTest extends AbstractCairoTest {

    @Before
    public void setUpMergeAppend() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
    }

    @Test
    public void testIndexedAsOfJoinProjectsColumnAddedAfterPartitionWentComposite() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeQuotesWithAddedColumn("q", ", INDEX(s CAPACITY 8)");
            createMasters();

            // A fresh reader has no partition open, so the join takes the lazy prediction path.
            engine.releaseAllReaders();
            assertQuery("SELECT /*+ asof_index(m q) */ sum(q.v) sv, count(q.v) cv, count(q.extra) ce, count() c" +
                    " FROM m ASOF JOIN q ON (s)")
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("AsOf Join Indexed")
                    .returns("sv\tcv\tce\tc\n40000455\t50\t0\t50\n");

            engine.releaseAllReaders();
            assertQuery("SELECT /*+ asof_index(m q) */ m.ts, q.ts qts, q.v, q.extra FROM m ASOF JOIN q ON (s) LIMIT 12")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tqts\tv\textra
                            2024-01-01T01:00:45.000000Z\t2024-01-01T01:00:30.000000Z\t800001\tnull
                            2024-01-01T01:01:45.000000Z\t2024-01-01T01:01:30.000000Z\t800002\tnull
                            2024-01-01T01:02:45.000000Z\t2024-01-01T01:02:30.000000Z\t800003\tnull
                            2024-01-01T01:03:45.000000Z\t2024-01-01T01:03:30.000000Z\t800004\tnull
                            2024-01-01T01:04:45.000000Z\t2024-01-01T01:04:30.000000Z\t800005\tnull
                            2024-01-01T01:05:45.000000Z\t2024-01-01T01:05:30.000000Z\t800006\tnull
                            2024-01-01T01:06:45.000000Z\t2024-01-01T01:06:30.000000Z\t800007\tnull
                            2024-01-01T01:07:45.000000Z\t2024-01-01T01:07:30.000000Z\t800008\tnull
                            2024-01-01T01:08:45.000000Z\t2024-01-01T01:08:30.000000Z\t800009\tnull
                            2024-01-01T01:09:45.000000Z\t2024-01-01T01:09:30.000000Z\t800010\tnull
                            2024-01-01T01:10:45.000000Z\t2024-01-01T01:09:30.000000Z\t800010\tnull
                            2024-01-01T01:11:45.000000Z\t2024-01-01T01:09:30.000000Z\t800010\tnull
                            """);
        });
    }

    @Test
    public void testNonIndexedAsOfJoinProjectsColumnAddedAfterPartitionWentComposite() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeQuotesWithAddedColumn("q", "");
            createMasters();

            // The same fixture with no index on the join key: the fast ASOF cursor reads the slave through
            // the same lazily built time frames.
            engine.releaseAllReaders();
            assertQuery("SELECT sum(q.v) sv, count(q.v) cv, count(q.extra) ce, count() c" +
                    " FROM m ASOF JOIN q ON (s)")
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("AsOf Join Fast")
                    .returns("sv\tcv\tce\tc\n40000455\t50\t0\t50\n");
        });
    }

    @Test
    public void testWindowJoinProjectsColumnAddedAfterPartitionWentComposite() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeQuotesWithAddedColumn("q", ", INDEX(s CAPACITY 8)");
            createMasters();

            // The parallel window join reads its slave through ConcurrentTimeFrameState, which predicts
            // frame boundaries with the same NativeFrameBoundaries. Each of the ten stride rows falls in
            // the five-minute window of five masters, so every one of them is summed exactly five times.
            engine.releaseAllReaders();
            assertQuery("SELECT sum(sv) tsv, sum(cv) tcv, sum(ce) tce, count() c FROM (" + windowJoin() + ")")
                    .expectSize()
                    .noRandomAccess()
                    .returns("tsv\ttcv\ttce\tc\n40000275\t50\t0\t50\n");
        });
    }

    private static String windowJoin() {
        return "SELECT m.ts, sum(q.v) sv, count(q.v) cv, count(q.extra) ce" +
                " FROM m WINDOW JOIN q ON (s)" +
                " RANGE BETWEEN 5 minutes PRECEDING AND 1 microseconds PRECEDING EXCLUDE PREVAILING";
    }

    private static void createMasters() throws Exception {
        // Fifty masters at :45 seconds: the first ten see the stride row a quarter minute below them, the
        // rest see the last stride row.
        execute("""
                CREATE TABLE m AS (
                  SELECT 'kz'::SYMBOL s, timestamp_sequence('2024-01-01T01:00:45', 60_000_000L) ts
                  FROM long_sequence(50)
                ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
        drainWalQueue();
    }

    /**
     * A day of 100 rows a minute apart, then a ten-row backdated stride that merge-append rewrites at the
     * shared files' tail, then a column added once that partition is already composite AND no longer the
     * last one - so 2024-01-01 gets no column version record for it.
     */
    private void createCompositeQuotesWithAddedColumn(String table, String index) throws Exception {
        execute("CREATE TABLE " + table + " AS (" +
                " SELECT x::INT v, ('k' || ((x % 2) + 1))::SYMBOL s," +
                " timestamp_sequence('2024-01-01', 60_000_000L) ts" +
                " FROM long_sequence(100))" + index + " TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + table + " VALUES (90_000, 'k1', '2024-01-03T00:00:00.000000Z')");
        drainWalQueue();
        execute("INSERT INTO " + table + " SELECT x::INT + 800_000, 'kz'," +
                " timestamp_sequence('2024-01-01T01:00:30', 60_000_000L) FROM long_sequence(10)");
        drainWalQueue();
        execute("ALTER TABLE " + table + " ADD COLUMN extra INT");
        drainWalQueue();

        try (TableReader reader = engine.getReader(engine.verifyTableName(table))) {
            Assert.assertTrue("2024-01-01 must be composite", reader.getTxFile().isPartitionComposite(0));
            final PartitionGeometry geometry = reader.getGeometry();
            Assert.assertEquals("fixture must hold one relocated piece", 1, geometry.getPieceCount(0));
            final long liveRows = reader.getTxFile().getPartitionSize(0);
            final long shift = geometry.getPieceShift(0, 0);
            Assert.assertEquals("live rows", 110, liveRows);
            Assert.assertEquals("piece shift", 100, shift);
            Assert.assertEquals("mapped extent", 210, geometry.getLiveFileExtent(0));
            // The divergence condition: the live count, read as a file row, lands strictly inside the piece.
            Assert.assertTrue("live count must fall inside the piece", liveRows - shift > 0 && liveRows - shift < liveRows);
        }
        engine.releaseAllReaders();
    }
}
