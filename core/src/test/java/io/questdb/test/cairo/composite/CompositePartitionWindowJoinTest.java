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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The parallel window join reads its slave table through {@link io.questdb.griffin.engine.table.ConcurrentTimeFrameState},
 * which pre-computes page frame boundaries from metadata without opening partitions and then, on first
 * access, opens the partition and asserts the real frame count matches. A COMPOSITE partition is several
 * pieces over one set of column files and yields one frame per piece, so a pre-computation that does not
 * split at piece boundaries undercounts and the query dies with "frame count mismatch for partition N".
 */
public class CompositePartitionWindowJoinTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // Small frames, so a partition holds several of them and a miscounted one is reachable.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 4);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 8);
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
    }

    @Test
    public void testWindowJoinOverCompositePartition() throws Exception {
        assertMemoryLeak(() -> {
            // Day one carries the composite partition; the small later day keeps it out of the active
            // slot, so the cut below goes through the O3 composite path.
            final String dayOne = "SELECT ('sym' || (x % 3))::symbol sym, x::DOUBLE price," +
                    " timestamp_sequence('2025-01-01', 60*1000000L) ts FROM long_sequence(240)";
            final String dayTwo = "SELECT ('sym' || (x % 3))::symbol sym, (x + 1000)::DOUBLE price," +
                    " timestamp_sequence('2025-01-03', 60*1000000L) ts FROM long_sequence(20)";

            execute("CREATE TABLE trades AS (" + dayOne + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO trades " + dayTwo);
            drainWalQueue();

            makeFirstPartitionComposite();

            final TableToken tt = engine.verifyTableName("trades");
            try (TableReader reader = engine.getReader(tt)) {
                Assert.assertTrue("2025-01-01 should be composite", reader.getTxFile().isPartitionComposite(0));
                Assert.assertTrue("2025-01-01 should have more than one piece", reader.getGeometry().getPieceCount(0) > 1);
            }

            // Same rows, never split - the oracle the composite table has to agree with.
            execute("CREATE TABLE oracle AS (SELECT sym, price, ts FROM (" + dayOne + " UNION ALL " + dayTwo +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext,
                    windowJoin("oracle"),
                    windowJoin("trades"),
                    LOG
            );
        });
    }

    /**
     * Cuts the middle of 2025-01-01 out with a REPLACE RANGE commit carrying no new rows and inserts the
     * same rows straight back, which leaves the partition composite without changing what it holds.
     */
    private void makeFirstPartitionComposite() throws Exception {
        final String rangeLoIso = "2025-01-01T01:00:00.000000Z";
        final String rangeHiIso = "2025-01-01T03:00:00.000000Z";
        execute("CREATE TABLE stash AS (SELECT * FROM trades WHERE ts >= '" + rangeLoIso +
                "' AND ts < '" + rangeHiIso + "')");
        final TableToken tt = engine.verifyTableName("trades");
        try (WalWriter walWriter = engine.getWalWriter(tt)) {
            walWriter.commitWithParams(
                    MicrosTimestampDriver.floor(rangeLoIso),
                    MicrosTimestampDriver.floor(rangeHiIso),
                    WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
            );
        }
        drainWalQueue();
        execute("INSERT INTO trades SELECT * FROM stash");
        drainWalQueue();
        execute("DROP TABLE stash");
    }

    private static String windowJoin(String table) {
        return "SELECT t.ts, t.sym::varchar sym, t.price, sum(w.price) total, count(w.price) cnt" +
                " FROM " + table + " t" +
                " WINDOW JOIN " + table + " w ON (t.sym = w.sym)" +
                " RANGE BETWEEN 5 minutes PRECEDING AND 1 microseconds PRECEDING EXCLUDE PREVAILING" +
                " ORDER BY t.ts, t.sym";
    }
}
