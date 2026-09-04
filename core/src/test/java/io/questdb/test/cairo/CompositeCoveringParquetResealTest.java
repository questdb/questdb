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

package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A COVERING posting index resealed on a COMPOSITE day's parquet cell.
 * <p>
 * When O3 rewrites an already-parquet partition, the worker builds only the non-covering {@code .pv};
 * {@code resealParquetCoveringForPartition} rebuilds the covering sidecars from the new parquet before
 * the commit exposes it. That method refused a routed composite table outright, because every lookup
 * in it answered for cellKey 0 -- the partition record, the directory, the row count and the covered
 * columns' tops and name txns.
 * <p>
 * This is the one covering path whose covered column tops are read UN-NORMALIZED (the parquet re-encode
 * forces them to zero, which is why an enterprise re-encode test cannot distinguish a cellKey-0 read).
 * So the fixture makes the tops differ per cell -- E0 holds three pre-column rows, E1 one -- and the
 * oracle is the value the covering index returns for the SECOND cell.
 */
public class CompositeCoveringParquetResealTest extends AbstractCairoTest {

    /**
     * The NATIVE counterpart, and the one that pins the covered-column TOPS.
     * <p>
     * {@code ALTER COLUMN ... ADD INDEX} rebuilds each existing partition through
     * {@code indexNativePartition}, which resolves the cell for its own column top and size but
     * configured the COVERING columns at cellKey 0. A native partition keeps genuinely per-cell tops
     * (a parquet one carries zeros after conversion, which is why no parquet-backed test can see this),
     * so a sibling cell's covering was sealed over cell 0's covered data.
     * <p>
     * E0 holds three rows before the covered column exists and E1 one, so their tops differ (3 vs 1).
     * The oracle is the covered VALUE the index returns for each cell.
     */
    @Test
    public void testNativeCoveringIndexBuildReadsEachCellsOwnCoveredTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (ts TIMESTAMP, exch SYMBOL, sym SYMBOL) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("INSERT INTO n VALUES ('2023-01-01T01:00:00.000000Z','E0','A'),"
                    + "('2023-01-01T01:10:00.000000Z','E0','A'),"
                    + "('2023-01-01T01:20:00.000000Z','E0','B'),"
                    + "('2023-01-01T02:00:00.000000Z','E1','A'),"
                    + "('2023-01-02T01:00:00.000000Z','E0','A')");
            drainWalQueue();
            execute("ALTER TABLE n ADD COLUMN val INT");
            execute("INSERT INTO n VALUES ('2023-01-01T03:00:00.000000Z','E0','C',7),"
                    + "('2023-01-01T04:00:00.000000Z','E1','D',42)");
            drainWalQueue();
            // Built AFTER the rows exist, so every partition is indexed through indexNativePartition --
            // the path whose covering configuration was cell-blind.
            execute("ALTER TABLE n ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
            drainWalQueue();

            assertQuery("SELECT ts, sym, val FROM n WHERE sym = 'D'")
                    .noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tsym\tval\n2023-01-01T04:00:00.000000Z\tD\t42\n");
            assertQuery("SELECT ts, sym, val FROM n WHERE sym = 'C'")
                    .noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tsym\tval\n2023-01-01T03:00:00.000000Z\tC\t7\n");
            // The pre-column rows still read NULL through the index, per cell.
            assertQuery("SELECT ts, sym, val FROM n WHERE sym = 'B'")
                    .noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tsym\tval\n2023-01-01T01:20:00.000000Z\tB\tnull\n");
        });
    }

    @Test
    public void testCoveringResealOnASiblingCellReadsItsOwnCoveredValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, sym SYMBOL) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            // Different pre-existing row counts per cell: E0 three, E1 one. That is what makes the
            // covered column's top differ between the two cells once it is added below.
            execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','E0','A'),"
                    + "('2023-01-01T01:10:00.000000Z','E0','A'),"
                    + "('2023-01-01T01:20:00.000000Z','E0','B'),"
                    + "('2023-01-01T02:00:00.000000Z','E1','A'),"
                    + "('2023-01-02T01:00:00.000000Z','E0','A')");
            drainWalQueue();
            execute("ALTER TABLE c ADD COLUMN val INT");
            execute("ALTER TABLE c ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
            execute("INSERT INTO c VALUES ('2023-01-01T03:00:00.000000Z','E0','C',7),"
                    + "('2023-01-01T04:00:00.000000Z','E1','D',42)");
            drainWalQueue();

            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            drainWalQueue();

            // O3 into the SECOND cell rewrites its parquet, and the rewrite is what drives the
            // covering reseal for that cell.
            execute("INSERT INTO c VALUES ('2023-01-01T02:30:00.000000Z','E1','D',99)");
            drainWalQueue();

            // The covering index must answer with E1's own covered values, not E0's.
            assertQuery("SELECT ts, sym, val FROM c WHERE sym = 'D' ORDER BY ts")
                    .noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tsym\tval\n"
                            + "2023-01-01T02:30:00.000000Z\tD\t99\n"
                            + "2023-01-01T04:00:00.000000Z\tD\t42\n");
            // The first cell's own covered value is equally its own.
            assertQuery("SELECT ts, sym, val FROM c WHERE sym = 'C'")
                    .noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tsym\tval\n2023-01-01T03:00:00.000000Z\tC\t7\n");
            // Rows that predate the covered column read NULL, per cell.
            assertQuery("SELECT count() FROM c").noLeakCheck().noRandomAccess().expectSize().returns("count\n8\n");
        });
    }
}
