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
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Folding the splits of a table wider than the frame's open-column budget (64). Such a frame keeps none of its
 * columns open, so the column a plan reserves space through is closed again before the append that writes into it
 * reopens the file - the append has to find the reservation on disk rather than in the column it was made through.
 * <p>
 * Every way a split day gets folded with merge-append off: the commit's own squash on a non-WAL table, and
 * {@code SQUASH PARTITIONS} and the compaction sweep on a WAL table. Fixed and var-size columns sit on both sides
 * of the 64th column.
 */
public class WideTableSplitSquashTest extends AbstractCairoTest {
    private static final int EXTRA_COLUMNS = 70;

    @Override
    @Before
    public void setUp() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        // Stands in for a day bigger than the default 50MB split threshold.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        super.setUp();
    }

    @Test
    public void testCommitSquashNonWal() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay("BYPASS WAL");
            Assert.assertEquals("the commit's squash must fold the split day", 2, partitionCount());
            assertRows();
        });
    }

    @Test
    public void testSquashPartitionsWal() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay("WAL");
            Assert.assertEquals("fixture must leave the day split", 3, partitionCount());
            execute("ALTER TABLE w SQUASH PARTITIONS");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("w")));
            Assert.assertEquals(2, partitionCount());
            assertRows();
        });
    }

    @Test
    public void testSweepSquashWal() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay("WAL");
            Assert.assertEquals("fixture must leave the day split", 3, partitionCount());
            final long idleTicks = MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, configuration.getFilesFacade(), () -> idleTicks)) {
                job.run();
            }
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("w")));
            Assert.assertEquals(2, partitionCount());
            assertRows();
        });
    }

    private void assertRows() throws Exception {
        final long base = 5760L * 5761 / 2;
        final long batch = 200L * 201 / 2;
        assertQuery("SELECT count() c, sum(i) si, sum(c0) s0, sum(c64::LONG) s64, sum(c68) s68, sum(c69::LONG) s69 FROM w")
                .noRandomAccess()
                .expectSize()
                .returns("c\tsi\ts0\ts64\ts68\ts69\n"
                        + "6160\t" + (base + 2 * batch + 200L * 90_000 + 200L * 70_000)
                        + '\t' + (base + 2 * batch)
                        + '\t' + (base + 2 * batch)
                        + '\t' + (base + 2 * batch)
                        + '\t' + (base + 2 * batch) + '\n');
        // Every column of a row still holds that row's value, the ones past the 64th included.
        assertQuery("SELECT count() c FROM w WHERE v::LONG <> c0 OR c63 <> c0 OR c64::LONG <> c0 OR c68 <> c0 OR c69::LONG <> c0")
                .noRandomAccess()
                .expectSize()
                .returns("c\n0\n");
    }

    private static void createSplitDay(String walClause) throws Exception {
        // One full day, at 15s a row.
        execute("CREATE TABLE w AS (" + select(0, "timestamp_sequence('2020-01-01', 15 * 1_000_000L)", 5760)
                + ") TIMESTAMP(ts) PARTITION BY DAY " + walClause);
        // A later day, so the first one is no longer the last partition.
        execute("INSERT INTO w " + select(90_000, "timestamp_sequence('2020-01-03', 60 * 1_000_000L)", 200));
        drainWalQueue();
        // O3 into the first day's tail splits it.
        execute("INSERT INTO w " + select(70_000, "timestamp_sequence('2020-01-01T22:00:07', 5 * 1_000_000L)", 200));
        drainWalQueue();
    }

    private static int partitionCount() {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        final TableToken token = engine.verifyTableName("w");
        try (TableReader reader = engine.getReader(token)) {
            return reader.getTxFile().getPartitionCount();
        }
    }

    private static String select(int iBase, String tsExpr, int rows) {
        final StringBuilder sb = new StringBuilder("SELECT x::INT + ").append(iBase).append(" i, x::VARCHAR v, ")
                .append(tsExpr).append(" ts");
        for (int c = 0; c < EXTRA_COLUMNS; c++) {
            // Var-size columns interleaved with fixed ones: a STRING and a VARCHAR every ten columns.
            final String type = switch (c % 10) {
                case 4 -> "STRING";
                case 9 -> "VARCHAR";
                default -> "LONG";
            };
            sb.append(", x::").append(type).append(" c").append(c);
        }
        return sb.append(" FROM long_sequence(").append(rows).append(')').toString();
    }
}
