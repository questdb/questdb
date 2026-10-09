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
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Two pieces may TOUCH - the dedup-free tie rule founds a piece at the very timestamp the piece below it ends on -
 * but the geometry still orders pieces by tsLo and refuses two founded at one timestamp. A pre-split cut that lands
 * on that shared timestamp would carve the lower piece's tie tail into a single-point piece starting exactly where
 * the next piece starts, and the commit died on {@code pieces must ascend by tsLo}.
 */
public class CompositeTouchingPieceCutTest extends AbstractCairoTest {

    @Test
    public void testClusterCutOnTouchingTimestampLeavesTieTailInPlace() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        // minPieceRows = 2 x this. Every piece below stays under 2 x minPieceRows, so the batch-driven pre-split
        // never cuts one: only the clusterer, which cuts at minute boundaries whatever the piece's size, can.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 1);
        assertMemoryLeak(() -> {
            final String ddl = " (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY";
            execute("CREATE TABLE x" + ddl + " WAL");
            execute("CREATE TABLE ref" + ddl + " BYPASS WAL");

            // 00:00, two rows at 00:10, three at 00:40. A later day so 2024-01-01 is never the active partition.
            both("SELECT '2024-01-01T00:00:00'::TIMESTAMP ts, 1 v FROM long_sequence(1)" +
                    " UNION ALL SELECT '2024-01-01T00:10:00'::TIMESTAMP ts, 10 + x::INT v FROM long_sequence(2)" +
                    " UNION ALL SELECT '2024-01-01T00:40:00'::TIMESTAMP ts, 40 + x::INT v FROM long_sequence(3)" +
                    " UNION ALL SELECT '2024-01-03T00:00:00'::TIMESTAMP ts, 0 v FROM long_sequence(1)");

            // 00:30 is cold on both sides: the clusterer cuts the day at 00:30, and the row founds a piece of its own.
            both("SELECT '2024-01-01T00:30:00'::TIMESTAMP ts, 30 v FROM long_sequence(1)");
            Assert.assertEquals(
                    "pieces=[0:[00:00:00..00:10:00]@0+3, 1:[00:30:00..00:30:00]@6+1, 2:[00:40:00..00:40:00]@3+3] E=7",
                    describeDay("x")
            );

            // Rows at the first piece's last timestamp: spared by the tie rule, they merge into the piece above and
            // leave it TOUCHING the first piece at 00:10.
            both("SELECT '2024-01-01T00:10:00'::TIMESTAMP ts, 100 + x::INT v FROM long_sequence(2)" +
                    " UNION ALL SELECT '2024-01-01T00:20:00'::TIMESTAMP ts, 20 v FROM long_sequence(1)");
            Assert.assertEquals(
                    "pieces=[0:[00:00:00..00:10:00]@0+3, 1:[00:10:00..00:30:00]@7+4, 2:[00:40:00..00:40:00]@3+3] E=11",
                    describeDay("x")
            );

            // One block of two commits whose ranges leave 00:01..00:10 cold: the clusterer cuts at 00:01 and at
            // 00:10, and both resolve to the first piece's rows at 00:10 - the tie tail the piece above starts on.
            execute("INSERT INTO x SELECT '2024-01-01T00:00:30'::TIMESTAMP ts, 2 v FROM long_sequence(1)");
            execute("INSERT INTO x SELECT '2024-01-01T00:10:30'::TIMESTAMP ts, 11 v FROM long_sequence(1)");
            drainWalQueue();
            execute("INSERT INTO ref SELECT '2024-01-01T00:00:30'::TIMESTAMP ts, 2 v FROM long_sequence(1)");
            execute("INSERT INTO ref SELECT '2024-01-01T00:10:30'::TIMESTAMP ts, 11 v FROM long_sequence(1)");

            Assert.assertFalse("the cut on the touching timestamp suspended the table",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            // The tie tail stayed with its piece. The cut at 00:11 on the piece above went ahead as usual.
            Assert.assertEquals(
                    "pieces=[0:[00:00:00..00:10:00]@11+4, 1:[00:10:00..00:10:00]@7+2, 2:[00:10:30..00:10:30]@15+1," +
                            " 3:[00:20:00..00:30:00]@9+2, 4:[00:40:00..00:40:00]@3+3] E=16",
                    describeDay("x")
            );
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "x", LOG);
        });
    }

    private static void both(String select) throws Exception {
        execute("INSERT INTO x " + select);
        execute("INSERT INTO ref " + select);
        drainWalQueue();
    }

    private static String describeDay(String tableName) throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            final PartitionGeometry geometry = reader.getGeometry();
            final int partitionIndex = reader.getTxFile().getPartitionIndex(MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z"));
            final StringBuilder sink = new StringBuilder("pieces=[");
            for (int p = 0, n = geometry.getPieceCount(partitionIndex); p < n; p++) {
                if (p > 0) {
                    sink.append(", ");
                }
                sink.append(p).append(":[").append(hhmm(geometry.getPieceTimestampLo(partitionIndex, p))).append("..")
                        .append(hhmm(geometry.getPieceTimestampHi(partitionIndex, p))).append("]@")
                        .append(geometry.getPieceRowOffset(partitionIndex, p)).append('+')
                        .append(geometry.getPieceRowCount(partitionIndex, p));
            }
            return sink.append("] E=").append(geometry.getE(partitionIndex)).toString();
        }
    }

    private static String hhmm(long micros) {
        final long seconds = micros / 1_000_000L % (24 * 3600);
        return String.format("%02d:%02d:%02d", seconds / 3600, seconds / 60 % 60, seconds % 60);
    }
}
