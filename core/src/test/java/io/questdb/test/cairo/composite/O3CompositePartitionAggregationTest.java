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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Regression tests for vectorized (SIMD-batch) aggregation over composite partitions - several
 * pieces over one set of column files, with dead space where a merge-append superseded a piece.
 * <p>
 * Historically tracked as "D6" on an earlier revision of this branch: a
 * vectorized aggregate (e.g. {@code sum(i)}) over a composite partition with dead space could
 * return wrong results or SIGSEGV the JVM, on the theory that the scan sized itself off the
 * physical extent (dead space included) rather than off the live piece boundaries
 * {@code FwdTableReaderPageFrameCursor#computeNativeFrame} computes. Reproducing the exact
 * original fixture against current HEAD no longer shows the bug, so these tests pin the fix: each
 * one cross-checks the vectorized SQL result against a plain row-cursor oracle (the ordinary
 * per-piece frame cursor path {@link O3CompositePartitionTest} already exercises), and asserts the
 * plan actually took the vectorized/async path under test - a silent fallback to the row-by-row
 * path would otherwise pass for the wrong reason.
 */
public class O3CompositePartitionAggregationTest extends AbstractCairoTest {

    @Test
    public void testKeyedVectorGroupBySumOverCompositePartition() throws Exception {
        assertMemoryLeak(() -> {
            enableCompositeFixtureConfig();

            // In-order data, then a narrow batch landing inside the range: three pieces, dead
            // space between piece 0 and piece 1 (see testUnkeyedVectorSumOverThreePieces... below
            // for the geometry this produces).
            execute("create table x as (select cast(x as int) i," +
                    " timestamp_sequence('2024-01-01', 1000000L) ts" +
                    " from long_sequence(20000)) timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            execute("insert into x select cast(x as int) + 500000 i," +
                    " timestamp_sequence('2024-01-01T02:00:00', 1000000L) ts from long_sequence(200)");
            drainWalQueue();

            assertPartitionHasDeadSpace("x", 0);

            final int keyModulo = 4;
            final long[] oracleSums = oracleKeyedSum("select i from x where ts in '2024-01-01'", keyModulo);
            final StringBuilder expected = new StringBuilder("k\ts\n");
            for (int k = 0; k < keyModulo; k++) {
                expected.append(k).append('\t').append(oracleSums[k]).append('\n');
            }

            assertQuery("select i % 4 k, sum(i) s from x where ts in '2024-01-01' order by k")
                    .expectSize()
                    .withPlanContaining("Async Group By")
                    .returns(expected.toString());
        });
    }

    @Test
    public void testUnkeyedVectorSumOverPartitionWithDeadSpaceAboveOneLivePiece() throws Exception {
        assertMemoryLeak(() -> {
            enableCompositeFixtureConfig();

            // One piece, 8 overlapping backdates of the same 400-row stride: each merge-append
            // abandons the previous copy, and the relocated live piece ends up at the tail with a
            // lot of dead space above it. This is the original D6 repro fixture exactly.
            execute("create table x as (select cast(x as int) i," +
                    " timestamp_sequence('2024-01-01', 1000000L) ts" +
                    " from long_sequence(600)) timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            for (int i = 0; i < 8; i++) {
                execute("insert into x select cast(x as int) + 500000 i," +
                        " timestamp_sequence('2024-01-01T06:00:00', 1000000L) ts from long_sequence(400)");
                drainWalQueue();
            }

            assertPartitionHasDeadSpace("x", 0);

            final String expected = "s\n" + oracleSum("select i from x where ts in '2024-01-01'") + "\n";
            assertQuery("select coalesce(sum(i),0) s from x where ts in '2024-01-01'")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Async Group By", "vectorized: true")
                    .returns(expected);
        });
    }

    @Test
    public void testUnkeyedVectorSumOverThreePiecesWithDeadSpaceBetweenFirstTwo() throws Exception {
        assertMemoryLeak(() -> {
            enableCompositeFixtureConfig();

            // In-order data, then a narrow batch landing inside the range: the pre-split cuts the
            // day into [front][back], the overlapping stride merges with the new rows and moves to
            // the tail as its own piece, and the space the stride used to occupy - between front
            // and back - is left dead. Three pieces in timestamp order: front (0), the relocated
            // merge (1), back (2) - dead space sits exactly at the piece 0/piece 1 transition.
            execute("create table x as (select cast(x as int) i," +
                    " timestamp_sequence('2024-01-01', 1000000L) ts" +
                    " from long_sequence(20000)) timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            execute("insert into x select cast(x as int) + 500000 i," +
                    " timestamp_sequence('2024-01-01T02:00:00', 1000000L) ts from long_sequence(200)");
            drainWalQueue();

            assertPartitionHasDeadSpace("x", 0);
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                Assert.assertEquals("fixture must have exactly 3 pieces", 3, reader.getGeometry().getPieceCount(0));
            }

            // Interval scan (ts in 'day' is pushed down as a partition/row-range restriction).
            String expected = "s\n" + oracleSum("select i from x where ts in '2024-01-01'") + "\n";
            assertQuery("select coalesce(sum(i),0) s from x where ts in '2024-01-01'")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Async Group By", "vectorized: true", "Interval forward scan")
                    .returns(expected);

            // Plain frame scan (no WHERE at all) - a different PageFrameCursor code path.
            expected = "s\n" + oracleSum("select i from x") + "\n";
            assertQuery("select coalesce(sum(i),0) s from x")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Async Group By", "vectorized: true", "Frame forward scan")
                    .returns(expected);
        });
    }

    private static void assertPartitionHasDeadSpace(String table, int partitionIndex) {
        final TableToken tt = engine.verifyTableName(table);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final PartitionGeometry geometry = reader.getGeometry();
            Assert.assertTrue("fixture is not composite - test would be vacuous",
                    txReader.isPartitionComposite(partitionIndex));
            Assert.assertTrue("fixture has no dead rows - test would be vacuous",
                    geometry.getE(partitionIndex) > geometry.getLiveRows(partitionIndex));
        }
    }

    private static void enableCompositeFixtureConfig() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);
    }

    /** Per-{@code key = i % keyModulo} sum of {@code i}, read off the ordinary row cursor. */
    private static long[] oracleKeyedSum(String sql, int keyModulo) throws Exception {
        final long[] sums = new long[keyModulo];
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                while (c.hasNext()) {
                    final int v = c.getRecord().getInt(0);
                    sums[v % keyModulo] += v;
                }
            }
        }
        return sums;
    }

    /** Sum of the single projected INT column, read off the ordinary row cursor. */
    private static long oracleSum(String sql) throws Exception {
        long sum = 0;
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                while (c.hasNext()) {
                    sum += c.getRecord().getInt(0);
                }
            }
        }
        return sum;
    }
}
