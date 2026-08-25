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

package io.questdb.test.cairo.o3;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Not a regression test - measures how much a query's wall time actually depends on how many
 * composite-partition PIECES the scanned range touches, at a fixed total row count. Three tables, one
 * day each, ~1M rows: a plain (never-composite) baseline, one forced into ~25 pieces, one forced into
 * ~2000 pieces - each piece founded by its own out-of-order WAL commit, landing in a distinct,
 * non-adjacent time slot so JOIN (compaction is disabled entirely here) never folds any of them back
 * together. Four query shapes per table: a full-table aggregate that must touch every row's data (not
 * just a row count), a GROUP BY, a narrow range (~1% of the day) and a wide range (~50%).
 */
public class ScratchPieceCountQueryCostTest extends AbstractCairoTest {

    private static final long BASE_START = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
    private static final int HIGH_PIECES = 2000;
    private static final int LOW_PIECES = 25;
    private static final String[] SYMBOLS = {"s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"};
    private static final long TOTAL_ROWS = 1_000_000L;
    private static final long TS_STEP = Micros.DAY_MICROS / TOTAL_ROWS;

    @Test
    public void testQueryCostVsPieceCount() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 0);
            setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);
            setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_TXN_COUNT, -1);
            setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 100_000);
            setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 100_000);
            // Compaction fully disabled: piece count must stay exactly what this test constructs. A
            // divisor of 1 makes the piece-count cap equal to liveRows - unreachable, since a piece needs
            // at least one row.
            setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 1);
            setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, Integer.MAX_VALUE);
            setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, 100);
            setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_STOP_PERCENT, 100);
            setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, Long.MAX_VALUE);

            buildPlainTable("plain_baseline");
            buildScatteredTable("low_pieces", LOW_PIECES);
            buildScatteredTable("high_pieces", HIGH_PIECES);

            System.out.printf(
                    "%n%-16s %8s %12s %12s %14s %14s%n",
                    "table", "pieces", "sum(us)", "groupBy(us)", "narrowRng(us)", "wideRng(us)"
            );
            printRow("plain_baseline");
            printRow("low_pieces");
            printRow("high_pieces");
        });
    }

    /**
     * All four query shapes are aggregates (sum/count/group by), whose result rows are computed as the
     * cursor is driven regardless of whether a caller reads their column values - draining {@code
     * hasNext()} fully is enough to force the real work.
     */
    private static long bestOfMicros(String sql, int iterations) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            long best = Long.MAX_VALUE;
            for (int i = 0; i < iterations; i++) {
                long startNanos = System.nanoTime();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // draining is enough - see method doc
                    }
                }
                best = Math.min(best, System.nanoTime() - startNanos);
            }
            return best / 1000;
        }
    }

    private static int piecesOf(String tableName) {
        engine.releaseAllWriters();
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            return reader.getGeometry().getPieceCount(0);
        }
    }

    private void buildPlainTable(String name) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + name + " SELECT timestamp_sequence(" + BASE_START + ", " + TS_STEP + ")," +
                " rnd_symbol('s0','s1','s2','s3','s4','s5','s6','s7'), rnd_double(), rnd_long(0, 100000, 0)" +
                " FROM long_sequence(" + TOTAL_ROWS + ")");
        drainWalQueue();
    }

    /**
     * {@code pieceCount} equal-width, non-overlapping, gap-free slots tiling the whole day, committed in
     * a SHUFFLED order - each slot is its own WAL commit, landing far from its physical file neighbours
     * in time, so nothing ties or overlaps and every commit founds exactly one new piece. Compaction is
     * disabled for the whole test, so nothing folds them back together afterward either.
     */
    private void buildScatteredTable(String name, int pieceCount) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        final long rowsPerPiece = TOTAL_ROWS / pieceCount;
        final int[] order = new int[pieceCount];
        for (int i = 0; i < pieceCount; i++) {
            order[i] = i;
        }
        final Rnd rnd = new Rnd();
        for (int i = pieceCount - 1; i > 0; i--) {
            final int j = rnd.nextInt(i + 1);
            final int tmp = order[i];
            order[i] = order[j];
            order[j] = tmp;
        }
        final TableToken token = engine.verifyTableName(name);
        final WalWriter w = engine.getWalWriter(token);
        try {
            for (final int slot : order) {
                final long slotStart = BASE_START + slot * rowsPerPiece * TS_STEP;
                for (long i = 0; i < rowsPerPiece; i++) {
                    final TableWriter.Row row = w.newRow(slotStart + i * TS_STEP);
                    row.putSym(1, SYMBOLS[rnd.nextInt(SYMBOLS.length)]);
                    row.putDouble(2, rnd.nextDouble());
                    row.putLong(3, rnd.nextLong(100_000));
                    row.append();
                }
                w.commit();
                // Drained per commit, not once at the end: a batched drain merges the whole backlog as
                // one combined O3 sort, which - since these slots tile the day with no gaps or overlaps -
                // produces one clean, already-sorted result with nothing left to fragment. Composite
                // pieces come from applying separate merges against an ALREADY-SETTLED partition, one
                // relocation at a time.
                drainWalQueue();
            }
        } finally {
            w.close();
        }
    }

    private void printRow(String table) throws Exception {
        final int pieces = piecesOf(table);
        final long fullScanMs = bestOfMicros("SELECT sum(qty) FROM " + table, 20);
        final long groupByMs = bestOfMicros("SELECT sym, count(), sum(qty) FROM " + table + " GROUP BY sym", 20);
        // ~1% of the day, starting a third of the way in.
        final long narrowLo = BASE_START + Micros.DAY_MICROS / 3;
        final long narrowHi = narrowLo + Micros.DAY_MICROS / 100;
        final long narrowMs = bestOfMicros(
                "SELECT sum(qty) FROM " + table + " WHERE ts BETWEEN " + narrowLo + "::timestamp AND " + narrowHi + "::timestamp", 20
        );
        // ~50% of the day.
        final long wideLo = BASE_START + Micros.DAY_MICROS / 4;
        final long wideHi = wideLo + Micros.DAY_MICROS / 2;
        final long wideMs = bestOfMicros(
                "SELECT sum(qty) FROM " + table + " WHERE ts BETWEEN " + wideLo + "::timestamp AND " + wideHi + "::timestamp", 20
        );
        System.out.printf("%-16s %8d %12d %12d %14d %14d%n", table, pieces, fullScanMs, groupByMs, narrowMs, wideMs);
    }
}
