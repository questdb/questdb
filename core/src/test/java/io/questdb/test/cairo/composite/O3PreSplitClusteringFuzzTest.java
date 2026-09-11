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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionCompactionPolicy;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Fuzzes the pre-split's clustering rules, described in
 * {@code core/src/main/java/io/questdb/cairo/PRE_SPLIT_BUCKETING.md}.
 * <p>
 * A table of a few days - some dense, some nearly empty, some with the rows evenly spread and some with them
 * bunched into blocks - takes a handful of WAL commits applied as ONE block. A commit spreads its rows evenly
 * over a random range, or drops them in a few tight bursts, or ties them to timestamps that already exist, or
 * lands on a partition's first and last rows, or appends past the end of the table. Most reach across several
 * partitions, so no partition sees the batch's own outer edges and what comes out is the clustering's work.
 * <p>
 * The piece limit is scaled to the shape of the run rather than fixed: a partition of tens of thousands of
 * rows against a commit of hundreds stands in for a production partition of tens of millions against a commit
 * of thousands, and the run is placed on either side of the break-even where a cut spares exactly what it
 * costs. Both sides are legal, and the checks hold for both.
 * <p>
 * What the run then checks, per partition:
 * <ul>
 *     <li>the rows read back match a plain non-WAL table that took the same inserts - a non-WAL table never
 *     founds a composite partition, so it is an independent answer;</li>
 *     <li>pieces claim disjoint file rows and the stored rows ascend by timestamp;</li>
 *     <li>the piece count stays inside the budget the compaction policy would rewrite the partition over,
 *     {@link PartitionCompactionPolicy#effectiveMaxPieces} - a cut has to spare more rows than the piece it
 *     costs, so cutting can never on its own drive the partition past that number;</li>
 *     <li>every piece either holds at least the piece floor of rows, or covers rows this run committed - the
 *     pre-split spares whole pieces, so the only thin pieces are the ones the incoming rows founded;</li>
 *     <li>on an evenly spread day, where the planner's uniform-density estimate is exact, the partition ends
 *     with at least as many pieces as the commits have clusters - they were cut around, not merged through.</li>
 * </ul>
 */
public class O3PreSplitClusteringFuzzTest extends AbstractCairoTest {

    private static final long DAY_0 = MicrosTimestampDriver.floor("2020-02-03T00:00:00.000000Z");
    private static final long DAY_MICROS = 86_400_000_000L;

    @Test
    public void testBlockApplyLeavesALegalGeometry() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int dayCount = 3 + rnd.nextInt(3);
            final int denseRows = 40_000 + rnd.nextInt(60_000);
            final int commitRows = 20 + rnd.nextInt(400);

            // Break-even is where the existing rows a cut spares equal the piece it costs, 4x the limit. The
            // shift puts the run on either side of it; both sides are a legal outcome the checks hold for.
            final int breakEven = Math.max(1, denseRows / commitRows / 4);
            final int avgRowsPieceLim = Math.max(4, Math.min(4096, (breakEven << rnd.nextInt(5)) >> 2));
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, avgRowsPieceLim);
            // The floor below which a cut is not worth the piece it makes. Twice it is what two incoming rows
            // must have between them, in existing rows, before they belong to different clusters.
            final long minPieceRows = 2L * avgRowsPieceLim;
            LOG.info().$("fuzz shape [days=").$(dayCount).$(", denseRows=").$(denseRows)
                    .$(", commitRows=").$(commitRows).$(", avgRowsPieceLim=").$(avgRowsPieceLim).I$();

            execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE o (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE b (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE c (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            // The first and last days are dense: a dense MID partition is what the pre-split has to cut, and a
            // partition that is neither first nor last is never the one being appended to. An evenly spread
            // day makes the planner's density estimate exact; a bunched one makes it wrong, which is what the
            // resolved cuts have to survive.
            final boolean[] isEven = new boolean[dayCount];
            final long[] dayRows = new long[dayCount];
            final long[] dayStep = new long[dayCount];
            for (int d = 0; d < dayCount; d++) {
                final boolean dense = d == 0 || d == dayCount - 1 || rnd.nextInt(4) == 0;
                final long rows = dense ? denseRows : 50 + rnd.nextInt(500);
                final long dayStart = DAY_0 + d * DAY_MICROS;
                isEven[d] = !dense || rnd.nextInt(3) > 0;
                dayRows[d] = rows;
                dayStep[d] = Math.max(1, DAY_MICROS / rows);
                if (isEven[d]) {
                    insertBase(d, "timestamp_sequence(" + dayStart + ", " + dayStep[d] + ") ts", rows);
                } else {
                    // Three blocks, each a twentieth of the day wide, with the rest of the day empty.
                    final long blockGap = DAY_MICROS * 45 / 100;
                    final long tight = Math.max(1, DAY_MICROS / 20 / Math.max(1, rows / 3));
                    insertBase(d, "(" + dayStart + " + ((x - 1) % 3) * " + blockGap
                            + " + ((x - 1) / 3) * " + tight + ")::TIMESTAMP ts", rows);
                }
                drainWalQueue();
            }

            for (int commit = 0, commits = 1 + rnd.nextInt(3); commit < commits; commit++) {
                final int base = 500_000 * (commit + 1);
                final int fromDay = rnd.nextInt(dayCount - 1);
                final int toDay = fromDay + 1 + rnd.nextInt(dayCount - fromDay - 1);
                final long lo = DAY_0 + fromDay * DAY_MICROS + rnd.nextLong(DAY_MICROS / 2);
                final long hi = DAY_0 + (long) (toDay + 1) * DAY_MICROS - 1 - rnd.nextLong(DAY_MICROS / 2);
                switch (rnd.nextInt(6)) {
                    case 0: {
                        // Evenly over a multi-day range: the shape with no slack at either edge.
                        final long step = Math.max(1, (hi - lo) / commitRows);
                        insertCommit(base, "timestamp_sequence(" + lo + ", " + step + ") ts", commitRows);
                        break;
                    }
                    case 1: {
                        // A few tight bursts over the same range, so most of it is spared by two cuts each.
                        final int clusters = 2 + rnd.nextInt(4);
                        final long gap = Math.max(1, (hi - lo) / clusters);
                        insertCommit(base, "(" + lo + " + ((x - 1) % " + clusters + ") * " + gap
                                + " + ((x - 1) / " + clusters + ") * 1000)::TIMESTAMP ts", commitRows);
                        break;
                    }
                    case 2: {
                        // Confined to one partition, so the batch's own outer edges are there to cut at.
                        final long step = Math.max(1, (DAY_MICROS / 4) / commitRows);
                        insertCommit(base, "timestamp_sequence(" + lo + ", " + step + ") ts", commitRows);
                        break;
                    }
                    case 3: {
                        // Every row ties with one that already exists. Without dedup, pieces may touch, so a
                        // tying row still founds its own piece rather than forcing a merge.
                        final int d = rnd.nextInt(dayCount);
                        final long rows = Math.min(commitRows, dayRows[d]);
                        final long stride = Math.max(1, dayRows[d] / rows) * dayStep[d];
                        insertCommit(base, "timestamp_sequence(" + (DAY_0 + d * DAY_MICROS) + ", " + stride + ") ts", rows);
                        break;
                    }
                    case 4: {
                        // Past the end of the table: a plain append onto the tail piece.
                        final long tail = DAY_0 + (long) dayCount * DAY_MICROS - DAY_MICROS / 8;
                        insertCommit(base, "timestamp_sequence(" + tail + ", 1000) ts", commitRows);
                        break;
                    }
                    default: {
                        // On a partition's very first and very last rows, where there is nothing below the one
                        // and nothing above the other to spare.
                        final int d = rnd.nextInt(dayCount);
                        final long dayStart = DAY_0 + d * DAY_MICROS;
                        final long last = dayStart + (dayRows[d] - 1) * dayStep[d];
                        insertCommit(base, "(CASE WHEN x % 2 = 0 THEN " + dayStart + " + x ELSE "
                                + last + " - x END)::TIMESTAMP ts", commitRows);
                        break;
                    }
                }
            }
            drainWalQueue();

            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT * FROM o ORDER BY ts, i",
                    "SELECT * FROM x ORDER BY ts, i",
                    LOG
            );
            assertRowsInTimestampOrder();
            assertGeometryFollowsTheRules(minPieceRows, isEven);
        });
    }

    /**
     * Every piece of every partition, against the rules the pre-split promises: the partition stays inside the
     * piece budget, a piece is thin only when this run's own rows founded it, and - where the density estimate
     * the planner works from is exact - a commit's clusters were each cut around rather than merged through.
     */
    private static void assertGeometryFollowsTheRules(long minPieceRows, boolean[] isEven) throws Exception {
        final LongList committed = readTimestamps("SELECT ts FROM c ORDER BY ts");
        final LongList existing = readTimestamps("SELECT ts FROM b ORDER BY ts");
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            final PartitionGeometry geometry = reader.getGeometry();
            final TxReader txReader = reader.getTxFile();
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                final int pieces = geometry.getPieceCount(i);
                final long liveRows = txReader.getPartitionSize(i);
                final String where = " [partition=" + i + ", " + describePieces(geometry, i) + ']';

                final int budget = PartitionCompactionPolicy.effectiveMaxPieces(configuration, liveRows);
                Assert.assertTrue(
                        "the pre-split blew the piece budget [pieces=" + pieces + ", budget=" + budget + ']' + where,
                        pieces <= budget
                );

                for (int p = 0; p < pieces; p++) {
                    final long loP = geometry.getPieceRowOffset(i, p);
                    final long hiP = loP + geometry.getPieceRowCount(i, p);
                    for (int q = 0; q < p; q++) {
                        final long loQ = geometry.getPieceRowOffset(i, q);
                        final long hiQ = loQ + geometry.getPieceRowCount(i, q);
                        Assert.assertFalse(
                                "two pieces claim the same file rows [pieceA=" + q + ", pieceB=" + p + ']' + where,
                                loP < hiQ && loQ < hiP
                        );
                    }
                    if (pieces == 1 || geometry.getPieceRowCount(i, p) >= minPieceRows) {
                        continue;
                    }
                    // A cut only ever spares the side it was made for, so what is left on the other side can be
                    // a handful of rows - the row a cluster starts on, or the tail of a partition a cluster
                    // ends against. Both sit next to the commit, so a thin piece is legal exactly where the
                    // commit reached: inside it, or inside a piece it touches.
                    Assert.assertTrue(
                            "a thin piece sits away from the commit, so a cut spared less than it cost"
                                    + " [piece=" + p + ", rows=" + geometry.getPieceRowCount(i, p)
                                    + ", minPieceRows=" + minPieceRows + ']' + where,
                            countInRange(
                                    committed,
                                    geometry.getPieceTimestampLo(i, Math.max(0, p - 1)),
                                    geometry.getPieceTimestampHi(i, Math.min(pieces - 1, p + 1))
                            ) > 0
                    );
                }

                // Each cluster after the first sits behind a stretch of existing rows worth more than the piece
                // a cut costs, so the pre-split has to have carved that stretch out on its own. Only claimed
                // for an evenly spread day, where the planner's uniform-density estimate is the real count.
                final int day = (int) ((txReader.getPartitionTimestampByIndex(i) - DAY_0) / DAY_MICROS);
                if (day < 0 || day >= isEven.length || !isEven[day]) {
                    continue;
                }
                final int clusters = clustersIn(
                        existing,
                        committed,
                        geometry.getPieceTimestampLo(i, 0),
                        geometry.getPieceTimestampHi(i, pieces - 1),
                        4 * minPieceRows
                );
                Assert.assertTrue(
                        "the commit was merged through instead of cut around [clusters=" + clusters
                                + ", pieces=" + pieces + ']' + where,
                        pieces >= clusters
                );
            }
        }
    }

    /**
     * A table scan reads pieces in {@code tsLo} order and each piece from its own row offset, so the
     * timestamps it produces must ascend. Every comparison above goes through {@code ORDER BY}, which sorts a
     * wrong storage order into the right answer and hides it; this reads the rows as stored.
     */
    private static void assertRowsInTimestampOrder() throws Exception {
        try (
                RecordCursorFactory factory = select("SELECT ts FROM x");
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            long previous = Long.MIN_VALUE;
            long row = 0;
            while (cursor.hasNext()) {
                final long ts = record.getTimestamp(0);
                Assert.assertTrue(
                        "rows are stored out of timestamp order [row=" + row + ", previous=" + previous
                                + ", ts=" + ts + ']',
                        ts >= previous
                );
                previous = ts;
                row++;
            }
        }
    }

    /**
     * The clusters the rules ask for, counted against the REAL rows rather than the uniform-density estimate
     * the planner works from: two committed rows belong to different clusters when the existing rows between
     * them outweigh the piece a cut costs. {@code minGapRows} is deliberately larger than the number the
     * planner uses, so a rounding difference cannot make this lower bound flaky.
     */
    private static int clustersIn(LongList existing, LongList committed, long tsLo, long tsHi, long minGapRows) {
        final int from = firstAtOrAbove(committed, tsLo);
        final int to = firstAtOrAbove(committed, tsHi + 1);
        if (from >= to) {
            return 0;
        }
        int clusters = 1;
        long previous = committed.getQuick(from);
        for (int i = from + 1; i < to; i++) {
            final long ts = committed.getQuick(i);
            if (firstAtOrAbove(existing, ts) - firstAtOrAbove(existing, previous + 1) >= minGapRows) {
                clusters++;
            }
            previous = ts;
        }
        return clusters;
    }

    private static long countInRange(LongList sorted, long tsLo, long tsHi) {
        if (tsHi == Numbers.LONG_NULL) {
            return 1;
        }
        return firstAtOrAbove(sorted, tsHi + 1) - firstAtOrAbove(sorted, tsLo);
    }

    private static String describePieces(PartitionGeometry geometry, int partitionIndex) {
        final StringBuilder sink = new StringBuilder("pieces=[");
        for (int p = 0, n = geometry.getPieceCount(partitionIndex); p < n; p++) {
            if (p > 0) {
                sink.append(", ");
            }
            sink.append(p).append(":[")
                    .append(geometry.getPieceTimestampLo(partitionIndex, p)).append("..")
                    .append(geometry.getPieceTimestampHi(partitionIndex, p)).append("]@")
                    .append(geometry.getPieceRowOffset(partitionIndex, p)).append('+')
                    .append(geometry.getPieceRowCount(partitionIndex, p));
        }
        return sink.append("] E=").append(geometry.getE(partitionIndex)).toString();
    }

    private static int firstAtOrAbove(LongList sorted, long value) {
        int lo = 0;
        int hi = sorted.size();
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (sorted.getQuick(mid) < value) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * Feeds the day's rows to the table under test, to the plain non-WAL oracle, and to the record of what was
     * there before the commits - which is what tells a spared piece from one the commits founded.
     */
    private static void insertBase(int day, String tsExpr, long rows) throws Exception {
        insertInto("x,o,b", "SELECT (" + (day * 1_000_000) + " + x)::INT i, " + tsExpr
                + " FROM long_sequence(" + rows + ")");
    }

    private static void insertCommit(int base, String tsExpr, long rows) throws Exception {
        insertInto("x,o,c", "SELECT (" + base + " + x)::INT i, " + tsExpr
                + " FROM long_sequence(" + rows + ")");
    }

    private static void insertInto(String tables, String rows) throws Exception {
        for (String table : tables.split(",")) {
            execute("INSERT INTO " + table + ' ' + rows);
        }
    }

    private static LongList readTimestamps(String query) throws Exception {
        final LongList list = new LongList();
        try (
                RecordCursorFactory factory = select(query);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                list.add(record.getTimestamp(0));
            }
        }
        return list;
    }
}
