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
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * DEDUP UPSERT or SKIP commits into a parquet partition of {@code pt} and the same commits into its native twin
 * {@code tw}: identical replays of stored rows, changed values and new rows, over a BOOLEAN, BYTE or SHORT column
 * {@code b} and an INT column {@code i} added after some rows of the partition, and a SHORT column {@code s} added
 * after more of them. The partition is converted while it is the last one, so the parquet file records the column
 * tops, and the row groups hold their NULLs when the UPSERT identity check meets them: the output keeps a top that ends
 * before the first O3 row, as native does, and the first identical MERGE then holds the rows of the kept top. The tops
 * end inside row group 0, so a merge that ends a top rewrites every row group that holds top rows. SKIP keeps the
 * stored row of a changed value, and its tables add the columns before the first row: a SKIP commit whose dedup drops
 * every O3 row of a MERGE that holds the NULLs of a top of b or s, which native turns into 0/false when a later O3 row
 * merges, leaves them NULL in the parquet file, a known issue. Each iteration draws the dead-bytes rewrite gate: a
 * rewrite on every commit, none, or the server defaults, under which a commit that resends stored rows rewrites only
 * when the row groups it re-encodes pass the gate. pt matches tw after each commit, and after a lazy ALTER of b and s,
 * which reads their NULLs apart from 0.
 */
public class O3ParquetUpsertReplayFuzzTest extends AbstractCairoTest {
    private static final String[] NO_SENTINEL_TYPES = {"BOOLEAN", "BYTE", "SHORT"};
    // the minutes between two rows of the partition
    private static final int ROW_MINUTES = 10;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // the native twin merges as production does by default, see O3ParquetNoSentinelColumnTopTest.setUp()
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
    }

    @Test
    public void testSkipReplaysMatchNative() throws Exception {
        assertReplaysMatchNative(true);
    }

    @Test
    public void testUpsertReplaysMatchNative() throws Exception {
        assertReplaysMatchNative(false);
    }

    // Adds an O3 row at the minute of the stored row, or between it and the next one for a new row, unless the commit
    // holds a row at that minute already. REPLAY resends the stored row, CHANGE gives it a new v and, under UPSERT,
    // keeps the change: SKIP keeps the stored row
    private static void addO3Row(List<O3Row> o3Rows, List<StoredRow> rows, int row, O3Kind kind, boolean isSkip) {
        final StoredRow stored = rows.get(row);
        final int minute = kind == O3Kind.NEW ? stored.minute + ROW_MINUTES / 2 : stored.minute;
        for (O3Row o3Row : o3Rows) {
            if (o3Row.row.minute == minute) {
                return;
            }
        }
        final StoredRow o3Row = switch (kind) {
            case REPLAY -> stored;
            case CHANGE -> {
                final StoredRow changed = new StoredRow(stored.v + 1000, stored.minute, stored.b, stored.i, stored.s);
                if (!isSkip) {
                    rows.set(row, changed);
                }
                yield changed;
            }
            case NEW -> new StoredRow(5000 + row, minute, "NULL", Integer.toString(row), Integer.toString(row));
        };
        o3Rows.add(new O3Row(o3Row, kind));
    }

    private static boolean hasKind(List<O3Row> o3Rows, O3Kind kind) {
        for (O3Row o3Row : o3Rows) {
            if (o3Row.kind == kind) {
                return true;
            }
        }
        return false;
    }

    // Whether the first commit of an iteration of a SKIP table holds a MERGE whose O3 rows the dedup all drops and an
    // action that writes. The partition holds the row groups of rowGroupSize rows that the conversion wrote, so the row
    // group of an O3 row follows from its row: a replayed or changed row lands in the MERGE of the row group of its
    // row, a new row, which goes between its row and the next one, in that row group too, or, after the last row of a
    // row group, in the gap after it, which a small next row group takes. Each new row writes.
    private static boolean isSkipResendWithWrite(List<O3Row> o3Rows, int rowGroupSize, int rowGroupCount) {
        final boolean[] hasDuplicate = new boolean[rowGroupCount];
        final boolean[] hasNewRow = new boolean[rowGroupCount];
        for (O3Row o3Row : o3Rows) {
            final int row = o3Row.row.minute / ROW_MINUTES;
            final int rowGroup = row / rowGroupSize;
            if (o3Row.kind == O3Kind.NEW) {
                hasNewRow[rowGroup] = true;
                if (row % rowGroupSize == rowGroupSize - 1 && rowGroup + 1 < rowGroupCount) {
                    hasNewRow[rowGroup + 1] = true;
                }
            } else {
                hasDuplicate[rowGroup] = true;
            }
        }
        if (!hasKind(o3Rows, O3Kind.NEW)) {
            return false;
        }
        for (int rowGroup = 0; rowGroup < rowGroupCount; rowGroup++) {
            if (hasDuplicate[rowGroup] && !hasNewRow[rowGroup]) {
                return true;
            }
        }
        return false;
    }

    private static String literalOf(String type, int value) {
        return switch (type) {
            case "BOOLEAN" -> value % 2 == 0 ? "true" : "false";
            case "BYTE" -> "(" + (value % 100) + ")::BYTE";
            default -> "(" + (value * 7 % 30_000) + ")::SHORT";
        };
    }

    private static String tuples(List<StoredRow> rows, int lo, int hi, int columnCount) {
        final StringBuilder sql = new StringBuilder();
        for (int row = lo; row < hi; row++) {
            sql.append(row > lo ? ", " : "").append(rows.get(row).tuple(columnCount));
        }
        return sql.toString();
    }

    // Runs the iterations of the dedup mode, each under a dead-bytes gate that it draws, and asserts that they made
    // the shapes of ShapeCounts
    private void assertReplaysMatchNative(boolean isSkip) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int iterationCount = Integer.getInteger("questdb.fuzz.parquet.upsertReplay.iterations", 30);
        final ShapeCounts shapeCounts = new ShapeCounts();
        final boolean isShapeRequired = iterationCount >= 10;
        // the run goes on past iterationCount, up to 200 iterations, until it makes every shape it needs
        for (
                int iteration = 0;
                iteration < iterationCount || (isShapeRequired && !shapeCounts.isComplete(isSkip) && iteration < 200);
                iteration++
        ) {
            final int rowGroupSize = 4 + rnd.nextInt(5);
            final boolean isStatistics = rnd.nextInt(5) > 0;
            // 0: a rewrite on every commit, 1: no rewrite, 2 and 3: the server defaults
            final int gate = rnd.nextInt(4);
            final boolean isDefaultGate = gate > 1;
            LOG.info().$("dedup replay fuzz settings [iteration=").$(iteration).$(", skip=").$(isSkip)
                    .$(", statistics=").$(isStatistics).$(", gate=").$(gate).I$();
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, rowGroupSize);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_STATISTICS_ENABLED, isStatistics);
            if (isDefaultGate) {
                node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, (String) null);
                node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, (String) null);
            } else {
                node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
                // -1 rewrites the partition on every commit
                node1.setProperty(
                        PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES,
                        gate == 0 ? -1 : Long.MAX_VALUE
                );
            }
            assertMemoryLeak(() -> runIteration(rnd, rowGroupSize, isStatistics, isSkip, isDefaultGate, shapeCounts));
        }
        LOG.info().$("dedup replay fuzz [skip=").$(isSkip).$(", keptTopReplays=").$(shapeCounts.keptTopReplays)
                .$(", upsertResends=").$(shapeCounts.upsertResends).$(", skipResends=").$(shapeCounts.skipResends).I$();
        if (isShapeRequired) {
            if (isSkip) {
                Assert.assertTrue("no SKIP commit under the default gate resent rows with a write", shapeCounts.skipResends > 0);
            } else {
                Assert.assertTrue("no commit replayed a row past the kept tops", shapeCounts.keptTopReplays > 0);
                Assert.assertTrue("no UPSERT commit under the default gate resent rows with a change", shapeCounts.upsertResends > 0);
            }
        }
    }

    // Runs the commits of one iteration, and counts the commits of the shapes of ShapeCounts
    private void runIteration(
            Rnd rnd,
            int rowGroupSize,
            boolean isStatistics,
            boolean isSkip,
            boolean isDefaultGate,
            ShapeCounts shapeCounts
    ) throws Exception {
        final int rowCount = 2 * rowGroupSize + rnd.nextInt(3 * rowGroupSize);
        final int rowGroupCount = (rowCount + rowGroupSize - 1) / rowGroupSize;
        // SKIP tables have no column tops, see the class comment
        final int topB = isSkip ? 0 : rnd.nextInt(rowGroupSize);
        final int topS = isSkip ? 0 : topB + rnd.nextInt(rowGroupSize - topB);
        final String type = NO_SENTINEL_TYPES[rnd.nextInt(NO_SENTINEL_TYPES.length)];
        LOG.info().$("dedup replay fuzz iteration [rowGroupSize=").$(rowGroupSize).$(", rowCount=").$(rowCount)
                .$(", topB=").$(topB).$(", topS=").$(topS).$(", type=").$(type).I$();
        // the stored rows by row index, the rows below a top hold NULL in its columns
        final List<StoredRow> rows = new ArrayList<>();
        for (int row = 0; row < rowCount; row++) {
            rows.add(new StoredRow(
                    row + 1,
                    row * ROW_MINUTES,
                    row < topB ? "NULL" : literalOf(type, row),
                    row < topB ? "NULL" : Integer.toString(row * 3),
                    row < topS ? "NULL" : Integer.toString(-row)
            ));
        }
        for (String t : new String[]{"pt", "tw"}) {
            execute("CREATE TABLE " + t + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP " + (isSkip ? "SKIP" : "UPSERT") + " KEYS(ts)");
            if (topB > 0) {
                execute("INSERT INTO " + t + " (v, ts) VALUES " + tuples(rows, 0, topB, 2));
            }
            execute("ALTER TABLE " + t + " ADD COLUMN b " + type);
            execute("ALTER TABLE " + t + " ADD COLUMN i INT");
            if (topS > topB) {
                execute("INSERT INTO " + t + " (v, ts, b, i) VALUES " + tuples(rows, topB, topS, 4));
            }
            execute("ALTER TABLE " + t + " ADD COLUMN s SHORT");
            execute("INSERT INTO " + t + " (v, ts, b, i, s) VALUES " + tuples(rows, topS, rowCount, 5));
        }
        drainWalQueue();
        // converted while it is the last partition, the parquet file records the column tops
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        for (String t : new String[]{"pt", "tw"}) {
            execute("INSERT INTO " + t + " (v, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
        }
        drainWalQueue();
        for (int commit = 0, n = 1 + rnd.nextInt(4); commit < n; commit++) {
            final List<O3Row> o3Rows = new ArrayList<>();
            if (!isSkip && commit == 0 && topS > 0 && topS + 1 < rowGroupSize && rnd.nextBoolean()) {
                // a replay of a row of row group 0 past the tops and a change in a later row group
                addO3Row(o3Rows, rows, topS + 1 + rnd.nextInt(rowGroupSize - topS - 1), O3Kind.REPLAY, false);
                addO3Row(o3Rows, rows, rowGroupSize + rnd.nextInt(rowCount - rowGroupSize), O3Kind.CHANGE, false);
            }
            if (isSkip && commit == 0 && rnd.nextBoolean()) {
                // replays of rows of one row group and a new row inside the partition or past its max
                final int rowLo = rnd.nextInt(rowGroupCount) * rowGroupSize;
                final int rowHi = Math.min(rowLo + rowGroupSize, rowCount);
                for (int k = 0, m = 1 + rnd.nextInt(3); k < m; k++) {
                    addO3Row(o3Rows, rows, rowLo + rnd.nextInt(rowHi - rowLo), O3Kind.REPLAY, true);
                }
                addO3Row(o3Rows, rows, rnd.nextBoolean() ? rowCount - 1 : rnd.nextInt(rowCount - 1), O3Kind.NEW, true);
            }
            for (int k = 0, m = rnd.nextInt(4); k < m; k++) {
                addO3Row(o3Rows, rows, rnd.nextInt(rowCount), O3Kind.REPLAY, isSkip);
            }
            for (int k = 0, m = rnd.nextInt(3); k < m; k++) {
                addO3Row(o3Rows, rows, rnd.nextInt(rowCount), O3Kind.CHANGE, isSkip);
            }
            for (int k = 0, m = rnd.nextInt(2); k < m; k++) {
                addO3Row(o3Rows, rows, rnd.nextInt(rowCount), O3Kind.NEW, isSkip);
            }
            if (o3Rows.isEmpty()) {
                addO3Row(o3Rows, rows, rnd.nextInt(rowCount), O3Kind.CHANGE, isSkip);
            }
            O3Row first = o3Rows.get(0);
            for (O3Row o3Row : o3Rows) {
                if (o3Row.row.minute < first.row.minute) {
                    first = o3Row;
                }
            }
            boolean isChanging = false;
            for (O3Row o3Row : o3Rows) {
                isChanging |= o3Row != first && o3Row.kind != O3Kind.REPLAY;
            }
            final int firstRow = first.row.minute / ROW_MINUTES;
            if (!isSkip && first.kind == O3Kind.REPLAY && topS > 0 && firstRow > topS && firstRow < rowGroupSize && isChanging) {
                shapeCounts.keptTopReplays++;
            }
            if (isDefaultGate) {
                if (isSkip) {
                    if (commit == 0 && isSkipResendWithWrite(o3Rows, rowGroupSize, rowGroupCount)) {
                        shapeCounts.skipResends++;
                    }
                } else if (hasKind(o3Rows, O3Kind.REPLAY) && (hasKind(o3Rows, O3Kind.NEW) || hasKind(o3Rows, O3Kind.CHANGE))) {
                    shapeCounts.upsertResends++;
                }
            }
            final StringBuilder values = new StringBuilder();
            for (O3Row o3Row : o3Rows) {
                values.append(values.length() > 0 ? ", " : "").append(o3Row.row.tuple(5));
            }
            for (String t : new String[]{"pt", "tw"}) {
                execute("INSERT INTO " + t + " (v, ts, b, i, s) VALUES " + values);
            }
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
            assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
        }
        // A lazy ALTER of a column without NULL to a type with NULL reads the NULLs of its top through the chunk null
        // counts: a chunk that statistics off left without one reads them as 0
        if (isStatistics) {
            for (String t : new String[]{"pt", "tw"}) {
                execute("ALTER TABLE " + t + " ALTER COLUMN b TYPE VARCHAR");
                execute("ALTER TABLE " + t + " ALTER COLUMN s TYPE INT");
            }
            drainWalQueue();
            assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
        }
        for (String t : new String[]{"pt", "tw"}) {
            execute("DROP TABLE " + t);
        }
        drainWalQueue();
    }

    private enum O3Kind {
        CHANGE, NEW, REPLAY
    }

    // the commits of the shapes that the iterations of a dedup mode must make, see assertReplaysMatchNative()
    private static class ShapeCounts {
        // UPSERT commits whose first O3 row replays a stored row of row group 0 past the tops, with a later O3 row that
        // changes the partition: their first MERGE holds the NULLs of the kept tops
        int keptTopReplays;
        // SKIP commits under the server-default gate with a MERGE whose O3 rows the dedup all drops and an action that
        // writes: the gate counts the row groups of the action that opens the output alone
        int skipResends;
        // UPSERT commits under the server-default gate that replay a stored row and bring a new or changed one: the gate
        // counts the row groups that the commit re-encodes, not the replayed ones the identity check leaves as stored
        int upsertResends;

        boolean isComplete(boolean isSkip) {
            return isSkip ? skipResends > 0 : keptTopReplays > 0 && upsertResends > 0;
        }
    }

    private record O3Row(StoredRow row, O3Kind kind) {
    }

    // a row of the partition at minute after 2024-01-01, with the SQL literals of b, i and s
    private record StoredRow(int v, int minute, String b, String i, String s) {
        // the VALUES tuple of the first columnCount columns of (v, ts, b, i, s)
        String tuple(int columnCount) {
            final StringBuilder sb = new StringBuilder("(").append(v).append(", '2024-01-01'::TIMESTAMP + ")
                    .append(minute).append(" * 60_000_000L");
            final String[] rest = {b, i, s};
            for (int c = 0; c < columnCount - 2; c++) {
                sb.append(", ").append(rest[c]);
            }
            return sb.append(')').toString();
        }
    }
}
