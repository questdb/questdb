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

import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * A composite table's data lives under {@code <day>/<cell>}. NOTHING may ever write to a DAY-level
 * column file.
 * <p>
 * Those day-level {@code ts.d}/{@code exch.d}/{@code px.d} files do exist, as 0-byte artefacts of
 * partition creation, sitting beside the cell directories. That makes them a trap: cell-blind code
 * builds a day path via {@code setStateForTimestamp}, finds a file that opens perfectly well, and
 * writes to it. A non-zero size is therefore a precise signature of a cell-blind write — and cheap to
 * check after any operation.
 * <p>
 * <b>This found a real defect.</b> {@code changeSymbolCapacity}'s trailing reopen grew a day-level
 * {@code exch.d} from 0 bytes to 2 MiB per {@code ALTER ... SYMBOL CAPACITY}, leaving the writer's
 * active handle for the column pointing outside every cell. Rows stayed correct only because the
 * composite write path re-resolves its own per-cell handles, so no data-level assertion anywhere in the
 * suite could see it. File size could.
 * <p>
 * This test generalises that check across the DDL surface, because {@code setStateForTimestamp} has ~15
 * call sites in {@code TableWriter} and the next ungated one would be invisible the same way. Operations
 * that composite refuses are exercised too: a gate must reject BEFORE touching files, so the invariant
 * has to hold for refusals as well as successes.
 */
public class CompositeCellBlindWriteSweepTest extends AbstractCairoTest {

    /**
     * {@code %s} is the table name. Every statement here either succeeds, is refused synchronously, or
     * is refused asynchronously via WAL suspension — the invariant must hold in all three cases, so the
     * outcome is deliberately not asserted per operation (that belongs to the gate-specific tests).
     */
    private static final String[][] OPERATIONS = {
            {"alter symbol capacity", "ALTER TABLE %s ALTER COLUMN exch SYMBOL CAPACITY 1024"},
            {"add column long", "ALTER TABLE %s ADD COLUMN extra LONG"},
            {"add column symbol", "ALTER TABLE %s ADD COLUMN esym SYMBOL"},
            {"drop column", "ALTER TABLE %s DROP COLUMN px"},
            {"rename column", "ALTER TABLE %s RENAME COLUMN px TO px2"},
            {"set ttl", "ALTER TABLE %s SET TTL 30d"},
            {"symbol nocache", "ALTER TABLE %s ALTER COLUMN exch NOCACHE"},
            {"add index", "ALTER TABLE %s ALTER COLUMN exch ADD INDEX"},
            {"dedup enable", "ALTER TABLE %s DEDUP ENABLE UPSERT KEYS(ts, exch)"},
            {"squash partitions", "ALTER TABLE %s SQUASH PARTITIONS"},
            {"drop partition", "ALTER TABLE %s DROP PARTITION LIST '2023-01-01'"},
            {"convert to parquet", "ALTER TABLE %s CONVERT PARTITION TO PARQUET LIST '2023-01-02'"},
            {"vacuum", "VACUUM TABLE %s"},
            {"set param o3MaxLag", "ALTER TABLE %s SET PARAM o3MaxLag = 20s"},
            {"change column type", "ALTER TABLE %s ALTER COLUMN px TYPE FLOAT"},
    };

    @Test
    public void testNoOperationWritesToADayLevelColumnFile() throws Exception {
        for (int i = 0; i < OPERATIONS.length; i++) {
            final String table = "sweep" + i;
            final String label = OPERATIONS[i][0];
            final String sql = String.format(OPERATIONS[i][1], table);
            assertMemoryLeak(() -> {
                execute("CREATE TABLE " + table + " (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                        + " TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
                // Two days, three cells: enough that "the last partition" is one cell among several,
                // which is the state cell-blind code mishandles.
                execute("INSERT INTO " + table + " VALUES"
                        + " ('2023-01-01T01:00:00.000000Z','A',1.0),('2023-01-01T02:00:00.000000Z','B',2.0),"
                        + " ('2023-01-02T01:00:00.000000Z','A',3.0),('2023-01-02T02:00:00.000000Z','B',4.0),"
                        + " ('2023-01-02T03:00:00.000000Z','C',5.0)");
                drainWalQueue();
                assertNoDayLevelData(table, label + " (before)");

                try {
                    execute(sql);
                    drainWalQueue();
                } catch (Throwable refusedSynchronously) {
                    // A refusal is a valid outcome for a composite table; the invariant must hold either
                    // way, which is the whole point of exercising refused operations here.
                }
                assertNoDayLevelData(table, label + " (after)");

                // One further commit, then check again. NOT needed for the known defect -- reverting
                // the changeSymbolCapacity fix fails the "(after)" assertion above on its own, with or
                // without this step (measured both ways). It is here because a different cell-blind
                // site could plausibly leave a mapping that only materialises on a later commit, and
                // one extra INSERT is a cheap way to cover that.
                //
                // A refused operation leaves the table suspended, so the insert simply will not apply
                // there; the assertion below still runs, which is what matters.
                try {
                    execute("INSERT INTO " + table + " VALUES ('2023-01-02T04:00:00.000000Z','A',6.0),"
                            + "('2023-01-02T05:00:00.000000Z','D',7.0)");
                    drainWalQueue();
                } catch (Throwable ignored) {
                    // the operation under test may have left the table unwritable; not this test's concern
                }
                assertNoDayLevelData(table, label + " (after a further commit)");
            });
        }
    }

    /**
     * The sweep above covers the DDL surface, reached through SQL. This covers the one WRITE path that
     * SQL cannot reach: a raw {@code TableWriter} obtained from the engine, appending via
     * {@code newRow}.
     * <p>
     * WHY IT IS NOT COVERED BY THE SWEEP, and why that gap was invisible: composite tables are refused
     * at CREATE unless WAL, so it is tempting to conclude the non-WAL {@code newRow} /
     * {@code ROW_ACTION_SWITCH_PARTITION} path is unreachable for them. That is false. The refusal
     * constrains how rows arrive through SQL, not who may open a writer — {@code getWriter} ->
     * {@code newRow} bypasses the WAL apply path and its composite dispatch entirely and lands in the
     * cell-blind active tail. MEASURED 2026-08-26: an assert placed in {@code switchPartition} claiming
     * composite could never reach it fired 20 times across this suite.
     * <p>
     * The timestamp deliberately falls in a LATER day than any existing row, which is what drives
     * {@code newRow} down the {@code switchPartition} branch rather than a plain append.
     * <p>
     * Uses the same day-level-file-size oracle as the sweep, and for the same reason: the composite
     * write path re-resolves its own per-cell handles, so a row can be queryable and correct while a
     * cell-blind write has still grown a file that belongs to no cell. Row-value assertions cannot see
     * that; file size can.
     */
    @Test
    public void testRawWriterNewRowDoesNotWriteToADayLevelColumnFile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE rawnr (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("INSERT INTO rawnr VALUES ('2023-01-01T01:00:00.000000Z','A',1.0),"
                    + " ('2023-01-01T02:00:00.000000Z','B',2.0),"
                    + " ('2023-01-02T01:00:00.000000Z','C',3.0)");
            drainWalQueue();
            assertNoDayLevelData("rawnr", "raw-writer newRow (before)");

            try (io.questdb.cairo.TableWriter w = getWriter("rawnr")) {
                try {
                    // crosses into a LATER day than any existing row -> ROW_ACTION_SWITCH_PARTITION
                    w.newRow(parseFloorPartialTimestamp("2023-01-03T01:00:00.000000Z"));
                    Assert.fail("a direct-writer partition switch must be refused on a routed composite table");
                } catch (io.questdb.cairo.CairoException expected) {
                    io.questdb.std.str.StringSink m = new io.questdb.std.str.StringSink();
                    m.put(expected.getFlyweightMessage());
                    Assert.assertTrue(
                            "expected the explicit composite refusal, got: " + m,
                            Chars.contains(m, "does not yet support switching the active partition"));
                }
            }
            // The refusal must not have left debris at the day level either.
            assertNoDayLevelData("rawnr", "raw-writer newRow (after refusal)");
        });
    }

    /**
     * Every {@code <table>/<day>/<name>.d} — a column file whose parent is the DAY directory itself
     * rather than a cell directory inside it — must be empty.
     */
    private void assertNoDayLevelData(String table, String stage) throws IOException {
        final Path root = Paths.get(configuration.getDbRoot());
        try (Stream<Path> walk = Files.walk(root, 4)) {
            walk.filter(p -> !Files.isDirectory(p))
                    .filter(p -> p.toString().contains(java.io.File.separator + table + "~"))
                    .filter(p -> {
                        final Path parent = p.getParent();
                        return p.getFileName().toString().endsWith(".d")
                                && parent != null && parent.getFileName().toString().startsWith("2023-");
                    })
                    .forEach(p -> {
                        final long size;
                        try {
                            size = Files.size(p);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        Assert.assertEquals(
                                "cell-blind write after " + stage + ": a composite table's data belongs under"
                                        + " <day>/<cell>, but a DAY-level column file is non-empty: "
                                        + root.relativize(p),
                                0L, size);
                    });
        }
    }
}
