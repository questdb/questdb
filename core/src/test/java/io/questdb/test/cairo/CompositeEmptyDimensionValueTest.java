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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * An EMPTY-STRING dimension value renders as an EMPTY path component.
 * <p>
 * {@code TableUtils#putCellSegmentPathSafe} decides the NULL token from the ORDINAL
 * ({@code SymbolTable.VALUE_IS_NULL} → {@code %NULL}) and throws on an ordinal that does not resolve.
 * An empty string is neither: it is a perfectly valid, resolvable symbol value, so it falls through to
 * {@code putPathSafe(sink, "")}, which emits nothing.
 * <p>
 * WHY THIS MATTERS, and how it was found. Enabling O3 in the composite differential fuzz made the
 * previously-unattributed `DROP PARTITION` failure reproducible, and its logs carried purge paths with
 * an empty component -- {@code /2023-01-02//SKEWLATE}, {@code /2023-01-02/0//SKEWLATE} -- alongside
 * ENOENT purge failures and readers failing to open a cell directory that {@code _txn} still
 * references. A filesystem collapses {@code //} to {@code /}, so two DISTINCT cells can name the SAME
 * directory, which is exactly the shape that would leave one cell's {@code _txn} entry pointing at a
 * directory another cell's drop removed.
 * <p>
 * This test does NOT claim to prove that causal chain -- see
 * {@code CompositeFuzzRunner}'s DROP PARTITION javadoc, which records the link as explicitly NOT
 * established. It isolates and pins the one piece that IS provable on its own: the empty value gets no
 * token of its own, while NULL does.
 * <p>
 * Same family as the already-recorded pre-release item that {@code putPathSafe} lets
 * {@code * ? : | " < >} through into directory names. Both are free to fix while unreleased and an
 * on-disk format break afterwards.
 */
public class CompositeEmptyDimensionValueTest extends AbstractCairoTest {

    /**
     * NULL is the CONTROL. It proves the renderer does have a mechanism for "no ordinary value" and
     * uses it -- so an empty component below is a gap in that mechanism, not an absence of one.
     */
    @Test
    public void testNullDimensionValueGetsItsOwnToken() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cnull (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("INSERT INTO cnull VALUES ('2023-01-01T01:00:00.000000Z', NULL, 1.0)");
            execute("INSERT INTO cnull VALUES ('2023-01-01T02:00:00.000000Z', 'BTC', 2.0)");
            drainWalQueue();

            final List<String> cells = cellDirectoriesOf("cnull", "2023-01-01");
            Assert.assertTrue("a NULL dimension must render a distinguishable token, got " + cells,
                    cells.stream().anyMatch(c -> c.contains("NULL")));
        });
    }

    /**
     * SQL cannot even express the case: {@code INSERT ... ''} into a SYMBOL becomes NULL, so it
     * renders {@code %NULL} and never produces an empty component.
     * <p>
     * Recorded because it is why the first version of this test was VACUOUS. It asserted "two distinct
     * cell directories, neither empty" after an SQL insert of {@code ''} and passed -- not because the
     * renderer handles an empty value, but because there was no empty value to handle. Anything
     * testing this must reach the writer API, as {@link #testEmptyDimensionValueFromWriterApi()} does.
     */
    @Test
    public void testSqlPreservesEmptyStringSoThisIsUserReachable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE csql (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("INSERT INTO csql VALUES ('2023-01-01T01:00:00.000000Z', '', 1.0)");
            execute("INSERT INTO csql VALUES ('2023-01-01T02:00:00.000000Z', 'BTC', 2.0)");
            drainWalQueue();

            // '' is NOT folded to NULL by INSERT -- so no writer-API access is needed to reach this
            final StringSink sink = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext,
                    "SELECT count() FROM csql WHERE exch IS NULL", sink);
            TestUtils.assertContains(sink, "0");

            final List<String> raw = rawCellDirectoriesOf("csql", "2023-01-01");
            Assert.assertEquals("plain SQL must still give the empty and 'BTC' values distinct cell "
                    + "directories; got " + raw, 2, raw.size());
        });
    }

    /**
     * The shape the fuzz actually produced. A row written through {@code TableWriterAPI#putSym("")}
     * does NOT pass through SQL's empty-to-NULL conversion, so the empty string is interned as a real,
     * resolvable symbol value -- which is precisely the case {@code putCellSegmentPathSafe} has no
     * token for.
     * <p>
     * This path is not exotic: ILP writes through the same writer API, so an empty tag value on a
     * dimension column reaches it without any fuzz involved.
     */
    @Test
    public void testEmptyDimensionValueFromWriterApi() throws Exception {
        assertMemoryLeak(() -> {
            // LAYOUT PLAIN deliberately: HIVE mode prefixes each component with "exch=", which keeps
            // an empty value's segment non-empty and hides this entirely. The fuzz randomises the
            // layout, which is why it saw the empty component and a HIVE-only test would not.
            execute("CREATE TABLE cempty (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            final TableToken token = engine.verifyTableName("cempty");
            try (TableWriterAPI w = engine.getTableWriterAPI(token, "empty dimension probe")) {
                TableWriter.Row r = w.newRow(ColumnType.getTimestampDriver(ColumnType.TIMESTAMP).parseFloorLiteral("2023-01-01T01:00:00.000000Z"));
                r.putSym(1, "");
                r.putDouble(2, 1.0);
                r.append();
                r = w.newRow(ColumnType.getTimestampDriver(ColumnType.TIMESTAMP).parseFloorLiteral("2023-01-01T02:00:00.000000Z"));
                r.putSym(1, "BTC");
                r.putDouble(2, 2.0);
                r.append();
                w.commit();
            }
            drainWalQueue();

            final StringSink sink = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM cempty", sink);
            TestUtils.assertContains(sink, "2");

            final List<String> raw = rawCellDirectoriesOf("cempty", "2023-01-01");
            System.out.println("RAW_CELL_DIRS=" + raw);
            for (int i = 0; i < raw.size(); i++) {
                Assert.assertFalse("an empty-string dimension value rendered an EMPTY path component: "
                                + "the cell directory is named " + raw.get(i) + " (all suffix, no value). "
                                + "Raw dirs: " + raw,
                        raw.get(i).startsWith("."));
            }
            Assert.assertEquals("the empty and 'BTC' dimension values must occupy DISTINCT cell "
                            + "directories; got " + raw, 2, raw.size());
        });
    }

    /**
     * Lists the cell-directory names under one day of a composite table, ignoring the {@code .nameTxn}
     * suffix so the assertions above are about the rendered VALUE, not the partition version.
     */
    private List<String> rawCellDirectoriesOf(String table, String day) {
        final List<String> out = new ArrayList<>();
        final File tableDir = new File(root, engine.verifyTableName(table).getDirName());
        final File[] days = tableDir.listFiles();
        if (days == null) {
            return out;
        }
        for (File d : days) {
            if (!d.isDirectory() || !d.getName().startsWith(day)) {
                continue;
            }
            final File[] cells = d.listFiles();
            if (cells == null) {
                continue;
            }
            for (File c : cells) {
                if (c.isDirectory()) {
                    out.add(c.getName());
                }
            }
        }
        out.sort(String::compareTo);
        return out;
    }

    private List<String> cellDirectoriesOf(String table, String day) {
        final List<String> out = new ArrayList<>();
        final File tableDir = new File(root, engine.verifyTableName(table).getDirName());
        final File[] days = tableDir.listFiles();
        if (days == null) {
            return out;
        }
        for (File d : days) {
            final String name = d.getName();
            if (!d.isDirectory() || !name.startsWith(day)) {
                continue;
            }
            final File[] cells = d.listFiles();
            if (cells == null) {
                continue;
            }
            for (File c : cells) {
                if (!c.isDirectory()) {
                    continue;
                }
                final String cell = c.getName();
                final int dot = cell.lastIndexOf('.');
                out.add(dot > 0 ? cell.substring(0, dot) : cell);
            }
        }
        out.sort(String::compareTo);
        return new ArrayList<>(new java.util.LinkedHashSet<>(Arrays.asList(out.toArray(new String[0]))));
    }
}
