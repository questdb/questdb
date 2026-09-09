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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * {@link WalReader} is rebound rather than reallocated: {@code WalSegmentPageFrameCursor}
 * keeps one instance per worker and calls {@link WalReader#of} once per segment, so the
 * reader's notion of "same (table, wal, segment) as last time" decides whether the symbol
 * dictionary is cleared and whether the event walk resumes from a saved byte offset.
 * <p>
 * That identity has to be exact. A rebind the reader wrongly believes is the same segment
 * keeps the previous segment's key to string entries and resumes at an offset computed
 * against a different event file, which surfaces as wrong SYMBOL values in the live view
 * and wrong filter results.
 */
public class WalReaderRebindTest extends AbstractCairoTest {
    // Aliases LOW_SYMBOL_COLUMN modulo 64, so an index that keys membership by the low six
    // bits of the column index - a single-word bit set, or one that never grows - confuses
    // the two columns.
    private static final int HIGH_SYMBOL_COLUMN = 69;
    private static final int LOW_SYMBOL_COLUMN = 5;
    private static final int WIDE_COLUMN_COUNT = 70;

    /**
     * The same-segment fast path folds only newly-appended events instead of clearing and
     * rescanning the whole event history, and it must keep skipping the columns the
     * projection does not name while it does so. A skip that fires only on a full rebuild
     * would fold an unprojected column's diff on every later commit - re-coupling the bind
     * to a clean-band file a concurrent DROP COLUMN deletes - and a skip that forgets to
     * step the event cursor past the skipped diff would shift the folded column's values.
     * The drain re-opens one segment per base commit, so this is the shape production runs
     * most.
     */
    @Test
    public void testIncrementalRebindKeepsSkippingUnprojectedSymbolMap() throws Exception {
        assertMemoryLeak(() -> {
            // Names si (column 3) but not s (column 2), so every commit's diff for s must be
            // skipped and drained on the incremental fold too.
            final IntList projection = new IntList();
            projection.add(0);
            projection.add(1);
            projection.add(3);

            final int commits = 4;
            final DirectString view = new DirectString();
            try (
                    WalWriter walWriter = seedSegmentWithCleanSymbolBand();
                    WalReader reader = new WalReader(configuration)
            ) {
                final TableToken token = engine.verifyTableName("base");
                final String walName = walWriter.getWalName();
                long rowCount = 1;
                reader.of(token, walName, 0, rowCount, projection);
                Assert.assertEquals(1, reader.getSymbolMapFoldedRecords());

                for (int c = 0; c < commits; c++) {
                    TableWriter.Row row = walWriter.newRow(3_000_000L + c);
                    row.putLong(1, 4 + c);
                    row.putSym(2, "s" + c);
                    row.putSym(3, "y" + c);
                    row.append();
                    walWriter.commit();
                    rowCount++;

                    // Same table, wal, segment and projection: the incremental fold.
                    reader.of(token, walName, 0, rowCount, projection);
                    Assert.assertEquals(
                            "an unprojected column's symbol map must stay unfolded on an incremental fold",
                            0,
                            reader.getSymbolCount(2)
                    );
                    // A fresh reader always clears and full-walks, so it is the oracle for
                    // what the incrementally-maintained maps must resolve to.
                    try (WalReader oracle = new WalReader(configuration)) {
                        oracle.of(token, walName, 0, rowCount, projection);
                        Assert.assertEquals(oracle.getSymbolCount(3), reader.getSymbolCount(3));
                        final DirectString oracleView = new DirectString();
                        for (int key = 0, n = oracle.getSymbolCount(3); key < n; key++) {
                            Assert.assertEquals(
                                    "an incremental fold must resolve every key the way a full rebuild does",
                                    oracle.getSymbolValue(3, key, oracleView).toString(),
                                    reader.getSymbolValue(3, key, view).toString()
                            );
                        }
                    }
                    // The clean band the first bind loaded survives every later fold.
                    Assert.assertEquals("x1", reader.getSymbolValue(3, 0, view).toString());
                    Assert.assertEquals("x2", reader.getSymbolValue(3, 1, view).toString());
                }

                // One record per bind, not the 1+2+...+N a per-bind full rebuild would fold:
                // this is what proves the binds above took the incremental path at all.
                Assert.assertEquals(1 + commits, reader.getSymbolMapFoldedRecords());
            }
        });
    }

    /**
     * The mixed shape neither projection test below covers: one symbol column folded while
     * another is skipped AHEAD of it within the same event record. A skipped diff still
     * occupies bytes in the event file, so the skip has to step the shared event cursor past
     * that diff's entries. Leaving the cursor where the skip found it makes the NEXT diff -
     * the one the projection does name - read its clean symbol count and its entries out of
     * the middle of the skipped diff, so the folded column resolves to wrong SYMBOL values
     * rather than failing loudly.
     */
    @Test
    public void testMixedProjectionFoldsSymbolMapAfterASkippedOne() throws Exception {
        assertMemoryLeak(() -> {
            final String walName;
            final TableToken token;
            try (WalWriter walWriter = seedSegmentWithCleanSymbolBand()) {
                token = engine.verifyTableName("base");
                walName = walWriter.getWalName();
            }

            final DirectString view = new DirectString();
            final ObjList<String> unprojected = new ObjList<>();
            try (WalReader reader = new WalReader(configuration)) {
                reader.of(token, walName, 0, 1);
                for (int key = 0, n = reader.getSymbolCount(3); key < n; key++) {
                    unprojected.add(reader.getSymbolValue(3, key, view).toString());
                }
            }
            Assert.assertEquals("[x1,x2,x3]", unprojected.toString());

            // Names si (column 3) but not s (column 2). The writer emits the diffs in column
            // order, so s's diff is skipped immediately ahead of the one that must be folded.
            final IntList projection = new IntList();
            projection.add(0);
            projection.add(1);
            projection.add(3);
            final ObjList<String> projected = new ObjList<>();
            try (WalReader reader = new WalReader(configuration)) {
                reader.of(token, walName, 0, 1, projection);
                Assert.assertEquals(
                        "an unprojected column's symbol map must stay unfolded",
                        0,
                        reader.getSymbolCount(2)
                );
                for (int key = 0, n = reader.getSymbolCount(3); key < n; key++) {
                    projected.add(reader.getSymbolValue(3, key, view).toString());
                }
            }
            Assert.assertEquals(
                    "a diff skipped ahead of a folded one must not shift the folded column's values",
                    unprojected.toString(),
                    projected.toString()
            );
        });
    }

    /**
     * A rebind that NARROWS the projection drops the columns the previous, wider bind
     * folded. Because a changed projection fails the same-segment test, the narrowing bind
     * clears every map and re-folds from the start of the event file, and the refold skips
     * the dropped column - so its map reads EMPTY. Were the narrowing bind to match the
     * same-segment fast path instead, the map the new projection no longer reaches would
     * keep the wider bind's entries and serve STALE keys to a caller that resolves against
     * its own column indexes.
     */
    @Test
    public void testNarrowingRebindClearsSymbolMapItNoLongerProjects() throws Exception {
        assertMemoryLeak(() -> {
            final String walName;
            final TableToken token;
            try (WalWriter walWriter = seedSegmentWithCleanSymbolBand()) {
                token = engine.verifyTableName("base");
                walName = walWriter.getWalName();
            }

            final IntList projection = new IntList();
            projection.add(0);
            projection.add(1);
            projection.add(2);
            projection.add(3);
            final DirectString view = new DirectString();
            try (WalReader reader = new WalReader(configuration)) {
                reader.of(token, walName, 0, 1, projection);
                Assert.assertEquals(3, reader.getSymbolCount(2));
                Assert.assertEquals("ccc", reader.getSymbolValue(2, 2, view).toString());
                Assert.assertEquals(3, reader.getSymbolCount(3));
                Assert.assertEquals("x3", reader.getSymbolValue(3, 2, view).toString());

                projection.clear();
                projection.add(0);
                projection.add(1);
                reader.of(token, walName, 0, 1, projection);
                Assert.assertEquals(
                        "a narrowing rebind must clear the map its new projection no longer reaches",
                        0,
                        reader.getSymbolCount(2)
                );
                Assert.assertEquals(0, reader.getSymbolCount(3));
                Assert.assertEquals(
                        "the narrowed bind must resolve nothing rather than a stale key",
                        SymbolTable.VALUE_NOT_FOUND,
                        reader.getSymbolKey(2, "ccc", 3)
                );
                // The third clause of the same contract: the wider bind resolved this very key to
                // "ccc" above, so a stale map would hand it back here.
                Assert.assertNull(reader.getSymbolValue(2, 2, view));
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.getSymbolKey(3, "x3", 3));
                // The columns the narrow projection does name stay mapped.
                Assert.assertEquals(2_000_000L, reader.getColumn(2).getLong(0));
                Assert.assertEquals(3L, reader.getColumn(4).getLong(0));
            }
        });
    }

    /**
     * A live view that never names a SYMBOL column must survive that column's DROP. The
     * clean-band files a symbol diff resolves against - {@code <wal>/<column>.o} and friends -
     * live at the WAL directory level, and {@code WalWriter.removeSymbolFiles} deletes them as
     * soon as the writer applies the structural change, while segments already written keep
     * emitting diffs for the column. Folding those diffs under a projection that excludes the
     * column therefore faults every later bind with "SymbolMap does not exist", which fails the
     * view's refresh and eventually invalidates it.
     * <p>
     * Both an indexed and an unindexed SYMBOL column are dropped: the writer removes the same
     * four files for either, and the reader opens the same three, so the bind must survive both.
     */
    @Test
    public void testProjectedBindSkipsDroppedUnprojectedSymbolColumn() throws Exception {
        // WalWriter.removeSymbolFiles deletes the clean band with ff.removeQuiet and ignores the
        // result, because removing a hard link whose destination file the writer holds open fails
        // with ACCESS_DENIED on Windows - its own comment says so. Where that delete no-ops the two
        // assertFalse preconditions below go red, and every later assertion would pass for the wrong
        // reason because the bind never meets a deleted file. The same guard sits on the three
        // LiveViewSmokeTest cases that assert this precondition; the one on
        // testRederiveAfterWalLossSurvivesBaseSymbolCapacityDrift carries the full reasoning.
        Assume.assumeFalse("the WAL symbol dictionary delete is best-effort on Windows", Os.isWindows());
        assertMemoryLeak(() -> {
            final String walName;
            final TableToken token;
            try (WalWriter walWriter = seedSegmentWithCleanSymbolBand()) {
                token = engine.verifyTableName("base");
                walName = walWriter.getWalName();
                Assert.assertTrue(
                        "the clean band must be linked into the WAL directory, or the diff carries no clean symbol count",
                        walSymbolOffsetFileExists(token, walName, "s")
                );
                Assert.assertTrue(walSymbolOffsetFileExists(token, walName, "si"));

                execute("ALTER TABLE base DROP COLUMN s");
                execute("ALTER TABLE base DROP COLUMN si");
                // What the writer pool does before it hands a writer out. This is where
                // WalWriter.markColumnRemoved -> removeSymbolFiles deletes the clean band.
                walWriter.goActive();
                Assert.assertFalse(
                        "the writer must delete the WAL-level clean band on DROP COLUMN",
                        walSymbolOffsetFileExists(token, walName, "s")
                );
                Assert.assertFalse(walSymbolOffsetFileExists(token, walName, "si"));
            }

            // The projection a view over SELECT ts, v would compile to: it names neither
            // symbol column.
            final IntList projection = new IntList();
            projection.add(0);
            projection.add(1);
            try (WalReader reader = new WalReader(configuration)) {
                reader.of(token, walName, 0, 1, projection);
                // Two memory slots per column, offset by the leading sentinel pair.
                Assert.assertEquals(2_000_000L, reader.getColumn(2).getLong(0));
                Assert.assertEquals(3L, reader.getColumn(4).getLong(0));
                Assert.assertEquals(
                        "an unprojected column's symbol map must stay unfolded",
                        0,
                        reader.getSymbolCount(2)
                );
                Assert.assertEquals(0, reader.getSymbolCount(3));
            }
        });
    }

    /**
     * A projection can name a column the SEGMENT does not have, and the reader has to drop such an
     * entry rather than map it. {@code WalSegmentPageFrameCursor.of} binds the reader BEFORE the
     * reconcile that compares the projection against the segment's own metadata and throws
     * {@code TableReferenceOutOfDateException}, so an index the segment lacks reaches
     * {@code buildMappedColumns} first. ADD COLUMN does not invalidate a live view, so a segment
     * narrower than the base it lags behind is an ordinary steady state, not a corruption.
     * <p>
     * Were the upper bound to admit {@code columnCount} itself, {@code openSegmentColumns} ->
     * {@code loadColumnAt(columnCount)} -> {@code metadata.getColumnType(columnCount)} would read
     * past the end of the segment's metadata. A negative entry names no column at all and travels
     * the same guard: {@code BitSet} indexes its words array unchecked below zero.
     */
    @Test
    public void testProjectionWithAnOutOfRangeIndexBindsWithThatColumnUnmapped() throws Exception {
        assertMemoryLeak(() -> {
            final String walName;
            final TableToken token;
            try (WalWriter walWriter = seedSegmentWithCleanSymbolBand()) {
                token = engine.verifyTableName("base");
                walName = walWriter.getWalName();
            }

            // base holds four columns, so 4 sits one past the end and -1 below the start. The bind
            // must drop both and still map the two entries the segment does have.
            final IntList projection = new IntList();
            projection.add(0);
            projection.add(1);
            projection.add(-1);
            projection.add(4);
            try (WalReader reader = new WalReader(configuration)) {
                reader.of(token, walName, 0, 1, projection);
                // Two memory slots per column, offset by the leading sentinel pair.
                Assert.assertEquals(
                        "an out-of-range projection entry must not disturb the in-range ones",
                        2_000_000L,
                        reader.getColumn(2).getLong(0)
                );
                Assert.assertEquals(3L, reader.getColumn(4).getLong(0));
                Assert.assertEquals(
                        "an out-of-range projection entry must map no column",
                        0,
                        reader.getSymbolCount(4)
                );
                // The projection names neither symbol column, so the bind stays a projected one
                // rather than degrading into a full open of every column.
                Assert.assertEquals(0, reader.getSymbolCount(2));
                Assert.assertEquals(0, reader.getSymbolCount(3));
            }
        });
    }

    /**
     * {@code of()} publishes the identity fields before the calls that can throw, so a
     * failure part-way through leaves the reader claiming the new segment while
     * {@code columnCount} and the saved resume offset still describe the old one. The
     * retry then satisfies the same-segment fast path and folds the new segment's event
     * file from an offset that belongs to a different file.
     * <p>
     * Two tables of identical shape stand in for the two segments: each table's first WAL
     * is named the same, so after the failed bind the identity matches on every field the
     * fast path reads.
     */
    @Test
    public void testRebindAfterFailedOpenRebuildsSymbolDictionary() throws Exception {
        final boolean[] failMetaOpen = {false};
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (failMetaOpen[0] && Utf8s.endsWithAscii(name, "_meta")) {
                    failMetaOpen[0] = false;
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t1 (ts TIMESTAMP, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE t2 (ts TIMESTAMP, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken t1 = engine.verifyTableName("t1");
            final TableToken t2 = engine.verifyTableName("t2");
            final String wal1 = writeTwoSymbols(t1, "aaa", "bbb");
            final String wal2 = writeTwoSymbols(t2, "xxx", "yyy");
            Assert.assertEquals(wal1, wal2);

            final DirectString view = new DirectString();
            try (WalReader reader = new WalReader(configuration)) {
                reader.of(t1, wal1, 0, 2);
                Assert.assertEquals("aaa", reader.getSymbolValue(1, 0, view).toString());

                // Fails inside of(), after the identity fields have been assigned.
                failMetaOpen[0] = true;
                try {
                    reader.of(t2, wal2, 0, 2);
                    Assert.fail("expected the injected _meta open failure to propagate");
                } catch (CairoException expected) {
                    Assert.assertFalse("the injection must have fired", failMetaOpen[0]);
                }

                // The retry must not trust anything the failed bind left behind.
                reader.of(t2, wal2, 0, 2);
                Assert.assertEquals("xxx", reader.getSymbolValue(1, 0, view).toString());
                Assert.assertEquals("yyy", reader.getSymbolValue(1, 1, view).toString());
            }
        });
    }

    /**
     * The identity is keyed on the directory the reader actually opens, not on the table
     * name. {@code TableToken.getDirName()} is {@code <name>~<tableId>}, so DROP + CREATE
     * of the same name - or a blue/green rename swap - yields a different directory under
     * an unchanged name. Keyed on the name alone the reader treats the two as the same
     * segment and serves the dropped table's dictionary for the new table's rows.
     */
    @Test
    public void testRebindToRecreatedTableClearsSymbolDictionary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken first = engine.verifyTableName("base");
            final String firstWal = writeTwoSymbols(first, "aaa", "bbb");

            final DirectString view = new DirectString();
            try (WalReader reader = new WalReader(configuration)) {
                reader.of(first, firstWal, 0, 2);
                Assert.assertEquals("aaa", reader.getSymbolValue(1, 0, view).toString());
                Assert.assertEquals("bbb", reader.getSymbolValue(1, 1, view).toString());

                execute("DROP TABLE base");
                execute("CREATE TABLE base (ts TIMESTAMP, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
                final TableToken second = engine.verifyTableName("base");
                Assert.assertNotEquals(
                        "the recreated table must occupy a different directory for this test to mean anything",
                        first.getDirName(),
                        second.getDirName()
                );
                final String secondWal = writeTwoSymbols(second, "xxx", "yyy");
                Assert.assertEquals(
                        "a fresh table's first WAL reuses the same name, which is what makes the identity ambiguous",
                        firstWal,
                        secondWal
                );

                reader.of(second, secondWal, 0, 2);
                Assert.assertEquals("xxx", reader.getSymbolValue(1, 0, view).toString());
                Assert.assertEquals("yyy", reader.getSymbolValue(1, 1, view).toString());
            }
        });
    }

    /**
     * {@code openSymbolMaps} decides whether to fold a column's diff from a membership index
     * over the mapped set, keyed by column index. An index that is wrongly TRUE for a column
     * the projection does not reach folds that column's diff and opens its clean-band file -
     * the very fault the projected skip exists to avoid - and one that is wrongly FALSE leaves
     * a projected column resolving against an empty map. Both are silent.
     * <p>
     * Every other test in this class uses a four-column table, so all of them exercise column
     * indexes 0 to 3 only. This one spans 70 columns with SYMBOL columns at 5 and 69 - indexes
     * that agree in their low six bits - so an index keyed on anything narrower than the whole
     * column index confuses them. The total projection stands in for "a projection that reaches
     * every column must fold exactly what no projection folds"; the two partial projections
     * each name one symbol column and must skip the other.
     */
    @Test
    public void testWideTableProjectionFoldsAndSkipsByExactColumnIndex() throws Exception {
        assertMemoryLeak(() -> {
            final String walName = seedWideSegmentWithCleanSymbolBands();
            final TableToken token = engine.verifyTableName("wide");

            final DirectString view = new DirectString();
            final ObjList<String> unprojectedLow = new ObjList<>();
            final ObjList<String> unprojectedHigh = new ObjList<>();
            try (WalReader reader = new WalReader(configuration)) {
                reader.of(token, walName, 0, 1);
                for (int key = 0, n = reader.getSymbolCount(LOW_SYMBOL_COLUMN); key < n; key++) {
                    unprojectedLow.add(reader.getSymbolValue(LOW_SYMBOL_COLUMN, key, view).toString());
                }
                for (int key = 0, n = reader.getSymbolCount(HIGH_SYMBOL_COLUMN); key < n; key++) {
                    unprojectedHigh.add(reader.getSymbolValue(HIGH_SYMBOL_COLUMN, key, view).toString());
                }
            }
            Assert.assertEquals("[aaa,bbb,ccc]", unprojectedLow.toString());
            Assert.assertEquals("[x1,x2,x3]", unprojectedHigh.toString());

            final IntList projection = new IntList();
            try (WalReader reader = new WalReader(configuration)) {
                for (int i = 0; i < WIDE_COLUMN_COUNT; i++) {
                    projection.add(i);
                }
                reader.of(token, walName, 0, 1, projection);
                final ObjList<String> projectedLow = new ObjList<>();
                final ObjList<String> projectedHigh = new ObjList<>();
                for (int key = 0, n = reader.getSymbolCount(LOW_SYMBOL_COLUMN); key < n; key++) {
                    projectedLow.add(reader.getSymbolValue(LOW_SYMBOL_COLUMN, key, view).toString());
                }
                for (int key = 0, n = reader.getSymbolCount(HIGH_SYMBOL_COLUMN); key < n; key++) {
                    projectedHigh.add(reader.getSymbolValue(HIGH_SYMBOL_COLUMN, key, view).toString());
                }
                Assert.assertEquals(
                        "a projection that reaches every column must fold what no projection folds",
                        unprojectedLow.toString(),
                        projectedLow.toString()
                );
                Assert.assertEquals(
                        "a projection that reaches every column must fold what no projection folds",
                        unprojectedHigh.toString(),
                        projectedHigh.toString()
                );

                // Names the low symbol column but not the high one.
                projection.clear();
                projection.add(0);
                projection.add(LOW_SYMBOL_COLUMN);
                reader.of(token, walName, 0, 1, projection);
                Assert.assertEquals("ccc", reader.getSymbolValue(LOW_SYMBOL_COLUMN, 2, view).toString());
                Assert.assertEquals(
                        "a projection naming column 5 must not reach column 69",
                        0,
                        reader.getSymbolCount(HIGH_SYMBOL_COLUMN)
                );

                // And the mirror image: the high symbol column alone.
                projection.clear();
                projection.add(0);
                projection.add(HIGH_SYMBOL_COLUMN);
                reader.of(token, walName, 0, 1, projection);
                Assert.assertEquals("x3", reader.getSymbolValue(HIGH_SYMBOL_COLUMN, 2, view).toString());
                Assert.assertEquals(
                        "a projection naming column 69 must not reach column 5",
                        0,
                        reader.getSymbolCount(LOW_SYMBOL_COLUMN)
                );
            }
        });
    }

    /**
     * Skipping a column's diffs is only safe while the projection stays narrow. A rebind that
     * widens it onto the same segment has to fold from the start of the event file again -
     * both the clean band and every diff entry the narrow bind walked past - or the widened
     * column resolves against an empty map and the view reads wrong SYMBOL values.
     */
    @Test
    public void testWideningRebindFoldsPreviouslySkippedSymbolMap() throws Exception {
        assertMemoryLeak(() -> {
            final String walName;
            final TableToken token;
            try (WalWriter walWriter = seedSegmentWithCleanSymbolBand()) {
                token = engine.verifyTableName("base");
                walName = walWriter.getWalName();
            }

            final IntList projection = new IntList();
            projection.add(0);
            projection.add(1);
            final DirectString view = new DirectString();
            try (WalReader reader = new WalReader(configuration)) {
                reader.of(token, walName, 0, 1, projection);
                Assert.assertEquals(
                        "an unprojected column's symbol map must stay unfolded",
                        0,
                        reader.getSymbolCount(2)
                );
                Assert.assertEquals(0, reader.getSymbolCount(3));

                projection.add(2);
                projection.add(3);
                reader.of(token, walName, 0, 1, projection);
                Assert.assertEquals(3, reader.getSymbolCount(2));
                Assert.assertEquals("aaa", reader.getSymbolValue(2, 0, view).toString());
                Assert.assertEquals("bbb", reader.getSymbolValue(2, 1, view).toString());
                Assert.assertEquals("ccc", reader.getSymbolValue(2, 2, view).toString());
                Assert.assertEquals(3, reader.getSymbolCount(3));
                Assert.assertEquals("x1", reader.getSymbolValue(3, 0, view).toString());
                Assert.assertEquals("x2", reader.getSymbolValue(3, 1, view).toString());
                Assert.assertEquals("x3", reader.getSymbolValue(3, 2, view).toString());
            }
        });
    }

    /**
     * Creates {@code base} with one unindexed and one indexed SYMBOL column, applies a
     * two-symbol dictionary to each, then commits one more row through a fresh WAL writer.
     * The fresh writer links the applied dictionaries into its own WAL directory, so its
     * segment 0 holds a single row whose symbol diffs carry {@code cleanSymbolCount = 2} -
     * diffs whose fold reads {@code <wal>/s.o} and {@code <wal>/si.o}. Returns the writer
     * still open, because the DROP COLUMN file deletion happens in the writer that owns that
     * directory; the caller closes it.
     */
    private static WalWriter seedSegmentWithCleanSymbolBand() throws SqlException {
        execute("CREATE TABLE base (ts TIMESTAMP, v LONG, s SYMBOL, si SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                    ('2024-01-01T00:00:00.000000Z', 1, 'aaa', 'x1'),
                    ('2024-01-01T00:00:01.000000Z', 2, 'bbb', 'x2')""");
        drainWalQueue();
        // The writer that wrote those rows was configured when the table's dictionary was
        // still empty, so its diffs reference no clean band at all. Drop it, so the next
        // writer links the applied dictionary into its WAL directory.
        engine.releaseInactive();

        final WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("base"));
        try {
            TableWriter.Row row = walWriter.newRow(2_000_000L);
            row.putLong(1, 3);
            row.putSym(2, "ccc");
            row.putSym(3, "x3");
            row.append();
            walWriter.commit();
            return walWriter;
        } catch (Throwable th) {
            walWriter.close();
            throw th;
        }
    }

    /**
     * Creates {@code wide}: {@value #WIDE_COLUMN_COUNT} columns whose only SYMBOL columns sit
     * at {@value #LOW_SYMBOL_COLUMN} and {@value #HIGH_SYMBOL_COLUMN}, applies a two-symbol
     * dictionary to each, then commits one more row through a fresh WAL writer so its segment
     * 0 holds a single row whose two symbol diffs each carry {@code cleanSymbolCount = 2}.
     * Returns the writer's WAL name.
     */
    private static String seedWideSegmentWithCleanSymbolBands() throws SqlException {
        final StringBuilder ddl = new StringBuilder("CREATE TABLE wide (ts TIMESTAMP");
        for (int i = 1; i < WIDE_COLUMN_COUNT; i++) {
            ddl.append(", c").append(i);
            ddl.append(i == LOW_SYMBOL_COLUMN || i == HIGH_SYMBOL_COLUMN ? " SYMBOL" : " LONG");
        }
        ddl.append(") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute(ddl.toString());
        execute("""
                INSERT INTO wide (ts, c5, c69) VALUES
                    ('2024-01-01T00:00:00.000000Z', 'aaa', 'x1'),
                    ('2024-01-01T00:00:01.000000Z', 'bbb', 'x2')""");
        drainWalQueue();
        // The writer that wrote those rows saw an empty dictionary, so its diffs reference no
        // clean band. Drop it, so the next writer links the applied dictionary into its WAL.
        engine.releaseInactive();

        try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("wide"))) {
            TableWriter.Row row = walWriter.newRow(2_000_000L);
            row.putSym(LOW_SYMBOL_COLUMN, "ccc");
            row.putSym(HIGH_SYMBOL_COLUMN, "x3");
            row.append();
            walWriter.commit();
            return walWriter.getWalName();
        }
    }

    private static String writeTwoSymbols(TableToken token, String first, String second) {
        try (WalWriter walWriter = engine.getWalWriter(token)) {
            TableWriter.Row row = walWriter.newRow(0);
            row.putSym(1, first);
            row.append();
            row = walWriter.newRow(1_000);
            row.putSym(1, second);
            row.append();
            walWriter.commit();
            return walWriter.getWalName();
        }
    }
}
