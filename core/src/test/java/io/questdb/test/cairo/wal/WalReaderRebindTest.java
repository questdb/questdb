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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
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
        assertMemoryLeak(() -> {
            final String walName;
            final TableToken token;
            try (WalWriter walWriter = seedSegmentWithCleanSymbolBand()) {
                token = engine.verifyTableName("base");
                walName = walWriter.getWalName();
                Assert.assertTrue(
                        "the clean band must be linked into the WAL directory, or the diff carries no clean symbol count",
                        symbolOffsetFileExists(token, walName, "s")
                );
                Assert.assertTrue(symbolOffsetFileExists(token, walName, "si"));

                execute("ALTER TABLE base DROP COLUMN s");
                execute("ALTER TABLE base DROP COLUMN si");
                // What the writer pool does before it hands a writer out. This is where
                // WalWriter.markColumnRemoved -> removeSymbolFiles deletes the clean band.
                walWriter.goActive();
                Assert.assertFalse(
                        "the writer must delete the WAL-level clean band on DROP COLUMN",
                        symbolOffsetFileExists(token, walName, "s")
                );
                Assert.assertFalse(symbolOffsetFileExists(token, walName, "si"));
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

    private static boolean symbolOffsetFileExists(TableToken token, String walName, String columnName) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token.getDirName()).concat(walName);
            TableUtils.offsetFileName(path, columnName, TableUtils.COLUMN_NAME_TXN_NONE);
            return configuration.getFilesFacade().exists(path.$());
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
