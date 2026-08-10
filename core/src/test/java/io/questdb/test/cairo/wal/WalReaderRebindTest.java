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
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.LPSZ;
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
