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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A WAL segment's symbol key space is the base table's committed ("clean") dictionary
 * {@code [0, cleanSymbolCount)} followed by the keys each transaction's diff adds. Only the
 * diff half is folded into {@link WalReader}'s off-heap maps; the clean half stays on disk
 * and resolves through the base table's own mapped symbol files, so a reader's memory
 * tracks the segment rather than the base table's total cardinality.
 * <p>
 * These tests pin the resolution contract across that split - values, keys, counts, the
 * null sentinel - and prove the clean band is not copied.
 */
public class WalReaderCleanSymbolBandTest extends AbstractCairoTest {

    /**
     * The bound belongs to the segment. A rebind to a segment whose first transaction
     * carries no clean band must drop it, or that segment's own diff keys - which start at
     * zero - get routed into the previous segment's dictionary and resolve to the wrong
     * strings.
     */
    @Test
    public void testCleanBandDoesNotSurviveRebindToUnbandedSegment() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createBaseTable();
            final String walName = writeTwoSegments(token);

            final DirectString view = new DirectString();
            try (WalReader reader = new WalReader(configuration)) {
                reader.of(token, walName, 1, 3);
                Assert.assertEquals(4, reader.getSymbolCount(1));
                Assert.assertEquals("aaa", reader.getSymbolValue(1, 0, view).toString());

                // Segment 0 predates every commit, so its keys 0..2 are its own diff entries.
                reader.of(token, walName, 0, 3);
                Assert.assertEquals(3, reader.getSymbolCount(1));
                Assert.assertEquals("aaa", reader.getSymbolValue(1, 0, view).toString());
                Assert.assertEquals("bbb", reader.getSymbolValue(1, 1, view).toString());
                Assert.assertEquals("ccc", reader.getSymbolValue(1, 2, view).toString());
                Assert.assertNull(reader.getSymbolValue(1, 3, view));
            }
        });
    }

    /**
     * The clean band is the reason a live view's WAL reader used to hold the base table's
     * whole dictionary in native memory: it was copied key by key into the segment's
     * off-heap map on every bind. What the reader folds now is the diff alone, so binding a
     * segment behind thousands of committed symbols costs a bounded amount of memory.
     */
    @Test
    public void testCleanBandIsNotCopiedIntoNativeMemory() throws Exception {
        assertMemoryLeak(() -> {
            final int cleanSymbols = 20_000;
            final TableToken token = createBaseTable();
            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                walName = walWriter.getWalName();
                for (int i = 0; i < cleanSymbols; i++) {
                    TableWriter.Row row = walWriter.newRow(i);
                    row.putSym(1, "sym-" + i);
                    row.append();
                }
                walWriter.commit();
                drainWalQueue();
                walWriter.rollSegment();
                TableWriter.Row row = walWriter.newRow(1_000_000);
                row.putSym(1, "tail");
                row.append();
                walWriter.commit();
            }
            drainWalQueue();

            final DirectString view = new DirectString();
            final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
            try (WalReader reader = new WalReader(configuration)) {
                reader.of(token, walName, 1, 1);
                // Resolving through the whole clean band must not accumulate either: each
                // value is a view over the mapped dictionary, not a copy.
                for (int key = 0; key < cleanSymbols; key++) {
                    Assert.assertEquals("sym-" + key, reader.getSymbolValue(1, key, view).toString());
                }
                Assert.assertEquals("tail", reader.getSymbolValue(1, cleanSymbols, view).toString());
                final long held = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_LIVE_VIEW_IN_MEM) - before;
                // A copied band would need at least 2 bytes per UTF-16 char plus a length
                // prefix and a hash slot per key - upwards of 400 KB for this dictionary.
                Assert.assertTrue(
                        "the clean band must not be folded into the segment's map, held=" + held,
                        held < 64 * 1024
                );
            }
        });
    }

    /**
     * The two halves of the key space have to answer as one table: values by key, keys by
     * value bounded at the caller's clean count, and a count that spans both. A caller
     * enumerating {@code 0..getSymbolCount()-1} must see every symbol the segment can carry.
     */
    @Test
    public void testCleanBandResolvesAcrossTheDiffBoundary() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createBaseTable();
            final String walName = writeTwoSegments(token);

            final DirectString viewA = new DirectString();
            final DirectString viewB = new DirectString();
            try (WalReader reader = new WalReader(configuration)) {
                reader.of(token, walName, 1, 3);

                // Three committed symbols plus the one this segment added.
                Assert.assertEquals(4, reader.getSymbolCount(1));
                Assert.assertEquals("aaa", reader.getSymbolValue(1, 0, viewA).toString());
                Assert.assertEquals("bbb", reader.getSymbolValue(1, 1, viewA).toString());
                Assert.assertEquals("ccc", reader.getSymbolValue(1, 2, viewA).toString());
                Assert.assertEquals("ddd", reader.getSymbolValue(1, 3, viewA).toString());
                Assert.assertNull(reader.getSymbolValue(1, 4, viewA));
                Assert.assertNull(reader.getSymbolValue(1, SymbolTable.VALUE_IS_NULL, viewA));

                // Two live views over the clean band must not alias, which is what lets a
                // residual filter compare two records of the same column.
                final CharSequence first = reader.getSymbolValue(1, 0, viewA);
                final CharSequence second = reader.getSymbolValue(1, 2, viewB);
                Assert.assertEquals("aaa", first.toString());
                Assert.assertEquals("ccc", second.toString());

                // The clean count is what a caller passes as the bound: it resolves the
                // committed keys and deliberately hides this transaction's own band, whose
                // local ids other transactions in the segment reuse.
                Assert.assertEquals(0, reader.getSymbolKey(1, "aaa", 3));
                Assert.assertEquals(2, reader.getSymbolKey(1, "ccc", 3));
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.getSymbolKey(1, "ddd", 3));
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.getSymbolKey(1, "zzz", 3));
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.getSymbolKey(1, null, 3));
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.getSymbolKey(1, "aaa", 0));

                // A wider bound admits the diff band on top of the clean one.
                Assert.assertEquals(3, reader.getSymbolKey(1, "ddd", 4));
                Assert.assertEquals(0, reader.getSymbolKey(1, "aaa", 4));
            }
        });
    }

    /**
     * The reader is rebound once per base commit and the same-segment fast path folds only
     * newly-appended events. A clean band bound on the first bind has to stay bound across
     * that incremental fold, or the second commit's rows stop resolving their committed
     * symbols.
     */
    @Test
    public void testCleanBandSurvivesIncrementalRebind() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createBaseTable();
            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                walName = walWriter.getWalName();
                appendSymbols(walWriter, 0, "aaa", "bbb", "ccc");
                walWriter.commit();
                drainWalQueue();
                walWriter.rollSegment();
                appendSymbols(walWriter, 10_000, "ddd");
                walWriter.commit();
                appendSymbols(walWriter, 20_000, "bbb", "eee");
                walWriter.commit();
            }
            drainWalQueue();

            final DirectString view = new DirectString();
            try (WalReader reader = new WalReader(configuration)) {
                reader.of(token, walName, 1, 1);
                Assert.assertEquals(4, reader.getSymbolCount(1));
                Assert.assertEquals("bbb", reader.getSymbolValue(1, 1, view).toString());

                // Same segment, more rows: the fold resumes rather than rebuilding. The
                // second commit restarts its local ids at the same unapplied clean count, so
                // "eee" takes key 3 from "ddd" and the band does not widen - the reason the
                // cumulative map is only ever a fallback behind the per-txn diff overlay.
                reader.of(token, walName, 1, 3);
                Assert.assertEquals(4, reader.getSymbolCount(1));
                Assert.assertEquals("aaa", reader.getSymbolValue(1, 0, view).toString());
                Assert.assertEquals("bbb", reader.getSymbolValue(1, 1, view).toString());
                Assert.assertEquals("ccc", reader.getSymbolValue(1, 2, view).toString());
                Assert.assertEquals("eee", reader.getSymbolValue(1, 3, view).toString());
                Assert.assertNull(reader.getSymbolValue(1, 4, view));
                // The clean band is what the bounded lookup answers from, unchanged by the
                // dirty keys the resumed fold appended over it.
                Assert.assertEquals(1, reader.getSymbolKey(1, "bbb", 3));
                Assert.assertEquals(0, reader.getSymbolKey(1, "aaa", 3));
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.getSymbolKey(1, "eee", 3));
            }
        });
    }

    private static void appendSymbols(WalWriter walWriter, long firstTimestamp, String... symbols) {
        for (int i = 0; i < symbols.length; i++) {
            TableWriter.Row row = walWriter.newRow(firstTimestamp + i);
            row.putSym(1, symbols[i]);
            row.append();
        }
    }

    private static TableToken createBaseTable() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
        return engine.verifyTableName("base");
    }

    /**
     * Segment 0 holds three symbols written against an empty table, so they are its own
     * diff entries. The commit is applied before the roll, which is what gives segment 1 a
     * clean band of three and starts its diff keys at three.
     */
    private static String writeTwoSegments(TableToken token) {
        try (WalWriter walWriter = engine.getWalWriter(token)) {
            appendSymbols(walWriter, 0, "aaa", "bbb", "ccc");
            walWriter.commit();
            drainWalQueue();
            walWriter.rollSegment();
            appendSymbols(walWriter, 10_000, "bbb", "ddd", null);
            walWriter.commit();
            return walWriter.getWalName();
        } finally {
            drainWalQueue();
        }
    }
}
