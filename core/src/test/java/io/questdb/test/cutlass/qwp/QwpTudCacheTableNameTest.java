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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.cutlass.qwp.server.QwpTudCache;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A QWP connection keeps one writer per table, whatever spelling of the table's
 * name a frame carries. The engine resolves names case-insensitively, folding
 * each UTF-16 char with {@link Character#toLowerCase(char)}, so the cache must
 * treat two spellings as one table exactly when the engine does: for non-ASCII
 * letters too, where it used to fold only ASCII.
 */
public class QwpTudCacheTableNameTest extends AbstractCairoTest {
    // Pairs of spellings, and whether the engine resolves them to the same table.
    private static final Object[][] SPELLINGS = {
            {"tä", "tÄ", true},
            {"TÄ", "tä", true},
            {"ж", "Ж", true},
            {"Σx", "σx", true},
            // Titlecase and uppercase share a lowercase.
            {"ǅx", "Ǆx", true},
            // İ lowercases to ASCII i, from two UTF-8 bytes to one.
            {"tİ", "TI", true},
            // The Kelvin sign lowercases to ASCII k, from three bytes to one.
            {"\u212Ax", "kx", true},
            // Ⱥ lowercases from two bytes to three.
            {"Ⱥx", "ⱥx", true},
            // A name the wire carries, 123 bytes, whose key outgrows the wire's 127.
            {"Ⱥ".repeat(60), "ⱥ".repeat(60), true},
            // A supplementary character next to a cased letter.
            {"😀Ä", "😀ä", true},
            {"Ａx", "ａx", true},
            // Final sigma lowercases to itself, not to σ.
            {"tσ", "tς", false},
            // The engine folds UTF-16 chars, not code points: a surrogate keeps its case,
            // so Deseret capital and small letters name different tables.
            {"𐐀", "𐐨", false},
            // Composed and decomposed ä.
            {"tä", "ta\u0308", false},
            {"ß", "ss", false},
    };

    @Test
    public void testReleasedTableTakenBackUnderAnotherSpelling() throws Exception {
        assertMemoryLeak(() -> {
            createTable("tä");
            createTable("u");
            // The entry keeps the spelling of the lookup that created it, which is not its
            // key "tä" when that spelling is "TÄ": the release must record the key.
            for (String[] spellings : new String[][]{{"tä", "TÄ"}, {"TÄ", "tä"}}) {
                final String context = spellings[0] + " / " + spellings[1];
                try (QwpTudCache cache = newCache()) {
                    lookup(cache, spellings[0], 1);
                    cache.releaseIdleWriters();
                    Assert.assertEquals(context, 0, cache.size());
                    // The released table keeps its slot under the table limit and takes it
                    // back under another spelling.
                    final WalTableUpdateDetails tud = lookup(cache, spellings[1], 1);
                    Assert.assertEquals(context, 1, cache.size());
                    Assert.assertSame(context, engine.verifyTableName("tä"), tud.getTableToken());
                    assertTooManyTables(cache, "u", 1);
                    // Taken back, the slot counts once.
                    Assert.assertNotNull(context, lookup(cache, "u", 2));
                    Assert.assertEquals(context, 2, cache.size());
                }
            }
        });
    }

    @Test
    public void testRowsUnderSpellingsOfOneTableCommitOnce() throws Exception {
        assertMemoryLeak(() -> {
            createTable("tä");
            final TableToken token = engine.verifyTableName("tä");
            final long txnBefore = engine.getTableSequencerAPI().lastTxn(token);
            try (QwpTudCache cache = newCache()) {
                appendRow(lookup(cache, "tä", 10), 1);
                appendRow(lookup(cache, "tÄ", 10), 2);
                appendRow(lookup(cache, "TÄ", 10), 3);
                commitAll(cache);
                Assert.assertEquals("one transaction", txnBefore + 1, engine.getTableSequencerAPI().lastTxn(token));
            }
            drainWalQueue();
            assertQuery("SELECT count(), sum(v) FROM \"tä\"").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tsum\n3\t6\n");
        });
    }

    @Test
    public void testSpellingsOfOneTableCountOnceAgainstTableLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTable("tä");
            createTable("u");
            try (QwpTudCache cache = newCache()) {
                final WalTableUpdateDetails tud = lookup(cache, "tä", 1);
                Assert.assertSame(tud, lookup(cache, "tÄ", 1));
                Assert.assertSame(tud, lookup(cache, "TÄ", 1));
                Assert.assertEquals(1, cache.size());
                assertTooManyTables(cache, "u", 1);
            }
        });
    }

    @Test
    public void testSpellingsShareWriterExactlyWhenEngineResolvesThemToOneTable() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTudCache cache = newCache()) {
                int tableCount = 0;
                for (int i = 0; i < SPELLINGS.length; i++) {
                    // A prefix per pair keeps the pairs' tables apart.
                    final String first = "p" + i + "_" + SPELLINGS[i][0];
                    final String second = "p" + i + "_" + SPELLINGS[i][1];
                    final boolean isSameTable = (boolean) SPELLINGS[i][2];
                    final String pair = first + " / " + second;
                    createTable(first);
                    final TableToken firstToken = engine.verifyTableName(first);
                    // The engine decides which spellings name one table.
                    Assert.assertEquals(pair, isSameTable, engine.getTableTokenIfExists(second) == firstToken);
                    if (!isSameTable) {
                        createTable(second);
                    }
                    final WalTableUpdateDetails firstTud = lookup(cache, first, 100);
                    final WalTableUpdateDetails secondTud = lookup(cache, second, 100);
                    tableCount += isSameTable ? 1 : 2;
                    Assert.assertEquals(pair, isSameTable, firstTud == secondTud);
                    Assert.assertSame(pair, firstToken, firstTud.getTableToken());
                    Assert.assertSame(pair, engine.verifyTableName(second), secondTud.getTableToken());
                    Assert.assertEquals(pair, tableCount, cache.size());
                    // The lookups that change nothing find the same entries, the shortcut
                    // to the entry found last included.
                    Assert.assertSame(pair, secondTud, cache.getCachedTableUpdateDetails(new Utf8String(second)));
                    Assert.assertSame(pair, firstTud, cache.getCachedTableUpdateDetails(new Utf8String(first)));
                    Assert.assertSame(pair, secondTud, cache.getCachedTableUpdateDetails(new Utf8String(second)));
                }
            }
        });
    }

    @Test
    public void testStaleEntryEvictedUnderAnotherSpelling() throws Exception {
        // The table is dropped and created again under another spelling. A lookup under a
        // third spelling finds the entry of the dropped table, evicts it, and caches the
        // new table in its place.
        assertMemoryLeak(() -> {
            createTable("tä");
            try (QwpTudCache cache = newCache()) {
                final WalTableUpdateDetails dropped = lookup(cache, "tä", 1);
                execute("DROP TABLE \"tä\"");
                createTable("TÄ");
                final TableToken recreated = engine.verifyTableName("TÄ");
                final WalTableUpdateDetails tud = lookup(cache, "tÄ", 1);
                Assert.assertNotSame(dropped, tud);
                Assert.assertSame(recreated, tud.getTableToken());
                Assert.assertEquals(1, cache.size());
                Assert.assertSame(tud, cache.getCachedTableUpdateDetails(new Utf8String("tä")));
                Assert.assertSame(tud, lookup(cache, "TÄ", 1));
            }
        });
    }

    private static void appendRow(WalTableUpdateDetails tud, long value) {
        final TableWriter.Row row = tud.getWriter().newRow(1_000_000L);
        row.putLong(0, value);
        row.append();
    }

    private static void assertTooManyTables(QwpTudCache cache, String tableName, int maxTables) {
        try {
            lookup(cache, tableName, maxTables);
            Assert.fail("the table limit must refuse " + tableName);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "too many distinct tables, limit: " + maxTables);
        }
    }

    private static void commitAll(QwpTudCache cache) {
        try {
            cache.commitAll();
        } catch (Throwable th) {
            throw new AssertionError(th);
        }
    }

    private static void createTable(String tableName) throws Exception {
        execute("CREATE TABLE \"" + tableName + "\" (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private static WalTableUpdateDetails lookup(QwpTudCache cache, String tableName, int maxTables) {
        final WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                AllowAllSecurityContext.INSTANCE,
                new Utf8String(tableName),
                null,
                null,
                maxTables
        );
        Assert.assertNotNull(tableName, tud);
        return tud;
    }

    private static QwpTudCache newCache() {
        return new QwpTudCache(
                engine,
                true,
                true,
                new DefaultColumnTypes(new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration)),
                PartitionBy.DAY
        );
    }
}
