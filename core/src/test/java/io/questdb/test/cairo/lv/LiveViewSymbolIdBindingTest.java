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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewSymbolIdRegistry;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Stage 2 of the partition-key classification, against real views: which base column each
 * admitted term binds its dictionary to, and which cursor families arm it.
 * <p>
 * Stage 1 runs inside the code generator, where the key type has to be fixed and the compiled
 * plan does not exist yet, so all it can say is that a term is a plain SYMBOL column reference
 * into the window's input. Which base column that input column actually reads is a property of
 * the compiled chain - a projection, an alias or a column drop sits between the two - and the
 * refresh path is the first place that can answer it. Binding the wrong answer is not a slower
 * view: an id from another column's dictionary is in range for the one it lands in, so nothing
 * downstream rejects it.
 * <p>
 * Every view here now translates: a direct SYMBOL partition term keys as this view's own
 * LV-private id, both in the runtime maps and in the checkpoint domain, which is why each test
 * also asserts the view's own output - the SELECT-list values a translated key resolves back
 * through are what proves the binding changed a representation and not a result.
 * {@link LiveViewSymbolIdRegistryTest} covers the translator contract these bindings read
 * through, on its own.
 */
public class LiveViewSymbolIdBindingTest extends AbstractLiveViewTest {

    @Test
    public void testCompositeSymbolKeyBindsBothColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym1 SYMBOL, sym2 SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym1, sym2, sum(x) OVER (PARTITION BY sym1, sym2 ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");
            execute("INSERT INTO base VALUES "
                    + "('2024-01-01T00:00:00.000000Z', 'a', 'p', 1), "
                    + "('2024-01-01T00:00:01.000000Z', 'b', 'q', 2), "
                    + "('2024-01-01T00:00:02.000000Z', 'a', 'p', 3)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            final LiveViewSymbolIdRegistry registry = translators();
            Assert.assertEquals("both key columns need a dictionary of their own", 2, registry.getBoundSlotCount());
            assertBoundTo(registry, "sym1");
            assertBoundTo(registry, "sym2");

            assertQuery("SELECT ts, sym1, sym2, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym1\tsym2\ts
                            2024-01-01T00:00:00.000000Z\ta\tp\t1.0
                            2024-01-01T00:00:01.000000Z\tb\tq\t2.0
                            2024-01-01T00:00:02.000000Z\ta\tp\t4.0
                            """
            );
        });
    }

    @Test
    public void testDirectSymbolKeyBindsItsBaseWriterColumn() throws Exception {
        // The binding is by writer index rather than by name or by scan position, because a WAL
        // segment names its columns by writer index and that is the only identity that survives
        // a projection between the scan and the window.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");
            execute("INSERT INTO base VALUES "
                    + "('2024-01-01T00:00:00.000000Z', 1, 'a'), "
                    + "('2024-01-01T00:00:01.000000Z', 2, 'a')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            final LiveViewSymbolIdRegistry registry = translators();
            Assert.assertEquals(1, registry.getBoundSlotCount());
            assertBoundTo(registry, "sym");
            // The WAL drain armed it for every transaction it read, and left it unarmed after
            // releasing the segment.
            Assert.assertTrue("the WAL drain must arm the dictionaries it could key through", registry.getArmCount() > 0);
            // Both rows carry the same string, so translating them interns it exactly once.
            Assert.assertEquals(1, registry.getTotalDictionarySize());

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym\ts
                            2024-01-01T00:00:00.000000Z\ta\t1.0
                            2024-01-01T00:00:01.000000Z\ta\t3.0
                            """
            );
        });
    }

    @Test
    public void testDrainLeavesEverySlotUnarmed() throws Exception {
        // A cursor open owns its arming and hands it back on close, so an unarmed slot is the
        // default state rather than a stale-but-plausible one. That is what makes a family
        // that forgets to arm fail loudly instead of keying its rows through whichever
        // boundary the previous cursor left behind - the id it would produce is in range for
        // the dictionary, so nothing downstream could tell.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 'a', 1)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            final LiveViewSymbolIdRegistry registry = translators();
            final int slot = registry.getBoundSlot(0);
            try {
                registry.translate(slot, 0);
                Assert.fail("no cursor is open, so no slot may translate");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "not armed for the current source");
            }
        });
    }

    @Test
    public void testWalDrainArmsThisTransactionsOwnBand() throws Exception {
        // The band is the transaction's, not the column's: above the clean count sit ids the
        // WAL writer restarts on every commit, and the drain has to publish that boundary per
        // transaction rather than let a slot keep the previous one.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:00.000000Z', 'a', 1), "
                        + "('2024-01-01T00:00:01.000000Z', 'b', 2)");
                driveRefreshToQuiescence(job);
                final LiveViewSymbolIdRegistry registry = translators();
                final int slot = registry.getBoundSlot(0);
                Assert.assertEquals(
                        "the first commit introduces both symbols, so both sit in its dirty band",
                        2,
                        registry.getArmedDirtyBandSize(slot)
                );
                Assert.assertEquals(0, registry.getArmedCleanSymbolCount(slot));

                // The next commit introduces one more symbol, and the WAL writer restarts its
                // local ids at the same clean count - so raw id 0 names 'a' in the first
                // transaction and 'c' in this one. That collision is the whole reason a
                // partition key cannot be the raw id, and it is why the band has to be armed
                // per transaction rather than per column.
                execute("INSERT INTO base VALUES ('2024-01-01T00:00:02.000000Z', 'c', 3)");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(1, registry.getArmedDirtyBandSize(slot));
                Assert.assertEquals(0, registry.getArmedCleanSymbolCount(slot));
            }
        });
    }

    @Test
    public void testExpressionKeyBindsNothing() throws Exception {
        // A symbol-valued expression has no base column to hang a dictionary off, so stage 1
        // never admits it and stage 2 has nothing to bind. The view keys through the resolved
        // string, which is the safe path it has always taken.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, sum(x) OVER (PARTITION BY lower(sym) ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");
            execute("INSERT INTO base VALUES "
                    + "('2024-01-01T00:00:00.000000Z', 'A', 1), "
                    + "('2024-01-01T00:00:01.000000Z', 'a', 2)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            // A live view's compile always ensures a registry exists - LiveViewRefreshJob
            // .compileViewSelect needs one to hand its classifier a translator before it
            // knows whether this compile has anything to bind - but an expression-keyed
            // view's registry stays empty: stage 1 admits nothing, so stage 2 binds nothing.
            final LiveViewSymbolIdRegistry registry = viewInstance().getPartitionKeyTranslators();
            Assert.assertNotNull(registry);
            Assert.assertEquals(
                    "an expression-keyed view must not bind a dictionary it can never trace a source for",
                    0,
                    registry.getBoundSlotCount()
            );
            Assert.assertEquals(0, registry.getTotalDictionarySize());
            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym\ts
                            2024-01-01T00:00:00.000000Z\tA\t1.0
                            2024-01-01T00:00:01.000000Z\ta\t3.0
                            """
            );
        });
    }

    @Test
    public void testOneSourceColumnBehindTwoWindowsSharesOneDictionary() throws Exception {
        // Two windows keyed by the same base column resolve to one slot, so they share an id
        // namespace. They have to: an anchor map, a window function's own map and a persisted
        // partition map all compare keys those sinks wrote.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, "
                    + "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s, "
                    + "count() OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS c "
                    + "FROM base");
            execute("INSERT INTO base VALUES "
                    + "('2024-01-01T00:00:00.000000Z', 'a', 1), "
                    + "('2024-01-01T00:00:01.000000Z', 'b', 2)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            final LiveViewSymbolIdRegistry registry = translators();
            Assert.assertEquals("one source column is one dictionary", 1, registry.getBoundSlotCount());
            assertBoundTo(registry, "sym");
        });
    }

    @Test
    public void testProjectedSymbolKeyBindsThroughTheProjection() throws Exception {
        // A pre-window scalar puts a projection between the base scan and the window, so the
        // key's window-input index is no longer its base-scan index. The trace has to follow the
        // compiled chain; reading the index straight through would bind whichever base column
        // happens to share the ordinal - here 'account', which is also a SYMBOL, so the
        // translation would succeed and the view would key through the wrong dictionary.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, account SYMBOL, region SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, region, sum(x * 2) OVER (PARTITION BY region ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s "
                    + "FROM base");
            execute("INSERT INTO base VALUES "
                    + "('2024-01-01T00:00:00.000000Z', 'acc', 'eu', 1), "
                    + "('2024-01-01T00:00:01.000000Z', 'acc', 'eu', 2)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            final LiveViewSymbolIdRegistry registry = translators();
            Assert.assertEquals(1, registry.getBoundSlotCount());
            assertBoundTo(registry, "region");

            assertQuery("SELECT ts, region, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tregion\ts
                            2024-01-01T00:00:00.000000Z\teu\t2.0
                            2024-01-01T00:00:01.000000Z\teu\t6.0
                            """
            );
        });
    }

    @Test
    public void testRepairScansArmTheDictionariesToo() throws Exception {
        // The repair path is where a wrong key is least likely to be noticed and most expensive
        // to undo, so its scans arm like any other source. An out-of-order commit is what drives
        // the bounds discovery and the replay through it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:00.000000Z', 'a', 1), "
                        + "('2024-01-01T00:00:02.000000Z', 'a', 2), "
                        + "('2024-01-01T00:00:04.000000Z', 'b', 3)");
                driveRefreshToQuiescence(job);
                final long armsAfterDrain = translators().getArmCount();

                // Strictly below the head, so the refresh diverts to a repair rather than
                // appending.
                execute("INSERT INTO base VALUES ('2024-01-01T00:00:01.000000Z', 'a', 10)");
                driveRefreshToQuiescence(job);
                Assert.assertTrue(
                        "the repair's own base scans must arm the dictionaries",
                        translators().getArmCount() > armsAfterDrain
                );
            }

            // And the repair produced the same view a from-base recompute would.
            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym\ts
                            2024-01-01T00:00:00.000000Z\ta\t1.0
                            2024-01-01T00:00:01.000000Z\ta\t11.0
                            2024-01-01T00:00:02.000000Z\ta\t13.0
                            2024-01-01T00:00:04.000000Z\tb\t3.0
                            """
            );
        });
    }

    private void assertBoundTo(LiveViewSymbolIdRegistry registry, String baseColumnName) {
        final TableToken baseToken = engine.verifyTableName("base");
        final int expectedWriterIndex;
        try (TableMetadata metadata = engine.getTableMetadata(baseToken)) {
            expectedWriterIndex = metadata.getWriterIndex(metadata.getColumnIndex(baseColumnName));
        }
        for (int i = 0, n = registry.getBoundSlotCount(); i < n; i++) {
            if (registry.getBaseWriterColumnIndex(registry.getBoundSlot(i)) == expectedWriterIndex) {
                return;
            }
        }
        Assert.fail("no dictionary slot is bound to base column '" + baseColumnName + "'");
    }

    private LiveViewSymbolIdRegistry translators() {
        final LiveViewSymbolIdRegistry registry = viewInstance().getPartitionKeyTranslators();
        Assert.assertNotNull("a direct SYMBOL partition term must bind a dictionary", registry);
        return registry;
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }
}
