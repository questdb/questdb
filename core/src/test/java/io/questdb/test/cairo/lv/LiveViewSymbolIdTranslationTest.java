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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointKeyProjector;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewSymbolIdRegistry;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

/**
 * The flip itself (section 12 step 7 of the untracked design doc) and the fix for the gap it
 * exposed (step 11): a live view's checkpoint key schema actually becomes SYMBOL once a
 * translator is bound, and a PARTITION BY mixing a direct SYMBOL term with an expression
 * term - which the checkpoint's function-backed sink could not translate before step 11 -
 * compiles, refreshes and repairs correctly instead of stalling silently.
 * <p>
 * {@link LiveViewSymbolIdBindingTest} covers stage-2 binding against real views, unaffected by
 * either step; this covers what changes once a translator actually moves a key.
 */
public class LiveViewSymbolIdTranslationTest extends AbstractLiveViewTest {

    @Test
    public void testCompositeSymbolKeyCheckpointSchemaIsSymbolSymbol() throws Exception {
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

            final int[] checkpointKeyTypes = checkpointKeyColumnTypes();
            Assert.assertArrayEquals(new int[]{ColumnType.SYMBOL, ColumnType.SYMBOL}, checkpointKeyTypes);

            final LiveViewSymbolIdRegistry registry = viewInstance().getPartitionKeyTranslators();
            Assert.assertNotNull(registry);
            // 'a', 'b' in one dictionary and 'p', 'q' in the other.
            Assert.assertEquals(4, registry.getTotalDictionarySize());

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
    public void testDirectSymbolKeyCheckpointSchemaIsSymbolAndNullTranslates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");
            execute("INSERT INTO base VALUES "
                    + "('2024-01-01T00:00:00.000000Z', 'a', 1), "
                    + "('2024-01-01T00:00:01.000000Z', null, 2), "
                    + "('2024-01-01T00:00:02.000000Z', 'a', 3), "
                    + "('2024-01-01T00:00:03.000000Z', null, 4)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            Assert.assertArrayEquals(new int[]{ColumnType.SYMBOL}, checkpointKeyColumnTypes());

            final LiveViewSymbolIdRegistry registry = viewInstance().getPartitionKeyTranslators();
            Assert.assertNotNull(registry);
            // Only 'a' is interned; NULL keeps SymbolTable.VALUE_IS_NULL and is never interned.
            Assert.assertEquals(1, registry.getTotalDictionarySize());

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym\ts
                            2024-01-01T00:00:00.000000Z\ta\t1.0
                            2024-01-01T00:00:01.000000Z\t\t2.0
                            2024-01-01T00:00:02.000000Z\ta\t4.0
                            2024-01-01T00:00:03.000000Z\t\t6.0
                            """
            );
        });
    }

    @Test
    public void testMixedDirectAndExpressionKeyCompilesAndTranslatesOnlyTheDirectTerm() throws Exception {
        // The regression this test guards: before step 11, this exact shape compiled at
        // CREATE time (validated on a plain context that never carries a translator) and
        // then failed every refresh cycle once a real translator was bound, retried forever,
        // and never advanced - all while live_views() kept reporting it as active. Driving a
        // refresh to quiescence and asserting real progress is the proof it no longer does.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym1 SYMBOL, sym2 SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym1, sym2, sum(x) OVER (PARTITION BY sym1, lower(sym2) ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");
            execute("INSERT INTO base VALUES "
                    + "('2024-01-01T00:00:00.000000Z', 'a', 'P', 1), "
                    + "('2024-01-01T00:00:01.000000Z', 'a', 'p', 2), "
                    + "('2024-01-01T00:00:02.000000Z', 'a', 'Q', 3)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            Assert.assertArrayEquals(new int[]{ColumnType.SYMBOL, ColumnType.STRING}, checkpointKeyColumnTypes());

            final LiveViewSymbolIdRegistry registry = viewInstance().getPartitionKeyTranslators();
            Assert.assertNotNull(registry);
            // sym1's dictionary interns 'a'; lower(sym2) is not a base column and builds none.
            Assert.assertEquals(1, registry.getBoundSlotCount());
            Assert.assertEquals(1, registry.getTotalDictionarySize());

            final LiveViewInstance instance = viewInstance();
            Assert.assertFalse("a mixed direct/expression key must not invalidate the view", instance.isInvalid());
            Assert.assertTrue(
                    "the refresh must actually advance rather than stall retrying the same compile",
                    instance.getLastProcessedSeqTxn() > 0
            );

            // 'P' and 'p' lower-case to the same key, 'Q' does not.
            assertQuery("SELECT ts, sym1, sym2, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym1\tsym2\ts
                            2024-01-01T00:00:00.000000Z\ta\tP\t1.0
                            2024-01-01T00:00:01.000000Z\ta\tp\t3.0
                            2024-01-01T00:00:02.000000Z\ta\tQ\t3.0
                            """
            );
        });
    }

    @Test
    public void testMixedKeyRepairKeepsReaderLocalSinkUntranslated() throws Exception {
        // The concrete hazard section 12 step 11 exists to prevent: LiveViewCheckpointRowsBounds
        // .createKey/findKey read the projector's reader-local sink unconditionally during a
        // ROWS-bound repair, and expressionKeyProjector used to compile one sink for both the
        // reader-local and checkpoint accessors. If that sink translated sym1, the repair would
        // key its scratch map through an LV-private id instead of the reader's own table-local
        // one - in range for the map either way, so a wrong answer here would not throw, only
        // compute the wrong sums. Driving an out-of-order insert into a mixed-key ROWS window
        // and checking the corrected output is the end-to-end proof the two sinks stayed apart.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym1 SYMBOL, sym2 SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym1, sym2, sum(x) OVER (PARTITION BY sym1, lower(sym2) ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:00.000000Z', 'a', 'p', 1), "
                        + "('2024-01-01T00:00:02.000000Z', 'a', 'p', 2), "
                        + "('2024-01-01T00:00:04.000000Z', 'a', 'q', 3)");
                driveRefreshToQuiescence(job);

                // Strictly below the head, so the refresh diverts to a repair rather than
                // appending - the ROWS-bound discovery path that reads getKeySink().
                execute("INSERT INTO base VALUES ('2024-01-01T00:00:01.000000Z', 'a', 'p', 10)");
                driveRefreshToQuiescence(job);
            }

            Assert.assertArrayEquals(new int[]{ColumnType.SYMBOL, ColumnType.STRING}, checkpointKeyColumnTypes());

            // The repair produced the same view a from-base recompute would.
            assertQuery("SELECT ts, sym1, sym2, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym1\tsym2\ts
                            2024-01-01T00:00:00.000000Z\ta\tp\t1.0
                            2024-01-01T00:00:01.000000Z\ta\tp\t11.0
                            2024-01-01T00:00:02.000000Z\ta\tp\t13.0
                            2024-01-01T00:00:04.000000Z\ta\tq\t3.0
                            """
            );
        });
    }

    @Test
    public void testRestartRestoresTranslatedPartitionKeys() throws Exception {
        // A same-string post-restart row would pass even if the dictionary were not restored
        // at all, as long as the fresh registry happens to re-intern that string to the same
        // id the old one gave it - id 0 for the first string either way. That is exactly the
        // failure this test is built to catch, so it deliberately does not rely on it: 'c'/'r'
        // is a string neither dictionary has ever seen, so if restore left the registry empty
        // instead of reloading it, 'c' would take a *fresh* id 0 in the post-restart
        // dictionary - colliding with 'a's id 0 in the *restored* anchor map, which still
        // holds keys from the pre-restart numbering - and 'c's row would silently continue
        // 'a's running sum instead of starting its own. The 'b'/'q' row is the same check from
        // the other direction: a string that already had a *non-zero* id pre-restart, whose
        // post-restart sum must still continue from the value the restored map held rather
        // than start over.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym1 SYMBOL, sym2 SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym1, sym2, sum(x) OVER (PARTITION BY sym1, sym2 ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");

            final long preHeadLvSeqTxn;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:00.000000Z', 'a', 'p', 1), "
                        + "('2024-01-01T00:00:01.000000Z', 'b', 'q', 2)");
                driveRefreshToQuiescence(job);

                final LiveViewInstance instance = viewInstance();
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals("a head checkpoint must have been written pre-restart", Numbers.LONG_NULL, preHeadLvSeqTxn);
                final LiveViewSymbolIdRegistry registry = instance.getPartitionKeyTranslators();
                Assert.assertNotNull(registry);
                Assert.assertEquals(4, registry.getTotalDictionarySize());
            }

            // Simulate restart: clear the in-memory registry and rebuild from on-disk state.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:02.000000Z', 'b', 'q', 20), "
                        + "('2024-01-01T00:00:03.000000Z', 'c', 'r', 100)");
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, sym1, sym2, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym1\tsym2\ts
                            2024-01-01T00:00:00.000000Z\ta\tp\t1.0
                            2024-01-01T00:00:01.000000Z\tb\tq\t2.0
                            2024-01-01T00:00:02.000000Z\tb\tq\t22.0
                            2024-01-01T00:00:03.000000Z\tc\tr\t100.0
                            """
            );
        });
    }

    private static int[] checkpointKeyColumnTypes() {
        final WindowRecordCursorFactory windowFactory = viewInstance().getCompiledPlan().getWindowFactory();
        final LiveViewCheckpointKeyProjector projector = windowFactory.getCheckpointKeyProjector();
        Assert.assertNotNull("a live view with a partition key must have a checkpoint projector", projector);
        final int n = projector.getCheckpointKeyColumnTypes().getColumnCount();
        final int[] types = new int[n];
        for (int i = 0; i < n; i++) {
            types[i] = ColumnType.tagOf(projector.getCheckpointKeyColumnTypes().getColumnType(i));
        }
        return types;
    }

    private static LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }
}
