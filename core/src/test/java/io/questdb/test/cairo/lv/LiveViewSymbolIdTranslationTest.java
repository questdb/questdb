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
 * What changes once a live view's direct SYMBOL partition terms actually key by an LV-private
 * id rather than by their resolved string: the checkpoint key schema becomes SYMBOL, a
 * PARTITION BY mixing a direct SYMBOL term with an expression term keys the two in different
 * domains, and every id survives a restart through the durable dictionary rather than being
 * re-minted from zero.
 * <p>
 * {@link LiveViewSymbolIdBindingTest} covers stage-2 binding against real views, unaffected by
 * either step; this covers what changes once a translator actually moves a key.
 */
public class LiveViewSymbolIdTranslationTest extends AbstractLiveViewTest {

    @Test
    public void testAnO3CorrectionTheFilterRejectsNeverGrowsTheDictionary() throws Exception {
        // An out-of-order correction is decomposed into a change set before anything decides
        // whether its rows survive the repair bounds and the view's own filter, and that walk
        // deliberately interns nothing: a dictionary is durable and append-only, so an id
        // spent on a key the repair never replays is spent for the life of the view. The
        // correction below carries a symbol the view has never seen, on a row the filter
        // rejects, which is the shape that would grow the dictionary by a key no map or root
        // will ever hold.
        //
        // The second correction is the control. Without it a decomposition that interned
        // nothing because it never ran at all would pass just as well.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s "
                    + "FROM base WHERE x > 100");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:00.000000Z', 'a', 200), "
                        + "('2024-01-01T00:00:04.000000Z', 'a', 400)");
                driveRefreshToQuiescence(job);

                final LiveViewSymbolIdRegistry registry = viewInstance().getPartitionKeyTranslators();
                Assert.assertNotNull(registry);
                Assert.assertEquals(1, registry.getTotalDictionarySize());

                execute("INSERT INTO base VALUES ('2024-01-01T00:00:02.000000Z', 'rejected', 1)");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "a correction whose rows the filter rejects must not grow the dictionary",
                        1,
                        registry.getTotalDictionarySize()
                );

                execute("INSERT INTO base VALUES ('2024-01-01T00:00:03.000000Z', 'admitted', 800)");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "a correction whose rows the filter admits must intern its key exactly once",
                        2,
                        registry.getTotalDictionarySize()
                );
            }

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym\ts
                            2024-01-01T00:00:00.000000Z\ta\t200.0
                            2024-01-01T00:00:03.000000Z\tadmitted\t800.0
                            2024-01-01T00:00:04.000000Z\ta\t600.0
                            """
            );
        });
    }

    @Test
    public void testCompositeKeyAcrossManyTransactionsKeepsWalLocalIdsApart() throws Exception {
        // The WAL writer restarts its local symbol numbering per transaction, so raw id 0
        // names 'a' in the first transaction below and 'b' in the second. The translator's
        // dirty band is stamped with the transaction that armed it for exactly this reason,
        // and a band that leaked across the boundary would give both strings one lv id and
        // merge two partitions' running sums with nothing to notice by - every row still
        // present, every sum quietly wrong.
        //
        // The six transactions are drained in one refresh pass, which is the arrangement that
        // exercises the boundary: a pass per transaction would re-arm from scratch each time
        // and never carry a band forward at all.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym1 SYMBOL, sym2 SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym1, sym2, sum(x) OVER (PARTITION BY sym1, sym2 ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");

            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 'a', 'p', 1)");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:01.000000Z', 'b', 'q', 2)");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:02.000000Z', 'c', 'r', 4)");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:03.000000Z', 'a', 'p', 8)");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:04.000000Z', 'b', 'r', 16)");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:05.000000Z', 'c', 'q', 32)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            final LiveViewSymbolIdRegistry registry = viewInstance().getPartitionKeyTranslators();
            Assert.assertNotNull(registry);
            // Three distinct strings per bound column, each interned once however many
            // transactions repeated it.
            Assert.assertEquals(6, registry.getTotalDictionarySize());

            assertQuery("SELECT ts, sym1, sym2, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym1\tsym2\ts
                            2024-01-01T00:00:00.000000Z\ta\tp\t1.0
                            2024-01-01T00:00:01.000000Z\tb\tq\t2.0
                            2024-01-01T00:00:02.000000Z\tc\tr\t4.0
                            2024-01-01T00:00:03.000000Z\ta\tp\t9.0
                            2024-01-01T00:00:04.000000Z\tb\tr\t16.0
                            2024-01-01T00:00:05.000000Z\tc\tq\t32.0
                            """
            );
        });
    }

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
        // The regression this test guards: this exact shape used to compile at
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
        // The concrete hazard the two sinks stay separate objects to prevent:
        // LiveViewCheckpointRowsBounds
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
    public void testNullInEachPositionOfACompositeKeySurvivesARestart() throws Exception {
        // NULL is not interned - it keeps SymbolTable.VALUE_IS_NULL through the translator,
        // the runtime map, the checkpoint key and the durable dictionary - so a composite key
        // has to carry it in either position, and in both at once, without either colliding
        // with an interned id or being minted one of its own. Id 0 is what a NULL misread as
        // an ordinary raw id would land on, and id 0 is exactly what the first interned string
        // in each column holds, so a collision here merges ('a', 'p') with (null, 'p') rather
        // than failing.
        //
        // The restart is what extends this past the runtime map: the encoded key travels
        // through the checkpoint and comes back, and each of the four combinations continues
        // its own running sum rather than starting over or continuing someone else's.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym1 SYMBOL, sym2 SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym1, sym2, sum(x) OVER (PARTITION BY sym1, sym2 ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:00.000000Z', null, 'p', 1), "
                        + "('2024-01-01T00:00:01.000000Z', 'a', null, 2), "
                        + "('2024-01-01T00:00:02.000000Z', null, null, 4), "
                        + "('2024-01-01T00:00:03.000000Z', 'a', 'p', 8)");
                driveRefreshToQuiescence(job);

                final LiveViewSymbolIdRegistry registry = viewInstance().getPartitionKeyTranslators();
                Assert.assertNotNull(registry);
                // 'a' in one dictionary and 'p' in the other; the three NULLs intern nothing.
                Assert.assertEquals(2, registry.getTotalDictionarySize());
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:04.000000Z', null, 'p', 16), "
                        + "('2024-01-01T00:00:05.000000Z', 'a', null, 32), "
                        + "('2024-01-01T00:00:06.000000Z', null, null, 64), "
                        + "('2024-01-01T00:00:07.000000Z', 'a', 'p', 128)");
                driveRefreshToQuiescence(job);

                final LiveViewSymbolIdRegistry registry = viewInstance().getPartitionKeyTranslators();
                Assert.assertNotNull(registry);
                Assert.assertEquals(
                        "the restored dictionary must hold the ids it held, and mint no new ones",
                        2,
                        registry.getTotalDictionarySize()
                );
            }

            assertQuery("SELECT ts, sym1, sym2, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym1\tsym2\ts
                            2024-01-01T00:00:00.000000Z\t\tp\t1.0
                            2024-01-01T00:00:01.000000Z\ta\t\t2.0
                            2024-01-01T00:00:02.000000Z\t\t\t4.0
                            2024-01-01T00:00:03.000000Z\ta\tp\t8.0
                            2024-01-01T00:00:04.000000Z\t\tp\t17.0
                            2024-01-01T00:00:05.000000Z\ta\t\t34.0
                            2024-01-01T00:00:06.000000Z\t\t\t68.0
                            2024-01-01T00:00:07.000000Z\ta\tp\t136.0
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

    @Test
    public void testRowsTheResidualFilterRejectsNeverGrowTheDictionary() throws Exception {
        // The dictionary is durable and append-only: nothing ever reclaims an id, so an id
        // spent on a string no published map or root will ever hold is spent for the life of
        // the view. A view whose residual filter admits a small share of a high-cardinality
        // stream is the shape where that matters - interning at the base scan rather than at
        // the key would grow the dictionary by every distinct value the base carries instead
        // of by the ones the view keys by.
        //
        // Interning is lazy and happens where the key is written, which is above the filter,
        // so a rejected row never reaches it. This pins that, in both directions: a
        // transaction that is entirely rejected grows the dictionary by nothing, and a mixed
        // one grows it only by what survived.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s "
                    + "FROM base WHERE x > 100");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:00.000000Z', 'kept', 200), "
                        + "('2024-01-01T00:00:01.000000Z', 'dropped-1', 1), "
                        + "('2024-01-01T00:00:02.000000Z', 'dropped-2', 2)");
                driveRefreshToQuiescence(job);

                final LiveViewSymbolIdRegistry registry = viewInstance().getPartitionKeyTranslators();
                Assert.assertNotNull(registry);
                Assert.assertEquals(
                        "only the key the filter admitted may be interned",
                        1,
                        registry.getTotalDictionarySize()
                );

                // A transaction the filter rejects whole.
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:03.000000Z', 'dropped-3', 3), "
                        + "('2024-01-01T00:00:04.000000Z', 'dropped-4', 4)");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "a transaction whose rows are all rejected must not grow the dictionary",
                        1,
                        registry.getTotalDictionarySize()
                );

                // And one where a subset survives.
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:05.000000Z', 'dropped-5', 5), "
                        + "('2024-01-01T00:00:06.000000Z', 'kept-2', 300)");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "a mixed transaction must grow the dictionary by what survived alone",
                        2,
                        registry.getTotalDictionarySize()
                );
            }

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym\ts
                            2024-01-01T00:00:00.000000Z\tkept\t200.0
                            2024-01-01T00:00:06.000000Z\tkept-2\t300.0
                            """
            );
        });
    }

    @Test
    public void testTruncateOnTheBasePreservesTheDictionary() throws Exception {
        // A WAL TRUNCATE applies through TableWriter.removeAllPartitions, which preserves the
        // base's symbol files rather than resetting them, and the live view's own
        // freeze-and-continue semantics keep its derived state
        // (LiveViewSmokeTest.testTruncateOnBaseIsTransparentToLiveView is the end-to-end
        // baseline for that). The durable ids have to follow: the runtime maps and every
        // sealed root still hold keys in the pre-truncate numbering, so an id renumbered or
        // re-minted here reads a partition it never named.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:00.000000Z', 'a', 1), "
                        + "('2024-01-01T00:00:01.000000Z', 'b', 2)");
                driveRefreshToQuiescence(job);

                final LiveViewSymbolIdRegistry registry = viewInstance().getPartitionKeyTranslators();
                Assert.assertNotNull(registry);
                Assert.assertEquals(2, registry.getTotalDictionarySize());

                execute("TRUNCATE TABLE base");
                driveRefreshToQuiescence(job);
                Assert.assertFalse("a base TRUNCATE must not invalidate a translated view", viewInstance().isInvalid());
                Assert.assertEquals(
                        "a base TRUNCATE must leave the LV-private dictionary exactly as it was",
                        2,
                        registry.getTotalDictionarySize()
                );

                // The base's own symbol numbering starts over here as far as the WAL writer is
                // concerned, but 'a' is a string the dictionary already holds, so it resolves
                // to the id it already had and its running sum continues rather than restarts.
                execute("INSERT INTO base VALUES ('2024-01-01T00:00:02.000000Z', 'a', 4)");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "a string the dictionary already holds must not be interned twice",
                        2,
                        registry.getTotalDictionarySize()
                );
            }

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym\ts
                            2024-01-01T00:00:00.000000Z\ta\t1.0
                            2024-01-01T00:00:01.000000Z\tb\t2.0
                            2024-01-01T00:00:02.000000Z\ta\t5.0
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
