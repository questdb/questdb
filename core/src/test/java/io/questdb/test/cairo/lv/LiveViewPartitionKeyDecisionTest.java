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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.lv.LiveViewCheckpointKeyProjector;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewPartitionKeyDecision;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewSymbolIdRegistry;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

/**
 * The partition-key binding decision a live view is created with, and what honoring it on
 * every later compile buys.
 * <p>
 * Whether a term keys as an LV-private symbol id is a property of the build, not of the SQL:
 * it takes a classifier that admits the term and a compiled plan that can trace it to a base
 * SYMBOL column, and both widen and narrow between releases. The answer decides a key type
 * that the checkpoint's persisted key schema records, so a build that classifies one term
 * differently makes the schema on disk disagree with the compiled runtime - which restore
 * validation catches, and pays for with a full rebuild from the base table. So CREATE decides
 * once, {@code _lv} keeps the answer, and every later compile honors it.
 * <p>
 * The end-to-end cases here simulate that upgrade by rewriting {@code _lv} with a different
 * decision and restarting, which is the only way to reach from one build's classification to
 * another's inside a single build.
 */
public class LiveViewPartitionKeyDecisionTest extends AbstractLiveViewTest {

    @Test
    public void testCreatePersistsCompositeKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym1 SYMBOL, sym2 SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym1, sym2, sum(x) OVER (PARTITION BY sym1, sym2 ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");

            assertPersistedDecision("sym1", "sym2");
        });
    }

    @Test
    public void testCreatePersistsDirectSymbolTerm() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");

            // CREATE compiles on a plain context that carries no translator, so nothing keys
            // as an id there - the decision is what that compile would have bound had one
            // been armed, which is exactly what the first refresh compile does arm.
            assertPersistedDecision("sym");
        });
    }

    @Test
    public void testExpressionKeyPersistsAnEmptyDecisionRatherThanNone() throws Exception {
        // An empty decision is a decision: it says this view translates nothing. A view that
        // persisted nothing at all would re-derive instead, and re-deriving is what the
        // decision exists to stop.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, sum(x) OVER (PARTITION BY lower(sym) ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");

            assertPersistedDecision();
        });
    }

    @Test
    public void testMixedKeyPersistsOnlyTheDirectTerm() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym1 SYMBOL, sym2 SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym1, sym2, sum(x) OVER (PARTITION BY sym1, lower(sym2) ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");

            assertPersistedDecision("sym1");
        });
    }

    @Test
    public void testPersistedDecisionKeepsATermOnTheKeyItWasCreatedWith() throws Exception {
        // The upgrade this exists to survive, run backwards: a view created by a build that
        // keyed nothing meets one that would key `sym` as an id. Honoring the decision keeps
        // the key it was created with, so the view's own checkpoint still describes it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");
            rewriteDecision(LiveViewPartitionKeyDecision.NOTHING_TRANSLATES);
            restart();

            execute("INSERT INTO base VALUES "
                    + "('2024-01-01T00:00:00.000000Z', 1, 'a'), "
                    + "('2024-01-01T00:00:01.000000Z', 2, 'a'), "
                    + "('2024-01-01T00:00:02.000000Z', 3, 'b')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            Assert.assertArrayEquals(new int[]{ColumnType.STRING}, checkpointKeyColumnTypes());
            final LiveViewSymbolIdRegistry registry = viewInstance().getPartitionKeyTranslators();
            Assert.assertNotNull("every live view compile creates a registry, bound or not", registry);
            Assert.assertEquals("the persisted decision names nothing, so nothing binds", 0, registry.getBoundSlotCount());
            Assert.assertEquals(0, registry.getTotalDictionarySize());
            Assert.assertFalse(viewInstance().isInvalid());

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym\ts
                            2024-01-01T00:00:00.000000Z\ta\t1.0
                            2024-01-01T00:00:01.000000Z\ta\t3.0
                            2024-01-01T00:00:02.000000Z\tb\t3.0
                            """
            );
        });
    }

    @Test
    public void testPersistedDecisionNamingATermTheViewNoLongerKeysBindsNothingExtra() throws Exception {
        // A decision that outlived the term it names - the SELECT still projects the column,
        // but no PARTITION BY term reads it any more. The allow-list only narrows what the
        // classifier admits, so the stale name buys no binding of its own and the term that
        // is still keyed keeps translating. The refresh path logs the gap, because the
        // checkpoint schema this view goes on to write no longer matches its predecessor's.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym1 SYMBOL, sym2 SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym1, sym2, sum(x) OVER (PARTITION BY sym1 ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");
            final ObjList<String> stale = new ObjList<>();
            stale.add("sym1");
            stale.add("sym2");
            rewriteDecision(LiveViewPartitionKeyDecision.of(stale));
            restart();

            execute("INSERT INTO base VALUES "
                    + "('2024-01-01T00:00:00.000000Z', 'a', 'p', 1), "
                    + "('2024-01-01T00:00:01.000000Z', 'a', 'q', 2)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            Assert.assertArrayEquals(new int[]{ColumnType.SYMBOL}, checkpointKeyColumnTypes());
            final LiveViewSymbolIdRegistry registry = viewInstance().getPartitionKeyTranslators();
            Assert.assertNotNull(registry);
            Assert.assertEquals("only the column an actual term keys by binds", 1, registry.getBoundSlotCount());
            Assert.assertFalse(viewInstance().isInvalid());

            assertQuery("SELECT ts, sym1, sym2, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym1\tsym2\ts
                            2024-01-01T00:00:00.000000Z\ta\tp\t1.0
                            2024-01-01T00:00:01.000000Z\ta\tq\t3.0
                            """
            );
        });
    }

    @Test
    public void testViewWithNoPersistedDecisionStillClassifiesFromScratch() throws Exception {
        // A view created before the decision was persisted carries no block at all, and that
        // is not an empty decision: it means re-derive, which is what such a view has always
        // done and has to keep doing.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");
            rewriteDecision(null);
            restart();
            Assert.assertNull(readDefinition().getPartitionKeyDecision());

            execute("INSERT INTO base VALUES "
                    + "('2024-01-01T00:00:00.000000Z', 1, 'a'), "
                    + "('2024-01-01T00:00:01.000000Z', 2, 'a')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            Assert.assertArrayEquals(new int[]{ColumnType.SYMBOL}, checkpointKeyColumnTypes());
            final LiveViewSymbolIdRegistry registry = viewInstance().getPartitionKeyTranslators();
            Assert.assertNotNull(registry);
            Assert.assertEquals(1, registry.getBoundSlotCount());

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").timestamp("ts").expectSize().returns(
                    """
                            ts\tsym\ts
                            2024-01-01T00:00:00.000000Z\ta\t1.0
                            2024-01-01T00:00:01.000000Z\ta\t3.0
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

    private void assertPersistedDecision(String... expectedColumnNames) {
        final LiveViewPartitionKeyDecision decision = readDefinition().getPartitionKeyDecision();
        Assert.assertNotNull("a view created by this build always persists a decision", decision);
        Assert.assertEquals(expectedColumnNames.length, decision.getColumnCount());
        for (int i = 0; i < expectedColumnNames.length; i++) {
            Assert.assertEquals(expectedColumnNames[i], decision.getColumnName(i));
            Assert.assertTrue(decision.isTranslated(expectedColumnNames[i]));
        }
    }

    /**
     * Reads the view's on-disk {@code _lv}. The metadata argument is the view's own output
     * shape, which {@code _lv} does not carry and nothing here reads, so an empty one serves.
     */
    private LiveViewDefinition readDefinition() {
        final TableToken viewToken = engine.verifyTableName("lv");
        try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
            path.of(configuration.getDbRoot());
            return LiveViewDefinition.readFrom(
                    reader,
                    path,
                    path.size(),
                    viewToken,
                    engine.verifyTableName("base"),
                    new GenericRecordMetadata()
            );
        }
    }

    /**
     * Rewrites {@code _lv} with a different partition-key decision, standing in for the view
     * having been created by a build whose classifier answered differently. Everything else
     * in the definition round-trips unchanged.
     */
    private void rewriteDecision(@Nullable LiveViewPartitionKeyDecision decision) {
        final LiveViewDefinition current = readDefinition();
        final LiveViewDefinition rewritten = new LiveViewDefinition(
                current.getViewName(),
                current.getViewSql(),
                current.getBaseTableName(),
                current.getBaseTableToken(),
                current.getBaseTimestampType(),
                current.getFlushEveryInterval(),
                current.getFlushEveryIntervalUnit(),
                current.getInMemoryInterval(),
                current.getInMemoryIntervalUnit(),
                current.getPartitionBy(),
                current.getViewLowerBoundTimestamp(),
                current.getStartFromKind(),
                current.getAnchorSpec(),
                current.getDependencyColumnNames(),
                current.getDependencyColumnTypes(),
                decision,
                current.getMetadata()
        );
        final TableToken viewToken = engine.verifyTableName("lv");
        try (
                Path path = new Path();
                BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode())
        ) {
            writer.of(path.of(configuration.getDbRoot()).concat(viewToken)
                    .concat(LiveViewDefinition.LIVE_VIEW_DEFINITION_FILE_NAME).$());
            LiveViewDefinition.append(rewritten, writer);
        }
    }

    /**
     * Simulated restart: drop the in-memory registry and rebuild it from the on-disk
     * {@code _lv} / {@code _lv.s}, which is the path startup takes.
     */
    private void restart() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }
}
