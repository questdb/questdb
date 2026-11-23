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

package io.questdb.test;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlCompiler;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A server's boot loads the materialized views before it starts the WAL apply: a definition event of ALTER MATERIALIZED
 * VIEW ... AS that a view's WAL holds unapplied at the boot waits as on the node that ran the statement.
 */
public class ServerMainMatViewBootTest extends AbstractBootstrapTest {
    private static final Log LOG = LogFactory.getLog(ServerMainMatViewBootTest.class);
    private static final String MAX_SQL = "SELECT ts, max(x) AS s FROM base SAMPLE BY 1h";
    private static final String SUM_SQL = "SELECT ts, sum(x) AS s FROM base SAMPLE BY 1h";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testBootRecordsPendingDefinitionBeforeWalApplyStarts() throws Exception {
        // The second server runs ALTER ... AS on a view in cairo.wal.apply.suspended.tables, so its definition event
        // waits in the view's WAL, and the refresh of the rows after it defers. The third server, without the list,
        // records the event as pending at its boot, before its WAL apply applies it on the state the boot created, which
        // wakes the refresh: the rows after the statement take the new SQL's maximum, not the old one's sum.
        TestUtils.assertMemoryLeak(() -> {
            final String viewDirName;
            try (TestServerMain serverMain = startWithEnvVariables(PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true")) {
                final CairoEngine engine = serverMain.getEngine();
                serverMain.execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO base VALUES ('2024-01-01T01:10:00.000000Z', 1.0)");
                serverMain.awaitTxn("base", 1);
                serverMain.execute("CREATE MATERIALIZED VIEW v AS (" + SUM_SQL + ") PARTITION BY DAY");
                awaitRefreshed(engine, 1);
                viewDirName = engine.verifyTableName("v").getDirName();
            }

            final long definitionSeqTxn;
            try (TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true",
                    PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES.getEnvVarName(), viewDirName
            )) {
                final CairoEngine engine = serverMain.getEngine();
                final TableToken viewToken = engine.verifyTableName("v");
                serverMain.execute("ALTER MATERIALIZED VIEW v AS (" + MAX_SQL + ")");
                definitionSeqTxn = engine.getTableSequencerAPI().lastTxn(viewToken);
                final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
                Assert.assertEquals(definitionSeqTxn, state.getPendingDefinitionSeqTxn());
                serverMain.execute("INSERT INTO base VALUES ('2024-01-01T02:10:00.000000Z', 1.0), ('2024-01-01T02:20:00.000000Z', 2.0), ('2024-01-01T02:30:00.000000Z', 3.0)");
                serverMain.awaitTxn("base", 2);
                // the refresh of the rows deferred, and wrote nothing after the definition event
                TestUtils.assertEventually(() -> Assert.assertTrue(state.hasDeferredDefinitionRefreshForTesting()));
                Assert.assertEquals(definitionSeqTxn, engine.getTableSequencerAPI().lastTxn(viewToken));
                Assert.assertTrue(state.isDefinitionPending());
            }

            try (TestServerMain serverMain = startWithEnvVariables(PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true")) {
                final CairoEngine engine = serverMain.getEngine();
                final TableToken viewToken = engine.verifyTableName("v");
                final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
                // the boot recorded the event before the WAL apply started; the record never lowers, so this read races
                // nothing
                Assert.assertEquals(definitionSeqTxn, state.getPendingDefinitionSeqTxn());
                TestUtils.assertEventually(() -> Assert.assertEquals(definitionSeqTxn, state.getAppliedDefinitionSeqTxnForTesting()));
                awaitRefreshed(engine, 2);
                serverMain.assertSql("v", """
                        ts\ts
                        2024-01-01T01:00:00.000000Z\t1.0
                        2024-01-01T02:00:00.000000Z\t3.0
                        """);
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    TestUtils.assertSqlCursors(compiler, serverMain.getSqlExecutionContext(), MAX_SQL, "v", LOG);
                }
                serverMain.assertSql(
                        "SELECT view_sql, view_status FROM materialized_views() WHERE view_name = 'v'",
                        "view_sql\tview_status\n" + MAX_SQL + "\tvalid\n"
                );
            }
        });
    }

    // Waits until the view v refreshed up to the base txn and its WAL applied everything.
    private static void awaitRefreshed(CairoEngine engine, long baseTxn) throws Exception {
        final TableToken viewToken = engine.verifyTableName("v");
        TestUtils.assertEventually(() -> {
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            final SeqTxnTracker viewTxnTracker = engine.getTableSequencerAPI().getTxnTracker(viewToken);
            Assert.assertNotNull(state);
            Assert.assertEquals(baseTxn, state.getLastRefreshBaseTxn());
            Assert.assertEquals(engine.getTableSequencerAPI().lastTxn(viewToken), viewTxnTracker.getWriterTxn());
        });
    }
}
