/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.view;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.Path;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertContains;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class ViewInvalidationTest extends AbstractViewTest {

    @Test
    public void testBrokenViewsAreInvalidatedRecursively() throws Exception {
        setCurrentMicros(1750345200000000L);

        String viewQuery1 = "select ts, k, max(v) as value from " + TABLE1 + " where v > 4";
        String viewQuery2 = "select ts, k, min(v) as value from " + TABLE2 + " where v > 6";
        String viewQuery3 = VIEW1 + " union " + VIEW2;
        String breakingSql = "RENAME TABLE " + TABLE1 + " TO " + TABLE3;
        String fixingSql = "RENAME TABLE " + TABLE3 + " TO " + TABLE1;
        String expectedErrorMessage = "table does not exist [table=" + TABLE1 + "]";

        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            // create views
            createView(VIEW1, viewQuery1);
            createView(VIEW2, viewQuery2);
            createView(VIEW3, viewQuery3);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n" +
                            "view1\t" + viewQuery1 + "\tview1~3\t\tvalid\t2025-06-19T15:00:00.000000Z\n" +
                            "view2\t" + viewQuery2 + "\tview2~4\t\tvalid\t2025-06-19T15:00:00.000000Z\n" +
                            "view3\t" + viewQuery3 + "\tview3~5\t\tvalid\t2025-06-19T15:00:00.000000Z\n",
                    "views() order by view_name",
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            Encode sort
                              keys: [view_name]
                                views()
                            """
            );

            compileView(VIEW1);
            compileView(VIEW2);
            compileView(VIEW3);
            drainViewQueue();

            setCurrentMicros(1750345201000000L);

            // breaking views by renaming the table
            execute(breakingSql);
            drainWalQueue();

            // automatic view invalidation is switched off in test, so have to query the view
            // to be able to detect that it does not work anymore
            // it is enough to detect the problem with VIEW1, because we invalidate all dependent
            // views recursively too, and VIEW3 depends on VIEW1
            detectInvalidView(VIEW1, expectedErrorMessage);
            drainViewQueue();

            assertViewDefinition(VIEW1, viewQuery1);
            assertViewDefinitionFile(VIEW1, viewQuery1);
            assertViewState(VIEW1, expectedErrorMessage);

            assertViewDefinition(VIEW2, viewQuery2);
            assertViewDefinitionFile(VIEW2, viewQuery2);
            assertViewState(VIEW2);

            assertViewDefinition(VIEW3, viewQuery3);
            assertViewDefinitionFile(VIEW3, viewQuery3);
            assertViewState(VIEW3, expectedErrorMessage);

            assertQueryAndPlan(
                    """
                            view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time
                            view1\tselect ts, k, max(v) as value from table1 where v > 4\tview1~3\ttable does not exist [table=table1]\tinvalid\t2025-06-19T15:00:01.000000Z
                            view2\tselect ts, k, min(v) as value from table2 where v > 6\tview2~4\t\tvalid\t2025-06-19T15:00:00.000000Z
                            view3\tview1 union view2\tview3~5\ttable does not exist [table=table1]\tinvalid\t2025-06-19T15:00:01.000000Z
                            """,
                    "views() order by view_name",
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            Encode sort
                              keys: [view_name]
                                views()
                            """
            );

            compileView(VIEW1, expectedErrorMessage);
            compileView(VIEW2);
            compileView(VIEW3, expectedErrorMessage);
            drainViewQueue();

            setCurrentMicros(1750345205000000L);

            // fixing views by rename the table back to the original name
            execute(fixingSql);
            drainWalQueue();

            // automatic view reset is switched off in test, so have to compile
            // the view manually to be able to fix it.
            // it is enough to compile VIEW1, because VIEW3 is dependent on it, and if
            // VIEW1 compiles successfully, its children will be compiled recursively too
            fixInvalidView(VIEW1);
            drainViewQueue();

            assertViewDefinition(VIEW1, viewQuery1);
            assertViewDefinitionFile(VIEW1, viewQuery1);
            assertViewState(VIEW1);

            assertViewDefinition(VIEW2, viewQuery2);
            assertViewDefinitionFile(VIEW2, viewQuery2);
            assertViewState(VIEW2);

            assertViewDefinition(VIEW3, viewQuery3);
            assertViewDefinitionFile(VIEW3, viewQuery3);
            assertViewState(VIEW3);

            assertQueryAndPlan(
                    """
                            view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time
                            view1\tselect ts, k, max(v) as value from table1 where v > 4\tview1~3\t\tvalid\t2025-06-19T15:00:05.000000Z
                            view2\tselect ts, k, min(v) as value from table2 where v > 6\tview2~4\t\tvalid\t2025-06-19T15:00:01.000000Z
                            view3\tview1 union view2\tview3~5\t\tvalid\t2025-06-19T15:00:05.000000Z
                            """,
                    "views() order by view_name",
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            Encode sort
                              keys: [view_name]
                                views()
                            """
            );

            compileView(VIEW1);
            compileView(VIEW2);
            compileView(VIEW3);
        });
    }

    @Test
    public void testDroppedColumnInsideFunctionInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(sqrt(v)) as v_max from " + TABLE1 + " where v > 4",
                "ALTER TABLE " + TABLE1 + " DROP COLUMN v",
                "alter table " + TABLE1 + " add column v long",
                "Invalid column: v"
        );
    }

    @Test
    public void testDroppedColumnInsideOperationInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(5 * v) as v_max from " + TABLE1 + " where v > 4",
                "ALTER TABLE " + TABLE1 + " DROP COLUMN v",
                "alter table " + TABLE1 + " add column v long",
                "Invalid column: v"
        );
    }

    @Test
    public void testDroppedColumnInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4",
                "ALTER TABLE " + TABLE1 + " DROP COLUMN k",
                "alter table " + TABLE1 + " add column k symbol",
                "Invalid column: k"
        );
    }

    @Test
    public void testDroppedColumnInvalidatesViewAndBringItBackWithDifferentType() throws Exception {
        testViewInvalidated(
                "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4",
                "ALTER TABLE " + TABLE1 + " DROP COLUMN k",
                "alter table " + TABLE1 + " add column k varchar",
                "Invalid column: k",
                "{" +
                        "\"columnCount\":3," +
                        "\"columns\":[" +
                        "{\"index\":0,\"name\":\"ts\",\"type\":\"TIMESTAMP\"}," +
                        "{\"index\":1,\"name\":\"k\",\"type\":\"SYMBOL\"}," +
                        "{\"index\":2,\"name\":\"v_max\",\"type\":\"LONG\"}" +
                        "]," +
                        "\"timestampIndex\":-1" +
                        "}",
                "{" +
                        "\"columnCount\":3," +
                        "\"columns\":[" +
                        "{\"index\":0,\"name\":\"ts\",\"type\":\"TIMESTAMP\"}," +
                        "{\"index\":1,\"name\":\"k\",\"type\":\"VARCHAR\"}," +
                        "{\"index\":2,\"name\":\"v_max\",\"type\":\"LONG\"}" +
                        "]," +
                        "\"timestampIndex\":-1" +
                        "}"
        );
    }

    @Test
    public void testDroppedTableInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4",
                "DROP TABLE " + TABLE1,
                "create table if not exists " + TABLE1 +
                        " (ts timestamp, k symbol capacity 2048, k2 symbol capacity 512, v long)" +
                        " timestamp(ts) partition by day wal",
                "table does not exist [table=" + TABLE1 + "]"
        );
    }

    @Test
    public void testDroppedTableMixedCases() throws Exception {
        String viewQuery = "select ts, k, max(v) as v_max from " + TABLE1.toLowerCase() + " where v > 4";
        assertMemoryLeak(() -> {
            setCurrentMicros(1750327200000000L);

            createTable(TABLE1);

            // creating view
            createView(VIEW1, viewQuery);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n" +
                            "view1\t" + viewQuery + "\tview1~2\t\tvalid\t2025-06-19T10:00:00.000000Z\n",
                    "views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            views()
                            """
            );

            compileView(VIEW1);

            // breaking view
            execute("DROP TABLE " + TABLE1.toUpperCase());
            drainWalQueue();

            detectInvalidView(VIEW1, "table does not exist [table=" + TABLE1 + "]");
            drainViewQueue();

            assertViewDefinition(VIEW1, viewQuery);
            assertViewDefinitionFile(VIEW1, viewQuery);
            assertViewState(VIEW1, "table does not exist [table=" + TABLE1 + "]");

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n" +
                            "view1\t" + viewQuery + "\tview1~2\t" + "table does not exist [table=" + TABLE1 + "]" + "\tinvalid\t2025-06-19T10:00:00.000000Z\n",
                    "views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            views()
                            """
            );

            compileView(VIEW1, "table does not exist [table=" + TABLE1 + "]");

            // fixing view
            execute("create table if not exists " + TABLE1.toUpperCase() +
                    " (ts timestamp, k symbol capacity 2048, k2 symbol capacity 512, v long)" +
                    " timestamp(ts) partition by day wal");
            drainWalQueue();

            fixInvalidView(VIEW1);
            drainViewQueue();

            assertViewDefinition(VIEW1, viewQuery);
            assertViewDefinitionFile(VIEW1, viewQuery);
            assertViewState(VIEW1);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n" +
                            "view1\t" + viewQuery + "\tview1~2\t\tvalid\t2025-06-19T10:00:00.000000Z\n",
                    "views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            views()
                            """
            );

            compileView(VIEW1);

            execute("DROP VIEW " + VIEW1.toUpperCase());
            drainWalQueue();
            assertNull(getViewDefinition(VIEW1.toUpperCase()));
        });
    }

    @Test
    public void testPersistedLateralNegativeLimitViewFailsClosedAndCascades() throws Exception {
        // Upgrade-break regression (intended break): older binaries accepted a negative LIMIT in a
        // correlated lateral body and silently produced wrong rows -- compensateLimit rewrites the
        // LIMIT into `__lateral_rn <= limit`, which is unsatisfiable for a negative bound. This
        // binary rejects the shape, so a stored definition carrying it must fail visibly.
        assertPersistedLateralViewFailsClosed(
                "select t1.ts, t1.k, l.c from " + TABLE1 + " t1 left join lateral ("
                        + "select count() c from " + TABLE2 + " t2 where t2.k = t1.k limit -1) l on true",
                "negative LIMIT is not supported in a correlated lateral sub-query"
        );
    }

    @Test
    public void testPersistedLateralOuterColumnLimitViewFailsClosedAndCascades() throws Exception {
        // The other half of the narrowed scalar-count contract: a run-time LIMIT reading an outer
        // column cannot be evaluated in the outer projection, so the count compensation guard
        // cannot be built and the stored definition must be rejected rather than silently
        // dropping the compensation.
        assertPersistedLateralViewFailsClosed(
                "select t1.ts, t1.k, l.c from " + TABLE1 + " t1 left join lateral ("
                        + "select count() c from " + TABLE2 + " t2 where t2.k = t1.k limit t1.v) l on true",
                "LIMIT referencing an outer column is not supported over a scalar count "
                        + "in a correlated lateral sub-query; use a constant or bind variable"
        );
    }

    @Test
    public void testRenamedColumnInsideFunctionInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(sqrt(v)) as v_max from " + TABLE1 + " where v > 4",
                "ALTER TABLE " + TABLE1 + " RENAME COLUMN v to v_renamed",
                "ALTER TABLE " + TABLE1 + " RENAME COLUMN v_renamed to v",
                "Invalid column: v"
        );
    }

    @Test
    public void testRenamedColumnInsideOperationInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(5 * v) as v_max from " + TABLE1 + " where v > 4",
                "ALTER TABLE " + TABLE1 + " RENAME COLUMN v to v_renamed",
                "ALTER TABLE " + TABLE1 + " RENAME COLUMN v_renamed to v",
                "Invalid column: v"
        );
    }

    @Test
    public void testRenamedColumnInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4",
                "ALTER TABLE " + TABLE1 + " RENAME COLUMN k to k_renamed",
                "ALTER TABLE " + TABLE1 + " RENAME COLUMN k_renamed to k",
                "Invalid column: k"
        );
    }

    @Test
    public void testRenamedTableInvalidatesView() throws Exception {
        testViewInvalidated(
                "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4",
                "RENAME TABLE " + TABLE1 + " TO " + TABLE3,
                "RENAME TABLE " + TABLE3 + " TO " + TABLE1,
                "table does not exist [table=" + TABLE1 + "]"
        );
    }

    @Test
    public void testRenamedTableNonAsciiChars() throws Exception {
        assertMemoryLeak(() -> {
            final String TABLE1_1 = "Részvény_áíóúüűöő";
            final String TABLE1_2 = "RÉSZVÉNY_ÁÍÓÚÜŰÖŐ";
            final String TABLE2_1 = "Aкции_ягоды_жЙлФэЮ";
            final String TABLE2_2 = "AКЦИИ_ЯГОДЫ_ЖйЛфЭю";
            final String VIEW1 = "株式_ウォール街";

            setCurrentMicros(1750327200000000L);

            createTable(TABLE1_1);
            createTable(TABLE2_1);

            // creating view
            createView(VIEW1, "select ts, k, max(v) as v_max from (" + TABLE1_2 + " union " + TABLE2_2 + ") where v > 4");

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n" +
                            VIEW1 + "\tselect ts, k, max(v) as v_max from (" + TABLE1_2 + " union " + TABLE2_2 + ") where v > 4" + "\t" + VIEW1 + "~3\t\tvalid\t2025-06-19T10:00:00.000000Z\n",
                    "views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            views()
                            """
            );

            compileView(VIEW1);

            // breaking view
            execute("RENAME TABLE " + TABLE1_2 + " TO " + TABLE3);
            drainWalQueue();

            detectInvalidView(VIEW1, "table does not exist [table=" + TABLE1_2 + "]");
            drainViewQueue();

            assertViewDefinition(VIEW1, "select ts, k, max(v) as v_max from (" + TABLE1_2 + " union " + TABLE2_2 + ") where v > 4");
            assertViewDefinitionFile(VIEW1, "select ts, k, max(v) as v_max from (" + TABLE1_2 + " union " + TABLE2_2 + ") where v > 4");
            assertViewState(VIEW1, "table does not exist [table=" + TABLE1_2 + "]");

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n" +
                            VIEW1 + "\tselect ts, k, max(v) as v_max from (" + TABLE1_2 + " union " + TABLE2_2 + ") where v > 4" + "\t" + VIEW1 + "~3\t" + "table does not exist [table=" + TABLE1_2 + "]" + "\tinvalid\t2025-06-19T10:00:00.000000Z\n",
                    "views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            views()
                            """
            );

            compileView(VIEW1, "table does not exist [table=" + TABLE1_2 + "]");

            // fixing view
            execute("RENAME TABLE " + TABLE3 + " TO " + TABLE1_2);
            drainWalQueue();

            fixInvalidView(VIEW1);
            drainViewQueue();

            assertViewDefinition(VIEW1, "select ts, k, max(v) as v_max from (" + TABLE1_2 + " union " + TABLE2_2 + ") where v > 4");
            assertViewDefinitionFile(VIEW1, "select ts, k, max(v) as v_max from (" + TABLE1_2 + " union " + TABLE2_2 + ") where v > 4");
            assertViewState(VIEW1);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n" +
                            VIEW1 + "\t" + "select ts, k, max(v) as v_max from (" + TABLE1_2 + " union " + TABLE2_2 + ") where v > 4" + "\t" + VIEW1 + "~3\t\tvalid\t2025-06-19T10:00:00.000000Z\n",
                    "views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            views()
                            """
            );

            compileView(VIEW1);

            execute("DROP VIEW " + VIEW1.toUpperCase());
            drainWalQueue();
            assertNull(getViewDefinition(VIEW1.toUpperCase()));
        });
    }

    private void testViewInvalidated(String viewQuery, String breakingSql, String fixingSql, String expectedErrorMessage) throws Exception {
        testViewInvalidated(viewQuery, breakingSql, fixingSql, expectedErrorMessage, null, null);
    }

    /**
     * Installs view SQL that this binary can no longer accept through CREATE VIEW, the way an
     * upgrade leaves it: the definition is already on disk, written by an older binary. Goes
     * through the same engine entry point the WAL apply path uses, so the {@code _view} file is
     * rewritten and a recompile is enqueued exactly as it would be after a restart.
     */
    private static void installLegacyViewSql(String viewName, String legacySql) {
        final TableToken viewToken = engine.verifyTableName(viewName);
        final ViewDefinition current = engine.getViewGraph().getViewDefinition(viewToken);
        assertNotNull(current);
        try (
                BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                Path path = new Path()
        ) {
            engine.updateViewDefinition(
                    viewToken,
                    legacySql,
                    current.getDependencies(),
                    current.getSeqTxn() + 1,
                    writer,
                    path
            );
        }
    }

    /**
     * Pins the persisted plain-VIEW half of the lateral-guard upgrade contract: a stored definition
     * this binary rejects must fail <em>closed</em> -- invalid with the same stable message CREATE
     * surfaces, cascading to dependent views -- rather than compile to something wrong.
     */
    private void assertPersistedLateralViewFailsClosed(String legacyViewSql, String expectedErrorMessage) throws Exception {
        // Same column shape as the legacy body, so only the lateral guard can explain the failure.
        final String benignViewSql = "select t1.ts, t1.k, l.c from " + TABLE1 + " t1 left join lateral ("
                + "select count() c from " + TABLE2 + " t2 where t2.k = t1.k limit 1) l on true";

        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            createView(VIEW1, benignViewSql);
            // A two-level dependency chain: VIEW3 reads VIEW1, VIEW4 reads VIEW3.
            createView(VIEW3, "select ts, k, c from " + VIEW1);
            createView(VIEW4, "select ts, k, c from " + VIEW3);

            // CREATE rejects the shape outright on this binary, which is exactly why it can only
            // reach here as an already-persisted definition. Without this the test would go
            // vacuous the moment the guard is removed.
            try {
                execute("CREATE VIEW rejected_view AS (" + legacyViewSql + ")");
                fail("expected CREATE VIEW to reject the legacy lateral shape");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), expectedErrorMessage);
            }

            // Upgrade simulation: the stored SQL becomes the shape this binary rejects.
            installLegacyViewSql(VIEW1, legacyViewSql);
            assertViewDefinition(VIEW1, legacyViewSql);
            assertViewDefinitionFile(VIEW1, legacyViewSql);

            detectInvalidView(VIEW1, expectedErrorMessage);
            drainViewQueue();

            // Fails closed with the same message CREATE surfaces...
            assertViewState(VIEW1, expectedErrorMessage);
            // ...and every view downstream of it goes invalid with the same reason, at both
            // levels of the chain.
            //
            // These pin the operator-visible outcome and deliberately not the mechanism. View
            // dependencies are flattened transitively at create time -- VIEW4's dependency set
            // lists view1 directly, not just view3 -- so getDependentViews(VIEW1) returns both and
            // ViewCompilerJob#compileDependentViews recompiles both, each failing on its own
            // re-expansion of the broken body. ViewCompilerJob#invalidateDependentViews is
            // therefore redundant at every depth and no black-box route can isolate it; these
            // assertions still pass with it removed. They do fail if a compile failure stops
            // producing invalid state, which is the contract that matters here.
            assertViewState(VIEW3, expectedErrorMessage);
            assertViewState(VIEW4, expectedErrorMessage);

            // Operator-visible surface: views() reports both as invalid with the reason.
            assertQuery("select view_name, view_status, invalidation_reason from views() order by view_name")
                    .noLeakCheck()
                    .expectSize(false)
                    .returns("view_name\tview_status\tinvalidation_reason\n"
                            + VIEW1 + "\tinvalid\t" + expectedErrorMessage + "\n"
                            + VIEW3 + "\tinvalid\t" + expectedErrorMessage + "\n"
                            + VIEW4 + "\tinvalid\t" + expectedErrorMessage + "\n");

            // The explicit recompile path reports the same stable message.
            compileView(VIEW1, expectedErrorMessage);

            // The failure is contained: the base tables keep working and the view drops cleanly.
            assertQuery("select count() from " + TABLE1).noLeakCheck().noRandomAccess().expectSize().returns("count\n9\n");
            execute("DROP VIEW " + VIEW4);
            execute("DROP VIEW " + VIEW3);
            execute("DROP VIEW " + VIEW1);
            drainWalQueue();
            assertNull(getViewDefinition(VIEW1));
        });
    }

    private void testViewInvalidated(String viewQuery, String breakingSql, String fixingSql, String expectedErrorMessage, String expectedCreateMetadata, String expectedFixedMetadata) throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(1750327200000000L);

            createTable(TABLE1);
            createTable(TABLE2);

            // creating view
            createView(VIEW1, viewQuery);

            if (expectedCreateMetadata != null) {
                assertViewMetadata(expectedCreateMetadata);
            }

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n" +
                            "view1\t" + viewQuery + "\tview1~3\t\tvalid\t2025-06-19T10:00:00.000000Z\n",
                    "views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            views()
                            """
            );

            compileView(VIEW1);

            // breaking view
            execute(breakingSql);
            drainWalQueue();

            detectInvalidView(VIEW1, expectedErrorMessage);
            drainViewQueue();

            assertViewDefinition(VIEW1, viewQuery);
            assertViewDefinitionFile(VIEW1, viewQuery);
            assertViewState(VIEW1, expectedErrorMessage);

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n" +
                            "view1\t" + viewQuery + "\tview1~3\t" + expectedErrorMessage + "\tinvalid\t2025-06-19T10:00:00.000000Z\n",
                    "views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            views()
                            """
            );

            compileView(VIEW1, expectedErrorMessage);

            // fixing view
            execute(fixingSql);
            drainWalQueue();

            fixInvalidView(VIEW1);
            drainWalAndViewQueues();

            assertViewDefinition(VIEW1, viewQuery);
            assertViewDefinitionFile(VIEW1, viewQuery);
            assertViewState(VIEW1);

            if (expectedFixedMetadata != null) {
                assertViewMetadata(expectedFixedMetadata);
            }

            assertQueryAndPlan(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tview_status_update_time\n" +
                            "view1\t" + viewQuery + "\tview1~3\t\tvalid\t2025-06-19T10:00:00.000000Z\n",
                    "views()",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            views()
                            """
            );

            compileView(VIEW1);
        });
    }

    protected void detectInvalidView(String viewName, String expectedErrorMessage) {
        // no-op
        // invalid views are automatically detected by default
    }

    protected void fixInvalidView(String viewName) throws SqlException {
        // no-op
        // invalid views are automatically fixed by default
    }
}
