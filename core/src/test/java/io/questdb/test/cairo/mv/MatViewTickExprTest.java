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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A materialized view refreshes on the engine's behalf, with no caller to report a failure to. A
 * value of a row that does not parse as a tick expression acts as a null value there, rather than
 * invalidating the view, while the query of the view keeps failing on it when a user runs it.
 */
public class MatViewTickExprTest extends AbstractCairoTest {

    @Test
    public void testLiteralWithUnknownDurationUnitFailsCreateAndActsAsNullInRefresh() throws Exception {
        // SQL rejects a view and a materialized view with a literal that has a duration unit that
        // the timestamp drivers do not add as it creates them. An older build created them, so a
        // stored definition can hold such a literal: the database starts with it, a refresh runs it
        // as a null value and keeps the materialized view valid, and a query of the view fails on
        // it, the same as with [$tomorrow@+00:00];1x, a literal without IR that does not parse. The
        // literal selected every row before $tomorrow. A refresh resolves $tomorrow against the
        // "now" of its context, see testRowValueDateVariableResolvesLikeLiteral, so each day that
        // it can resolve to, the day after the clock and the second day of the epoch, has a row in
        // its first hour and one before it.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            execute("CREATE TABLE base (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('1970-01-01T09:00:00.000000Z', 1),
                    ('1970-01-02T00:30:00.000000Z', 1),
                    ('2024-01-02T09:00:00.000000Z', 1),
                    ('2024-01-03T00:30:00.000000Z', 1),
                    ('2024-01-03T09:00:00.000000Z', 1)
                    """);
            drainWalQueue();
            final String viewSql = "SELECT ts, v FROM base WHERE ts IN '$tomorrow;1x'";
            final String matViewSql = "SELECT ts, count() c FROM base WHERE ts IN '$tomorrow;1x' SAMPLE BY 1h";
            for (String variable : new String[]{"$tomorrow", "$today"}) {
                for (String sql : new String[]{"CREATE VIEW v AS (" + viewSql + ")", "CREATE MATERIALIZED VIEW mv AS (" + matViewSql + ")"}) {
                    final String create = sql.replace("$tomorrow", variable);
                    assertExceptionNoLeakCheck(create, create.indexOf('\''), "Invalid duration unit: x");
                }
            }

            // with a unit that the drivers add, the views hold the first hour of $tomorrow
            for (String name : new String[]{"v", "v_not_parsed"}) {
                execute("CREATE VIEW " + name + " AS (" + viewSql.replace(";1x", ";1h") + ")");
            }
            for (String name : new String[]{"mv", "mv_not_parsed"}) {
                execute("CREATE MATERIALIZED VIEW " + name + " AS (" + matViewSql.replace(";1x", ";1h") + ")");
            }
            drainWalAndMatViewQueues();
            drainViewQueue();
            assertQuery("v")
                    .noLeakCheck()
                    .inferRandomAccess()
                    .inferTimestamp()
                    .sizeMayVary()
                    .returns("""
                            ts\tv
                            2024-01-03T00:30:00.000000Z\t1
                            """);
            // whichever day $tomorrow is in a refresh
            assertQuery("SELECT count(), sum(c) FROM mv")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            1\t1
                            """);

            // the definitions that an older build stored
            writeViewSql("v", viewSql);
            writeViewSql("v_not_parsed", viewSql.replace("$tomorrow;1x", "[$tomorrow@+00:00];1x"));
            writeMatViewSql("mv", matViewSql);
            writeMatViewSql("mv_not_parsed", matViewSql.replace("$tomorrow;1x", "[$tomorrow@+00:00];1x"));
            engine.releaseInactive();
            engine.releaseAllWriters();

            try (
                    CairoEngine restarted = new CairoEngine(configuration);
                    SqlExecutionContext restartedContext = TestUtils.createSqlExecutionCtx(restarted)
            ) {
                restarted.load();
                restarted.buildViewGraphs();
                drainWalAndMatViewQueues(restarted);
                drainViewQueue(restarted);
                for (String name : new String[]{"mv", "mv_not_parsed"}) {
                    restarted.execute("REFRESH MATERIALIZED VIEW " + name + " FULL", restartedContext);
                }
                drainWalAndMatViewQueues(restarted);
                assertQuery("SELECT view_name, view_status FROM materialized_views() ORDER BY view_name")
                        .withEngine(restarted)
                        .withContext(restartedContext)
                        .noLeakCheck()
                        .inferRandomAccess()
                        .sizeMayVary()
                        .returns("""
                                view_name\tview_status
                                mv\tvalid
                                mv_not_parsed\tvalid
                                """);
                for (String name : new String[]{"mv", "mv_not_parsed"}) {
                    assertQuery("SELECT count() FROM " + name)
                            .withEngine(restarted)
                            .withContext(restartedContext)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    count
                                    0
                                    """);
                }
                for (String name : new String[]{"v", "v_not_parsed"}) {
                    try (RecordCursorFactory ignore = restarted.select("SELECT * FROM " + name, restartedContext)) {
                        Assert.fail("compiled: " + name);
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Invalid duration unit: x");
                    }
                }
                restarted.releaseInactive();
            }
        });
    }

    @Test
    public void testPointInTimeRangeLiteralWithMalformedDurationFailsCreateAndActsAsNullInRefresh() throws Exception {
        // A range between two points in time ignores its duration, which still has to parse: SQL
        // rejects a view and a materialized view whose literal has a duration that does not parse as
        // it creates them. An older build created them, so a stored definition can hold such a
        // literal: the database starts with it, a refresh runs it as a null value and keeps the
        // materialized view valid, where it selected the rows of the range, and a query of the view
        // fails on it. A refresh resolves $now against the "now" of its context, see
        // testRowValueDateVariableResolvesLikeLiteral, so the hour after each "now" that it can
        // resolve to, the clock and the start of the epoch, has a row.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            execute("CREATE TABLE base (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('1970-01-01T00:30:00.000000Z', 1),
                    ('2024-01-02T03:30:00.000000Z', 1),
                    ('2024-01-02T09:00:00.000000Z', 1)
                    """);
            drainWalQueue();
            final String viewSql = "SELECT ts, v FROM base WHERE ts IN '$now..$now + 1h;1x'";
            final String matViewSql = "SELECT ts, count() c FROM base WHERE ts IN '$now..$now + 1h;1x' SAMPLE BY 1h";
            for (String sql : new String[]{"CREATE VIEW v AS (" + viewSql + ")", "CREATE MATERIALIZED VIEW mv AS (" + matViewSql + ")"}) {
                assertExceptionNoLeakCheck(sql, sql.indexOf('\''), "Invalid duration unit: x");
            }

            // with a duration that parses, the views hold the hour after "now"
            execute("CREATE VIEW v AS (" + viewSql.replace(";1x", ";1h") + ")");
            execute("CREATE MATERIALIZED VIEW mv AS (" + matViewSql.replace(";1x", ";1h") + ")");
            drainWalAndMatViewQueues();
            drainViewQueue();
            assertQuery("v")
                    .noLeakCheck()
                    .inferRandomAccess()
                    .inferTimestamp()
                    .sizeMayVary()
                    .returns("""
                            ts\tv
                            2024-01-02T03:30:00.000000Z\t1
                            """);
            // whichever "now" the refresh resolves against
            assertQuery("SELECT count(), sum(c) FROM mv")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            1\t1
                            """);

            // the definitions that an older build stored
            writeViewSql("v", viewSql);
            writeMatViewSql("mv", matViewSql);
            engine.releaseInactive();
            engine.releaseAllWriters();

            try (
                    CairoEngine restarted = new CairoEngine(configuration);
                    SqlExecutionContext restartedContext = TestUtils.createSqlExecutionCtx(restarted)
            ) {
                restarted.load();
                restarted.buildViewGraphs();
                drainWalAndMatViewQueues(restarted);
                drainViewQueue(restarted);
                restarted.execute("REFRESH MATERIALIZED VIEW mv FULL", restartedContext);
                drainWalAndMatViewQueues(restarted);
                assertQuery("SELECT view_name, view_status FROM materialized_views()")
                        .withEngine(restarted)
                        .withContext(restartedContext)
                        .noLeakCheck()
                        .inferRandomAccess()
                        .sizeMayVary()
                        .returns("""
                                view_name\tview_status
                                mv\tvalid
                                """);
                assertQuery("SELECT count() FROM mv")
                        .withEngine(restarted)
                        .withContext(restartedContext)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                0
                                """);
                try (RecordCursorFactory ignore = restarted.select("SELECT * FROM v", restartedContext)) {
                    Assert.fail("compiled: v");
                } catch (SqlException e) {
                    TestUtils.assertEquals("Invalid duration unit: x", e.getFlyweightMessage());
                }
                restarted.releaseInactive();
            }
        });
    }

    @Test
    public void testRowValueDateVariableResolvesLikeLiteral() throws Exception {
        // A refresh resolves the date variables of a value of a row against the "now" of its
        // context, as it resolves those of a literal, not against the clock of the machine, so
        // that every refresh, and every replica, gets the same rows
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            execute("CREATE TABLE base (ts TIMESTAMP, s STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW literal_view AS (SELECT ts, count() c FROM base WHERE ts IN '$today' SAMPLE BY 1h)");
            execute("CREATE MATERIALIZED VIEW row_value_view AS (SELECT ts, count() c FROM base WHERE ts IN s SAMPLE BY 1h)");
            // one row on the day of the clock of the machine, and one on the first day of the epoch
            execute("""
                    INSERT INTO base VALUES
                    ('1970-01-01T01:00:00.000000Z', '$today'),
                    ('2024-01-02T08:00:00.000000Z', '$today')
                    """);
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_name, view_status FROM materialized_views() ORDER BY view_name")
                    .noLeakCheck()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            view_name\tview_status
                            literal_view\tvalid
                            row_value_view\tvalid
                            """);
            // whichever day "$today" is in a refresh, it is one of the two days
            assertQuery("SELECT count() FROM row_value_view")
                    .noLeakCheck()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            count
                            1
                            """);
            assertSqlCursors("literal_view", "row_value_view");
        });
    }

    @Test
    public void testRowValueThatDoesNotParseKeepsViewValid() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            execute("CREATE TABLE base (ts TIMESTAMP, s STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW in_view AS (SELECT ts, count() c FROM base WHERE ts IN s SAMPLE BY 1h)");
            execute("CREATE MATERIALIZED VIEW not_in_view AS (SELECT ts, count() c FROM base WHERE ts NOT IN s SAMPLE BY 1h)");
            execute("""
                    INSERT INTO base VALUES
                    ('2024-01-02T08:00:00.000000Z', '2024-01-02'),
                    ('2024-01-02T09:00:00.000000Z', 'garbage'),
                    ('2024-01-02T10:00:00.000000Z', '2024-01-02T10:00'),
                    ('2024-01-02T11:00:00.000000Z', '2024-01-03')
                    """);
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_name, view_status FROM materialized_views() ORDER BY view_name")
                    .noLeakCheck()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            view_name\tview_status
                            in_view\tvalid
                            not_in_view\tvalid
                            """);
            // the value that does not parse acts as null: no timestamp is in it
            assertQuery("in_view")
                    .noLeakCheck()
                    .inferRandomAccess()
                    .inferTimestamp()
                    .sizeMayVary()
                    .returns("""
                            ts\tc
                            2024-01-02T08:00:00.000000Z\t1
                            2024-01-02T10:00:00.000000Z\t1
                            """);
            assertQuery("not_in_view")
                    .noLeakCheck()
                    .inferRandomAccess()
                    .inferTimestamp()
                    .sizeMayVary()
                    .returns("""
                            ts\tc
                            2024-01-02T09:00:00.000000Z\t1
                            2024-01-02T11:00:00.000000Z\t1
                            """);
            // the query of the view fails on the value when a user runs it
            final String query = "SELECT ts, count() c FROM base WHERE ts IN s SAMPLE BY 1h";
            assertExceptionNoLeakCheck(query, query.indexOf(" s ") + 1, "Invalid date: garbage");
        });
    }

    /**
     * Writes the definition of the materialized view with another query, as an older build that
     * accepted the query stored it.
     */
    private static void writeMatViewSql(String name, String sql) {
        final TableToken token = engine.verifyTableName(name);
        final MatViewDefinition definition = engine.getDependentViewGraph().getViewDefinition(token);
        Assert.assertNotNull(definition);
        definition.setMatViewSqlForTesting(sql);
        try (
                BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                Path path = new Path()
        ) {
            writer.of(path.of(configuration.getDbRoot()).concat(token).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME).$());
            MatViewDefinition.append(definition, writer);
        }
    }

    /**
     * Writes the definition of the view with another query, as an older build that accepted the
     * query stored it.
     */
    private static void writeViewSql(String name, String sql) {
        final TableToken token = engine.verifyTableName(name);
        final ViewDefinition current = engine.getViewGraph().getViewDefinition(token);
        Assert.assertNotNull(current);
        final ViewDefinition definition = new ViewDefinition();
        definition.init(token, sql, current.getDependencies(), current.getSeqTxn(), false);
        try (
                BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                Path path = new Path()
        ) {
            writer.of(path.of(configuration.getDbRoot()).concat(token.getDirName()).concat(ViewDefinition.VIEW_DEFINITION_FILE_NAME).$());
            ViewDefinition.append(definition, writer);
        }
    }
}
