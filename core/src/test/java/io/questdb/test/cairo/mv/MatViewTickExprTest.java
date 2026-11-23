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
    public void testDateVariableRangeAddingNoIntervalWithMalformedElementSuffixFailsCreate() throws Exception {
        // SQL rejects a view and a materialized view whose literal holds a date variable range
        // with a timezone or day filter of its element that does not parse as it creates them, at
        // the literal, even when the range adds no interval then: at 03:00 the range from 09:30 to
        // "now" starts after it ends, and so does the range from tomorrow to today at any time.
        // A range that starts after it ends fails the query of a view when CREATE runs it, on
        // its order, so the views with a timezone that parses are created at 12:00, when the
        // range does not start after it ends. Both load after a restart, the materialized view
        // stays valid after a refresh, and the view selects the row at 10:00.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            execute("CREATE TABLE base (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('2024-01-02T08:00:00.000000Z', 1),
                    ('2024-01-02T10:00:00.000000Z', 1),
                    ('2024-01-03T09:00:00.000000Z', 1)
                    """);
            drainWalQueue();
            // ? stands for the literal
            final String viewSql = "SELECT ts, v FROM base WHERE ts IN '?'";
            final String matViewSql = "SELECT ts, count() c FROM base WHERE ts IN '?' SAMPLE BY 1h";
            for (String[] literal : new String[][]{
                    {"[$today + 570m..$now@Bogus/Zone]", "invalid timezone: Bogus/Zone"},
                    {"[$tomorrow..$today#Xyz]", "Invalid day name: Xyz"}
            }) {
                for (String sql : new String[]{"CREATE VIEW v AS (" + viewSql + ")", "CREATE MATERIALIZED VIEW mv AS (" + matViewSql + ")"}) {
                    final String create = sql.replace("?", literal[0]);
                    assertExceptionNoLeakCheck(create, create.indexOf('\''), literal[1]);
                }
            }

            final String wellFormed = "[$today + 570m..$now@+00:00]";
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z"));
            execute("CREATE VIEW v AS (" + viewSql.replace("?", wellFormed) + ")");
            execute("CREATE MATERIALIZED VIEW mv AS (" + matViewSql.replace("?", wellFormed) + ")");
            drainWalAndMatViewQueues();
            drainViewQueue();
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
                // the query of the view runs at 12:00
                restartedContext.initNow();
                assertQuery("v")
                        .withEngine(restarted)
                        .withContext(restartedContext)
                        .noLeakCheck()
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns("""
                                ts\tv
                                2024-01-02T10:00:00.000000Z\t1
                                """);
                restarted.releaseInactive();
            }
        });
    }

    @Test
    public void testLiteralWithElementTimePartUnderListTimeFailsCreateAndActsAsNullInRefresh() throws Exception {
        // SQL rejects a view and a materialized view with a literal that has a static element with
        // a time part of its own that starts with a bracket, under a time of the list, as it creates
        // them. An older build created them, so a stored definition can hold such a literal: the
        // database starts with it, a refresh runs it as a null value and keeps the materialized view
        // valid, and a query of the view fails on it. The literal selected 10:00 on every day of
        // January. A month element without a time part of its own selects that too, which holds the
        // first row whatever day $today is in a refresh.
        assertLiteralFailsCreateAndActsAsNullInRefresh(
                """
                        ('2024-01-05T10:00:00.000000Z', 1),
                        ('2024-01-05T11:00:00.000000Z', 1),
                        ('2024-02-01T10:00:00.000000Z', 1)
                        """,
                new String[]{"[2024-01T[09:00,10:00], $today]T10:00"},
                "[2024-01, $today]T10:00",
                "2024-01-05T10:00:00.000000Z\t1",
                new String[]{"[2024-01T[09:00,10:00], $today]T10:00"},
                "Invalid date: 2024-01-01T09:00T10:00"
        );
    }

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
        // its first hour and one before it. With a unit that the drivers add, the views hold the
        // first hour of $tomorrow.
        assertLiteralFailsCreateAndActsAsNullInRefresh(
                """
                        ('1970-01-01T09:00:00.000000Z', 1),
                        ('1970-01-02T00:30:00.000000Z', 1),
                        ('2024-01-02T09:00:00.000000Z', 1),
                        ('2024-01-03T00:30:00.000000Z', 1),
                        ('2024-01-03T09:00:00.000000Z', 1)
                        """,
                new String[]{"$tomorrow;1x", "$today;1x"},
                "$tomorrow;1h",
                "2024-01-03T00:30:00.000000Z\t1",
                new String[]{"$tomorrow;1x", "[$tomorrow@+00:00];1x"},
                "Invalid duration unit: x"
        );
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
        // resolve to, the clock and the start of the epoch, has a row. With a duration that parses,
        // the views hold the hour after "now".
        assertLiteralFailsCreateAndActsAsNullInRefresh(
                """
                        ('1970-01-01T00:30:00.000000Z', 1),
                        ('2024-01-02T03:30:00.000000Z', 1),
                        ('2024-01-02T09:00:00.000000Z', 1)
                        """,
                new String[]{"$now..$now + 1h;1x"},
                "$now..$now + 1h;1h",
                "2024-01-02T03:30:00.000000Z\t1",
                new String[]{"$now..$now + 1h;1x"},
                "Invalid duration unit: x"
        );
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

    /**
     * Asserts that SQL rejects a view and a materialized view with each rejected literal as it
     * creates them, at the literal, and that definitions that an older build stored with each
     * stored literal load: the database starts with them, a refresh runs the literal as a null
     * value and keeps the materialized view valid, and a query of the view fails on it. Before the
     * restart, the views hold the well-formed literal, which selects one row of the base table: the
     * expected row of the view, which is the only row of the materialized view at the "now" of a
     * refresh. The clock is 03:00 on 2024-01-02.
     *
     * @param rows the rows of the base table, (ts, v) with v = 1
     */
    private void assertLiteralFailsCreateAndActsAsNullInRefresh(
            String rows,
            String[] rejectedLiterals,
            String wellFormedLiteral,
            String expectedViewRow,
            String[] storedLiterals,
            String error
    ) throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            execute("CREATE TABLE base (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES " + rows);
            drainWalQueue();
            // ? stands for the literal
            final String viewSql = "SELECT ts, v FROM base WHERE ts IN '?'";
            final String matViewSql = "SELECT ts, count() c FROM base WHERE ts IN '?' SAMPLE BY 1h";
            for (String literal : rejectedLiterals) {
                for (String sql : new String[]{"CREATE VIEW v AS (" + viewSql + ")", "CREATE MATERIALIZED VIEW mv AS (" + matViewSql + ")"}) {
                    final String create = sql.replace("?", literal);
                    assertExceptionNoLeakCheck(create, create.indexOf('\''), error);
                }
            }

            // with the well-formed literal, each view holds the row
            for (int i = 0; i < storedLiterals.length; i++) {
                execute("CREATE VIEW v" + i + " AS (" + viewSql.replace("?", wellFormedLiteral) + ")");
                execute("CREATE MATERIALIZED VIEW mv" + i + " AS (" + matViewSql.replace("?", wellFormedLiteral) + ")");
            }
            drainWalAndMatViewQueues();
            drainViewQueue();
            for (int i = 0; i < storedLiterals.length; i++) {
                assertQuery("v" + i)
                        .noLeakCheck()
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns("ts\tv\n" + expectedViewRow + "\n");
                // whichever "now" the refresh resolves against
                assertQuery("SELECT count(), sum(c) FROM mv" + i)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count\tsum
                                1\t1
                                """);
            }

            // the definitions that an older build stored
            for (int i = 0; i < storedLiterals.length; i++) {
                writeViewSql("v" + i, viewSql.replace("?", storedLiterals[i]));
                writeMatViewSql("mv" + i, matViewSql.replace("?", storedLiterals[i]));
            }
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
                final StringBuilder statuses = new StringBuilder("view_name\tview_status\n");
                for (int i = 0; i < storedLiterals.length; i++) {
                    restarted.execute("REFRESH MATERIALIZED VIEW mv" + i + " FULL", restartedContext);
                    statuses.append("mv").append(i).append("\tvalid\n");
                }
                drainWalAndMatViewQueues(restarted);
                assertQuery("SELECT view_name, view_status FROM materialized_views() ORDER BY view_name")
                        .withEngine(restarted)
                        .withContext(restartedContext)
                        .noLeakCheck()
                        .inferRandomAccess()
                        .sizeMayVary()
                        .returns(statuses);
                for (int i = 0; i < storedLiterals.length; i++) {
                    assertQuery("SELECT count() FROM mv" + i)
                            .withEngine(restarted)
                            .withContext(restartedContext)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    count
                                    0
                                    """);
                    try (RecordCursorFactory ignore = restarted.select("SELECT * FROM v" + i, restartedContext)) {
                        Assert.fail("compiled: v" + i);
                    } catch (SqlException e) {
                        TestUtils.assertEquals("v" + i, error, e.getFlyweightMessage());
                    }
                }
                restarted.releaseInactive();
            }
        });
    }
}
