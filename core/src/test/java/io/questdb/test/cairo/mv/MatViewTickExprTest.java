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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.CreateViewOperation;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A materialized view refreshes on the engine's behalf, with no caller to report a failure to. A
 * value of a row that does not parse as a tick expression acts as a null value there, rather than
 * invalidating the view, while the query of the view keeps failing on it when a user runs it.
 * <p>
 * The statements that check the query of a view, as they create or alter it, open the query without
 * running it for anyone, and a tick expression fails nothing there, so they succeed before the range
 * from 09:30 to "now" starts, when it starts after it ends.
 */
public class MatViewTickExprTest extends AbstractCairoTest {
    // 2024-01-02, before the range from 09:30 to "now" starts, and after
    private static final long NOW_0300 = MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z");
    private static final long NOW_1200 = MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z");
    private static final String RANGE_ORDER_ERROR = "Invalid date range: start is after end";
    // makes the next view that the engine creates publish a row-expiry policy update, as a policy
    // that changes while CREATE VIEW creates the view does
    private static volatile boolean isExpiryPolicyUpdateArmed;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public @NotNull ViewDefinition createView(
                    SecurityContext securityContext,
                    MemoryMARW mem,
                    BlockFileWriter blockFileWriter,
                    Path path,
                    boolean ifNotExists,
                    CreateViewOperation operation,
                    @Nullable RecordMetadata metadata
            ) {
                final ViewDefinition definition = super.createView(securityContext, mem, blockFileWriter, path, ifNotExists, operation, metadata);
                if (isExpiryPolicyUpdateArmed) {
                    isExpiryPolicyUpdateArmed = false;
                    getMetadataCache().publishExpiryPolicyUpdate();
                }
                return definition;
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testBusinessDayRangeWithoutDayWithMalformedElementSuffixFailsCreate() throws Exception {
        // SQL rejects a view and a materialized view whose literal holds a range of business days
        // with a timezone of its element that does not parse as it creates them, at the literal,
        // on a Saturday too, when the range holds no business day and adds no interval, and
        // creates neither of them. With a timezone that parses, both are created on the Saturday,
        // and the view selects no row, not even the one on the Saturday.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-06T10:00:00.000000Z"));
            execute("CREATE TABLE base (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-06T09:00:00.000000Z', 1)");
            drainWalQueue();
            // ? stands for the literal
            final String[] creates = {
                    "CREATE VIEW v AS (SELECT ts, v FROM base WHERE ts IN '?')",
                    "CREATE MATERIALIZED VIEW mv AS (SELECT ts, count() c FROM base WHERE ts IN '?' SAMPLE BY 1h)"
            };
            for (String sql : creates) {
                final String create = sql.replace("?", "[$today..$today + 0bd@Bogus/Zone]");
                assertExceptionNoLeakCheck(create, create.indexOf('\''), "invalid timezone: Bogus/Zone");
            }
            Assert.assertNull(engine.getTableTokenIfExists("v"));
            Assert.assertNull(engine.getTableTokenIfExists("mv"));

            for (String sql : creates) {
                execute(sql.replace("?", "[$today..$today + 0bd@+00:00]"));
            }
            drainWalAndMatViewQueues();
            drainViewQueue();
            Assert.assertNotNull(engine.getTableTokenIfExists("v"));
            Assert.assertNotNull(engine.getTableTokenIfExists("mv"));
            assertQuery("v")
                    .noLeakCheck()
                    .inferRandomAccess()
                    .inferTimestamp()
                    .sizeMayVary()
                    .returns("ts\tv\n");
        });
    }

    @Test
    public void testDateVariableRangeAddingNoIntervalWithMalformedElementSuffixFailsCreate() throws Exception {
        // SQL rejects a view and a materialized view whose literal holds a date variable range
        // with a timezone or day filter of its element that does not parse as it creates them, at
        // the literal, even when the range adds no interval then: at 03:00 the range from 09:30 to
        // "now" starts after it ends, and so does the range from tomorrow to today at any time.
        // With a timezone that parses, CREATE creates both views at 03:00 too: it opens the query
        // of a view to check it, without running it for anyone, and the range fails nothing there.
        // Both load after a restart, the materialized view stays valid after a refresh, and the
        // view selects the row at 10:00 at 12:00.
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
            execute("CREATE VIEW v AS (" + viewSql.replace("?", wellFormed) + ")");
            execute("CREATE MATERIALIZED VIEW mv AS (" + matViewSql.replace("?", wellFormed) + ")");
            drainWalAndMatViewQueues();
            drainViewQueue();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z"));
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
    public void testMatViewFailingCheckBeforeRangeStart() throws Exception {
        // CREATE MATERIALIZED VIEW opens the query of the view to check it. At 03:00, when the range
        // from 09:30 to "now" starts after it ends, a query whose scalar sub-query returns both rows
        // of base fails as CREATE opens it: CREATE fails with its error and creates no view, and a
        // query fails on the range when it runs at 03:00 after that.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_0300);
            createRangeTables();
            assertStatementFails(
                    "CREATE MATERIALIZED VIEW mvx AS (SELECT ts, count() c FROM base WHERE ts > (SELECT ts FROM base) SAMPLE BY 1h)",
                    "scalar sub-query returned more than one row"
            );
            Assert.assertNull(engine.getTableTokenIfExists("mvx"));
            assertCompileProbeDown();
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

    @Test
    public void testStructuralExpiryPolicyFailingCheckBeforeRangeStart() throws Exception {
        // SQL checks a row-expiry policy with a window function as it sets it, by opening the query
        // that a read of the view runs with the policy. At 03:00, when the range from 09:30 to "now"
        // starts after it ends, a policy whose scalar sub-query returns both rows of the view fails
        // as ALTER opens that query: ALTER rejects the policy, the view keeps showing both rows, and
        // a query fails on the range when it runs at 03:00 after that.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_0300);
            createRangeTables();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM pbase)");
            drainWalAndMatViewQueues();
            final String policy = "v < max(v) OVER (PARTITION BY k) AND ts > (SELECT ts FROM mv)";
            final String sql = "ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN " + policy;
            assertExceptionNoLeakCheck(sql, sql.indexOf(policy), "invalid EXPIRE ROWS policy: scalar sub-query returned more than one row");
            assertCompileProbeDown();
            assertQuery("SELECT count() FROM mv")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            2
                            """);
        });
    }

    @Test
    public void testStructuralExpiryPolicySetBeforeRangeStart() throws Exception {
        // SQL checks a row-expiry policy with a window function as it sets it, by opening the query
        // that a read of the view runs with the policy, which evaluates the range of the policy. At
        // 03:00, when the range from 09:30 to "now" starts after it ends, ALTER and CREATE set such
        // policies, which the views keep after a restart.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_0300);
            createRangeTables();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM pbase)");
            drainWalAndMatViewQueues();
            final String orPolicy = "ts IN '$today + 570m..$now' OR v < max(v) OVER (PARTITION BY k)";
            for (String policy : new String[]{"v < max(v) OVER (PARTITION BY k) AND ts IN '$today + 570m..$now'", orPolicy}) {
                execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN " + policy);
                drainWalAndMatViewQueues();
                assertCompileProbeDown();
                assertExpiryPolicy(engine, sqlExecutionContext, "mv", policy);
            }
            execute("CREATE MATERIALIZED VIEW mv2 AS (SELECT * FROM pbase) EXPIRE ROWS WHEN " + orPolicy);
            drainWalAndMatViewQueues();
            assertCompileProbeDown();
            assertExpiryPolicy(engine, sqlExecutionContext, "mv2", orPolicy);
            engine.releaseInactive();
            engine.releaseAllWriters();

            try (
                    CairoEngine restarted = new CairoEngine(configuration);
                    SqlExecutionContext restartedContext = TestUtils.createSqlExecutionCtx(restarted)
            ) {
                restarted.load();
                restarted.buildViewGraphs();
                drainWalAndMatViewQueues(restarted);
                assertExpiryPolicy(restarted, restartedContext, "mv", orPolicy);
                assertExpiryPolicy(restarted, restartedContext, "mv2", orPolicy);
                restarted.releaseInactive();
            }
        });
    }

    @Test
    public void testViewOverLazilyBoundJoinsCreatedAndAlteredBeforeRangeStart() throws Exception {
        // A parallel HORIZON JOIN binds the filter of the table it joins, and a parallel WINDOW JOIN
        // its join filter, when it reads its first row, and a serial one when it opens. CREATE VIEW
        // opens the query of the view, and ALTER VIEW also reads its first row. At 03:00, when the
        // range from 09:30 to "now" starts after it ends, both succeed either way. The query of the
        // view fails on the range when it runs at 03:00, and returns the rows of the range at 12:00.
        // ALTER VIEW fails on a first row that fails for another reason, and a query fails on the
        // range when it runs at 03:00 after that.
        final boolean isParallelHorizonJoinEnabled = sqlExecutionContext.isParallelHorizonJoinEnabled();
        final boolean isParallelWindowJoinEnabled = sqlExecutionContext.isParallelWindowJoinEnabled();
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_0300);
            createRangeTables();
            // the query of the view, and its rows at 12:00: of q, only the rows at 10:00 and 11:00
            // are in the range, and no bid is negative
            final String[][] queries = {
                    {
                            "SELECT avg(q.bid) a FROM t HORIZON JOIN (SELECT * FROM q WHERE ts IN '$today + 570m..$now' OR bid < 0) q ON (t.sym = q.sym) RANGE FROM 0s TO 0s STEP 1s AS h",
                            "a\n2.5\n"
                    },
                    {
                            "SELECT avg(q.bid) a FROM t HORIZON JOIN q ON (t.sym = q.sym AND (q.ts IN '$today + 570m..$now' OR q.bid < 0)) RANGE FROM 0s TO 0s STEP 1s AS h",
                            "a\n2.5\n"
                    },
                    // at offset 0, each row of t gets the row of q at its time
                    {
                            "SELECT t.ts, h.offset, q.bid FROM t HORIZON JOIN (SELECT * FROM q WHERE ts IN '$today + 570m..$now' OR bid < 0) q ON (t.sym = q.sym) LIST (0s) AS h",
                            "ts\toffset\tbid\n2024-01-02T10:00:00.000000Z\t0\t2.0\n2024-01-02T11:00:00.000000Z\t0\t3.0\n"
                    },
                    // the 3 hours around each row of t hold both rows of q in the range
                    {
                            "SELECT t.ts, sum(q.bid) s FROM t WINDOW JOIN q ON (t.sym = q.sym AND (q.ts IN '$today + 570m..$now' OR q.bid < 0)) RANGE BETWEEN 3 hours PRECEDING AND 3 hours FOLLOWING",
                            "ts\ts\n2024-01-02T10:00:00.000000Z\t5.0\n2024-01-02T11:00:00.000000Z\t5.0\n"
                    }
            };
            try {
                for (boolean isParallel : new boolean[]{true, false}) {
                    sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                    sqlExecutionContext.setParallelWindowJoinEnabled(isParallel);
                    for (int i = 0; i < queries.length; i++) {
                        final String view = (isParallel ? "parallel" : "serial") + i;
                        setCurrentMicros(NOW_0300);
                        execute("CREATE VIEW " + view + " AS (" + queries[i][0] + ")");
                        execute("ALTER VIEW " + view + " AS (" + queries[i][0] + ")");
                        drainViewQueue();
                        Assert.assertFalse(sqlExecutionContext.isCompileProbe());
                        assertViewFails(view, RANGE_ORDER_ERROR);
                        setCurrentMicros(NOW_1200);
                        assertQuery(view)
                                .noLeakCheck()
                                .inferRandomAccess()
                                .inferTimestamp()
                                .sizeMayVary()
                                .returns(queries[i][1]);
                    }
                }

                sqlExecutionContext.setParallelHorizonJoinEnabled(true);
                setCurrentMicros(NOW_0300);
                assertStatementFails(
                        "ALTER VIEW parallel0 AS (SELECT avg(q.bid) a FROM t HORIZON JOIN (SELECT * FROM q WHERE bid > (SELECT bid FROM q) OR bid < 0) q ON (t.sym = q.sym) RANGE FROM 0s TO 0s STEP 1s AS h)",
                        "scalar sub-query returned more than one row"
                );
                assertCompileProbeDown();
            } finally {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallelHorizonJoinEnabled);
                sqlExecutionContext.setParallelWindowJoinEnabled(isParallelWindowJoinEnabled);
            }
        });
    }

    @Test
    public void testViewReprobedAfterExpiryChangeBeforeRangeStart() throws Exception {
        // CREATE VIEW checks the query of the view again, by opening it and reading its first row,
        // when a row-expiry policy changes while it creates the view. At 03:00, when the range from
        // 09:30 to "now" starts after it ends, that check succeeds too: over a parallel HORIZON JOIN,
        // which binds the filter of the table it joins when it reads its first row, and over a
        // filter that the JIT compiles. A check that fails for another reason fails CREATE VIEW, as
        // does a first check that fails as it opens the query, and a query fails on the range when
        // it runs at 03:00 after each of them.
        final boolean isParallelHorizonJoinEnabled = sqlExecutionContext.isParallelHorizonJoinEnabled();
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_0300);
            createRangeTables();
            try {
                sqlExecutionContext.setParallelHorizonJoinEnabled(true);
                for (String[] view : new String[][]{
                        {"w1", "SELECT avg(q.bid) a FROM t HORIZON JOIN (SELECT * FROM q WHERE ts IN '$today + 570m..$now' OR bid < 0) q ON (t.sym = q.sym) RANGE FROM 0s TO 0s STEP 1s AS h"},
                        {"w2", "SELECT ts, v FROM base WHERE ts IN '$today + 570m..$now' AND v > 0"}
                }) {
                    isExpiryPolicyUpdateArmed = true;
                    execute("CREATE VIEW " + view[0] + " AS (" + view[1] + ")");
                    // the engine published the update as it created the view
                    Assert.assertFalse(isExpiryPolicyUpdateArmed);
                    assertCompileProbeDown();
                }
                drainViewQueue();
                setCurrentMicros(NOW_1200);
                // of q, only the rows at 10:00 and 11:00 are in the range, and no bid is negative
                assertQuery("w1")
                        .noLeakCheck()
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns("a\n2.5\n");
                assertQuery("w2")
                        .noLeakCheck()
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns("""
                                ts\tv
                                2024-01-02T10:00:00.000000Z\t1
                                """);

                setCurrentMicros(NOW_0300);
                isExpiryPolicyUpdateArmed = true;
                assertStatementFails(
                        "CREATE VIEW w3 AS (SELECT avg(q.bid) a FROM t HORIZON JOIN (SELECT * FROM q WHERE bid > (SELECT bid FROM q) OR bid < 0) q ON (t.sym = q.sym) RANGE FROM 0s TO 0s STEP 1s AS h)",
                        "scalar sub-query returned more than one row"
                );
                Assert.assertFalse(isExpiryPolicyUpdateArmed);
                assertCompileProbeDown();
                assertStatementFails(
                        "CREATE VIEW w4 AS (SELECT * FROM t WHERE ts > (SELECT ts FROM t))",
                        "scalar sub-query returned more than one row"
                );
                assertCompileProbeDown();
            } finally {
                isExpiryPolicyUpdateArmed = false;
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallelHorizonJoinEnabled);
            }
        });
    }

    @Test
    public void testViewsCreatedBeforeRangeStartRunAfterIt() throws Exception {
        // At 03:00 the range from 09:30 to "now" starts after it ends. CREATE VIEW, CREATE
        // MATERIALIZED VIEW and ALTER VIEW open the query of the view to check it, and ALTER VIEW
        // reads its first row too, which evaluates the range, a runtime constant condition on it,
        // and the value of the row: they succeed, also when that value does not parse. The query of
        // a view fails on the range or on the value when it runs at 03:00, and returns the rows of
        // the range at 12:00, also after a restart, after which the materialized views refresh and
        // stay valid.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_0300);
            createRangeTables();
            execute("CREATE VIEW v1 AS (SELECT ts, v FROM base WHERE ts IN '$today + 570m..$now')");
            execute("CREATE VIEW v2 AS (SELECT ts, v FROM base WHERE ts IN '$today + 570m..$now' AND v > 0)");
            execute("CREATE VIEW v3 AS (SELECT ts, v FROM base WHERE now() IN '$today + 570m..$now')");
            execute("CREATE MATERIALIZED VIEW mv1 AS (SELECT ts, count() c FROM base WHERE ts IN '$today + 570m..$now' SAMPLE BY 1h)");
            execute("CREATE MATERIALIZED VIEW mv2 AS (SELECT ts, count() c FROM base WHERE ts IN '$today + 570m..$now' AND v > 0 SAMPLE BY 1h)");
            execute("ALTER VIEW v1 AS (SELECT ts, v FROM base WHERE ts IN '$today + 570m..$now' AND v > 0)");
            execute("CREATE VIEW v4 AS (SELECT ts, v FROM base)");
            drainWalAndMatViewQueues();
            drainViewQueue();
            for (String view : new String[]{"v1", "v2", "v3"}) {
                assertViewFails(view, RANGE_ORDER_ERROR);
            }
            // the first row holds the range in s and vc, and then a value that does not parse
            for (String[] value : new String[][]{{null, RANGE_ORDER_ERROR}, {"garbage", "Invalid date: garbage"}}) {
                if (value[0] != null) {
                    execute("UPDATE base SET s = '" + value[0] + "', vc = '" + value[0] + "'");
                    drainWalQueue();
                }
                for (String column : new String[]{"s", "vc"}) {
                    execute("ALTER VIEW v4 AS (SELECT ts, v FROM base WHERE ts IN " + column + ")");
                    drainViewQueue();
                    assertViewFails("v4", value[1]);
                }
            }

            setCurrentMicros(NOW_1200);
            for (String view : new String[]{"v1", "v2"}) {
                assertQuery(view)
                        .noLeakCheck()
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns("""
                                ts\tv
                                2024-01-02T10:00:00.000000Z\t1
                                """);
            }
            // "now" is in the range, so the condition holds for every row
            assertQuery("v3")
                    .noLeakCheck()
                    .inferRandomAccess()
                    .inferTimestamp()
                    .sizeMayVary()
                    .returns("""
                            ts\tv
                            2024-01-02T08:00:00.000000Z\t1
                            2024-01-02T10:00:00.000000Z\t1
                            """);
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
                restarted.execute("REFRESH MATERIALIZED VIEW mv1 FULL", restartedContext);
                restarted.execute("REFRESH MATERIALIZED VIEW mv2 FULL", restartedContext);
                drainWalAndMatViewQueues(restarted);
                assertQuery("SELECT view_name, view_status FROM materialized_views() ORDER BY view_name")
                        .withEngine(restarted)
                        .withContext(restartedContext)
                        .noLeakCheck()
                        .inferRandomAccess()
                        .sizeMayVary()
                        .returns("""
                                view_name\tview_status
                                mv1\tvalid
                                mv2\tvalid
                                """);
                // the query of the view runs at 12:00
                restartedContext.initNow();
                assertQuery("v1")
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

    /**
     * Asserts that the statements before left the compile probe flag down: a query fails on the
     * range from 09:30 to "now" when it runs at 03:00.
     */
    private static void assertCompileProbeDown() throws Exception {
        Assert.assertFalse(sqlExecutionContext.isCompileProbe());
        final String sql = "SELECT count() FROM pbase WHERE ts IN '$today + 570m..$now'";
        assertExceptionNoLeakCheck(sql, sql.indexOf('\''), RANGE_ORDER_ERROR);
    }

    private static void assertExpiryPolicy(CairoEngine engine, SqlExecutionContext context, String matView, String policy) throws SqlException {
        final StringSink ddl = new StringSink();
        TestUtils.printSql(engine, context, "SHOW CREATE MATERIALIZED VIEW " + matView, ddl);
        TestUtils.assertContains(ddl, "EXPIRE ROWS WHEN " + policy);
    }

    /**
     * Asserts that the statement, which checks the query of a view, fails with the error that the
     * query raises. The position of the error is not checked: the statement reports it at the name
     * of the view or in the query, depending on where the query raises it.
     */
    private static void assertStatementFails(String sql, String error) {
        try {
            execute(sql);
            Assert.fail("the statement should fail: " + sql);
        } catch (CairoException | SqlException e) {
            TestUtils.assertEquals(sql, error, e.getFlyweightMessage());
        }
    }

    /**
     * Asserts that the query of the view fails when it runs. The query of the view is not part of
     * the text of the statement that reads the view, so the position of the error is not checked.
     */
    private static void assertViewFails(String view, String error) {
        final String sql = "SELECT * FROM " + view;
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            TestUtils.drainCursor(cursor);
            Assert.fail("the query should fail: " + sql);
        } catch (CairoException | SqlException e) {
            TestUtils.assertEquals(sql, error, e.getFlyweightMessage());
        }
    }

    /**
     * Creates the tables of the views that SQL checks before the range from 09:30 to "now" starts,
     * with rows on 2024-01-02: WAL table base, with the range in its string columns, tables t and q
     * for the joins, and WAL table pbase for a row-expiry policy with a window function.
     */
    private static void createRangeTables() throws SqlException {
        execute("CREATE TABLE base (ts TIMESTAMP, v INT, s STRING, vc VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('2024-01-02T08:00:00.000000Z', 1, '$today + 570m..$now', '$today + 570m..$now'),
                ('2024-01-02T10:00:00.000000Z', 1, '$today + 570m..$now', '$today + 570m..$now')
                """);
        execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t VALUES ('2024-01-02T10:00:00.000000Z', 'A', 1), ('2024-01-02T11:00:00.000000Z', 'A', 1)");
        execute("CREATE TABLE q (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO q VALUES
                ('2024-01-02T08:00:00.000000Z', 'A', 1.0),
                ('2024-01-02T10:00:00.000000Z', 'A', 2.0),
                ('2024-01-02T11:00:00.000000Z', 'A', 3.0)
                """);
        execute("CREATE TABLE pbase (ts TIMESTAMP, k SYMBOL, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO pbase VALUES ('2024-01-02T08:00:00.000000Z', 'a', 1), ('2024-01-02T10:00:00.000000Z', 'a', 2)");
        drainWalQueue();
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
