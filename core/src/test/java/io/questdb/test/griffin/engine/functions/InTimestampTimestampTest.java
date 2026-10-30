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

package io.questdb.test.griffin.engine.functions;

import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.jit.JitUtil;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.MutationStep;
import org.junit.Assert;
import org.junit.Test;

public class InTimestampTimestampTest extends AbstractCairoTest {

    @Test
    public void testBindVarRuntimeConstantsWithConstant() throws Exception {
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(
                "runtime constants",
                "participantId\tparticipantIdType\n",
                bindVariableService -> {
                    bindVariableService.setDate(0, 1000L);
                    bindVariableService.setTimestamp(1, 2000L);
                    bindVariableService.setTimestamp(2, 3000L);
                }
        ));

        assertQuery("""
                SELECT DISTINCT initParticipantId AS participantId, initParticipantIdType AS participantIdType
                FROM 'MovementLog'
                WHERE movementBusinessDate=$1 AND slotId IN ($2, '1970-01-01T00:00:00.005000Z', $3)
                ORDER BY participantId
                LIMIT 0,6""")
                .ddl("""
                        create table MovementLog(
                        ts timestamp,
                        initParticipantId long,
                        initParticipantIdType symbol,
                        movementBusinessDate date,
                        slotId timestamp
                        ) timestamp(ts) partition by day wal
                        """)
                .assertBinds(cases);
    }

    @Test
    public void testBindVarTypeChange() throws Exception {
        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(
                "simple",
                """
                        a\tts
                        -1148479920\t1970-01-01T00:00:00.000000Z
                        315515118\t1970-01-01T00:00:00.001000Z
                        -948263339\t1970-01-01T00:00:00.005000Z
                        """,
                bindVariableService -> {
                    bindVariableService.setInt(0, 0);
                    bindVariableService.setInt(1, 1000);
                    bindVariableService.setStr(2, "1970-01-01T00:00:00.005000Z");
                }
        ));

        cases.add(BindVarTuple.ok(
                "type change",
                """
                        a\tts
                        1326447242\t1970-01-01T00:00:00.006000Z
                        592859671\t1970-01-01T00:00:00.007000Z
                        -1191262516\t1970-01-01T00:00:00.010000Z
                        """,
                bindVariableService -> {
                    bindVariableService.setLong(0, 6000);
                    bindVariableService.setStr(1, "1970-01-01T00:00:00.007000Z");
                    bindVariableService.setInt(2, 10_000);
                }
        ));

        cases.add(BindVarTuple.ok(
                "type change with varchar",
                """
                        a\tts
                        1326447242\t1970-01-01T00:00:00.006000Z
                        -1191262516\t1970-01-01T00:00:00.010000Z
                        -2041844972\t1970-01-01T00:00:00.011000Z
                        """,
                bindVariableService -> {
                    bindVariableService.setLong(0, 6000);
                    bindVariableService.setVarchar(1, new Utf8String("1970-01-01T00:00:00.011000Z"));
                    bindVariableService.setInt(2, 10_000);
                }
        ));

        assertQuery("test where ts in ($1,$2,$3)")
                .ddl("create table test as (select rnd_int() a, timestamp_sequence(0, 1000) ts from long_sequence(100))")
                .assertBinds(cases);
    }

    @Test
    public void testDateListWithDenseTimeExpansionAndDayFilter() throws Exception {
        // The minute expansion emits 1440 intervals per date, past the incremental merge
        // threshold (256). 2024-01-03 is a Wednesday and 2024-01-04 a Thursday, so #Wed keeps
        // 2024-01-03 only.
        assertMemoryLeak(() -> {
            createHourlyTable();
            final String expectedRow = "24\t2024-01-03T00:00:00.000000Z\t2024-01-03T23:00:00.000000Z";
            // the hour expansion stays below the threshold
            assertTickExprMatches("[2024-01-03,2024-01-04]T[00..23]#Wed", expectedRow);
            assertTickExprMatches("[2024-01-03,2024-01-04]T[00..23]:[00..59]#Wed", expectedRow);
            assertTickExprBindVariableMatches("[2024-01-03,2024-01-04]T[00..23]:[00..59]#Wed", expectedRow);
        });
    }

    @Test
    public void testDateListWithDenseTimeExpansionAndTimezone() throws Exception {
        // The minute expansion emits 1440 intervals per date, past the incremental merge
        // threshold (256). +02:00 shifts both dates two hours back, so the list covers
        // 2024-01-01T22:00Z to 2024-01-03T21:59:59.999999Z.
        assertMemoryLeak(() -> {
            createHourlyTable();
            final String expectedRow = "48\t2024-01-01T22:00:00.000000Z\t2024-01-03T21:00:00.000000Z";
            // the hour expansion stays below the threshold
            assertTickExprMatches("[2024-01-02,2024-01-03]T[00..23]@+02:00", expectedRow);
            assertTickExprMatches("[2024-01-02,2024-01-03]T[00..23]:[00..59]@+02:00", expectedRow);
            assertTickExprBindVariableMatches("[2024-01-02,2024-01-03]T[00..23]:[00..59]@+02:00", expectedRow);
        });
    }

    @Test
    public void testDateVariableBareTimeListInCachedPlanFollowsDayChange() throws Exception {
        // A bare date variable takes a bracketed time part, as in $todayT[09:00,14:30], the same way
        // as [$today]T[09:00,14:30]. A cached plan must move the slots to the next day along with
        // $today wherever it evaluates them: in the interval scan, for IN, NOT IN and the second IN
        // of an OR, and in the row filter, with the JIT on and off. The compiled form represents the
        // first time part and parses the second one on every execution.
        assertMemoryLeak(() -> {
            final int jitMode = sqlExecutionContext.getJitMode();
            try {
                for (String slots : new String[]{"$todayT[09:00,14:30]", "$todayT[09,14]:[00,30]"}) {
                    for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                        sqlExecutionContext.setJitMode(mode);
                        // ts is the designated timestamp, so its IN narrows the interval scan, and ts2
                        // is not, so the row filter evaluates its IN
                        for (String column : new String[]{"ts", "ts2"}) {
                            final String select = "SELECT count(), min(" + column + "), max(" + column + ") FROM t WHERE ";
                            assertHeldFactoryFollowsDayChange(
                                    select + column + " IN '" + slots + "'",
                                    "2\t2024-01-02T09:00:00.000000Z\t2024-01-02T14:30:00.000000Z",
                                    "2\t2024-01-03T09:00:00.000000Z\t2024-01-03T14:30:00.000000Z"
                            );
                            assertHeldFactoryFollowsDayChange(
                                    select + column + " NOT IN '" + slots + "'",
                                    "1\t2024-01-02T10:30:00.000000Z\t2024-01-02T10:30:00.000000Z",
                                    "4\t2024-01-02T09:00:00.000000Z\t2024-01-03T10:30:00.000000Z"
                            );
                        }
                        assertHeldFactoryFollowsDayChange(
                                "SELECT count(), min(ts), max(ts) FROM t WHERE ts IN '2024-01-05' OR ts IN '" + slots + "'",
                                "2\t2024-01-02T09:00:00.000000Z\t2024-01-02T14:30:00.000000Z",
                                "2\t2024-01-03T09:00:00.000000Z\t2024-01-03T14:30:00.000000Z"
                        );
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    @Test
    public void testDateVariableBareTimeListParseErrorFails() throws Exception {
        // A bare date variable with a time part that does not parse fails the query with the
        // parser's message, at the position of the expression, the same as its bracketed form
        assertMemoryLeak(() -> {
            createHourlyTable();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z"));
            try {
                for (String tickExpr : new String[]{"$todayT[09,14]:[00,99]", "[$today]T[09,14]:[00,99]"}) {
                    final String error = "Invalid date: 2024-01-02T09:99";
                    // the interval scan
                    assertExceptionNoLeakCheck("SELECT count() FROM t WHERE ts IN '" + tickExpr + "'", 34, error);
                    assertExceptionNoLeakCheck("SELECT count() FROM t WHERE ts NOT IN '" + tickExpr + "'", 38, error);
                    // the row filter
                    assertExceptionNoLeakCheck("SELECT count() FROM t WHERE ts IN '" + tickExpr + "' OR ts = 0", 34, error);
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDateVariableInCachedCountFilterFollowsNow() throws Exception {
        // count() takes the count-only filter, which the JIT compiles apart from the row filter
        assertCachedFilterFollowsNow(
                "SELECT count() FROM t WHERE ts2 IN '$today..$now'",
                """
                        count
                        61
                        """,
                """
                        count
                        91
                        """
        );
    }

    @Test
    public void testDateVariableInCachedFilterFollowsDayChange() throws Exception {
        // A cached plan must move to the next day along with $today, rather than keep matching
        // the day it compiled on
        setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
        try {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            final ObjList<MutationStep> steps = new ObjList<>();
            steps.add(MutationStep.of(
                    insertEveryTenMinutes("2024-01-02T00:00:00.000000Z", 61),
                    """
                            count\tmin\tmax
                            61\t2024-01-02T00:00:00.000000Z\t2024-01-02T10:00:00.000000Z
                            """
            ));
            steps.add(MutationStep.of(
                    () -> setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-03T10:00:00.000000Z")),
                    insertEveryTenMinutes("2024-01-03T00:00:00.000000Z", 61),
                    """
                            count\tmin\tmax
                            61\t2024-01-03T00:00:00.000000Z\t2024-01-03T10:00:00.000000Z
                            """
            ));
            assertQuery("SELECT count(), min(ts2), max(ts2) FROM t WHERE ts2 IN '$today'")
                    .noRandomAccess()
                    .expectSize()
                    .ddl("CREATE TABLE t (ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY")
                    .mutateStepwise(steps);
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testDateVariableInCachedFilterFollowsNow() throws Exception {
        // ts2 is not the designated timestamp, so the IN stays in the row filter
        assertCachedFilterFollowsNow(
                "SELECT count(), max(ts2) FROM t WHERE ts2 IN '$today..$now'",
                """
                        count\tmax
                        61\t2024-01-02T10:00:00.000000Z
                        """,
                """
                        count\tmax
                        91\t2024-01-02T15:00:00.000000Z
                        """
        );
    }

    @Test
    public void testDateVariableInCachedOrFilterFollowsNow() throws Exception {
        // the OR keeps the IN on the designated timestamp out of the interval scan, in the row filter
        assertCachedFilterFollowsNow(
                "SELECT count(), max(ts) FROM t WHERE ts IN '$today..$now' OR ts > '2030-01-01'",
                """
                        count\tmax
                        61\t2024-01-02T10:00:00.000000Z
                        """,
                """
                        count\tmax
                        91\t2024-01-02T15:00:00.000000Z
                        """
        );
    }

    @Test
    public void testDateVariableIntervalLimit() throws Exception {
        // A literal with a date variable evaluates its IR when the query runs, and must stop at the
        // interval limit where the parser stops
        assertMemoryLeak(() -> {
            createHourlyTable();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:30:00.000000Z"));
            try {
                final String tickExpr = "[$today..$today + 2000d]T09:00";
                final String error = "Date range expansion produces too many intervals (max 1024)";
                assertExceptionNoLeakCheck("SELECT count() FROM t WHERE ts IN '" + tickExpr + "'", 34, error);
                // the OR keeps the IN in the row filter
                assertExceptionNoLeakCheck("SELECT count() FROM t WHERE ts IN '" + tickExpr + "' OR ts = 0", 34, error);
                // a bind variable fails in the row filter too
                bindVariableService.clear();
                bindVariableService.setStr(0, tickExpr);
                assertExceptionNoLeakCheck("SELECT count() FROM t WHERE ts IN $1 OR ts = 0", -1, error);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDateVariableLiteralTimeShapes() throws Exception {
        // A literal with a date variable compiles to a form that re-evaluates for every query. Time
        // brackets and element suffixes must evaluate the same way as in a bind variable.
        assertMemoryLeak(() -> {
            // a row every ten minutes on 2024-01-02
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-02T00:00:00.000000Z', 600_000_000L) ts
                        FROM long_sequence(144)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z"));
            // the minutes 01:30 and 02:30
            assertTickExprMatches("[$today]T[01,02]:30", "2\t2024-01-02T01:30:00.000000Z\t2024-01-02T02:30:00.000000Z");
            // the minutes 01:00, 01:30, 02:00 and 02:30
            assertTickExprMatches("[$today]T[01..02]:[00,30]", "4\t2024-01-02T01:00:00.000000Z\t2024-01-02T02:30:00.000000Z");
            // 10:30 at +02:00 is 08:30Z
            assertTickExprMatches("[$today]T[10:30@+02:00,11:00]", "2\t2024-01-02T08:30:00.000000Z\t2024-01-02T11:00:00.000000Z");
            // 2024-01-02 is a Tuesday
            assertTickExprMatches("[$today#Tue]", "144\t2024-01-02T00:00:00.000000Z\t2024-01-02T23:50:00.000000Z");
            assertTickExprMatches("[$today#Mon, $yesterday]", "0\t\t");
        });
    }

    @Test
    public void testDateVariableNowAtMidnight() throws Exception {
        // https://github.com/questdb/questdb/issues/8083
        // $now and expressions on it stay points in time when they land exactly on midnight
        assertMemoryLeak(() -> {
            createHourlyTable();

            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T01:00:00.000000Z"));
            assertTickExprMatches("$now - 1h..$now", "2\t2024-01-02T00:00:00.000000Z\t2024-01-02T01:00:00.000000Z");

            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T00:00:00.000000Z"));
            assertTickExprMatches("$now - 2h..$now", "3\t2024-01-01T22:00:00.000000Z\t2024-01-02T00:00:00.000000Z");
            assertTickExprMatches("$now", "1\t2024-01-02T00:00:00.000000Z\t2024-01-02T00:00:00.000000Z");
        });
    }

    @Test
    public void testDateVariableRangeElementDayFilterBindVariable() throws Exception {
        // the range's own day filter keeps the Mondays: 2024-01-01 and 2024-01-08
        assertMemoryLeak(() -> {
            // hourly rows from 2024-01-01T00:00 to 2024-01-14T23:00
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts
                        FROM long_sequence(336)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-01T12:00:00.000000Z"));
            final ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok(
                    "range with its own day filter",
                    """
                            count\tmin\tmax
                            48\t2024-01-01T00:00:00.000000Z\t2024-01-08T23:00:00.000000Z
                            """,
                    bindVariableService -> bindVariableService.setStr(0, "[$today..$today+7d#Mon]")
            ));
            assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .assertBinds(cases);
        });
    }

    @Test
    public void testDateVariableRangeKeepsEndpointTimeOfDay() throws Exception {
        // https://github.com/questdb/questdb/issues/8083
        // A range endpoint keeps its time of day when the other endpoint is a whole day
        assertMemoryLeak(() -> {
            createHourlyTable();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.123456Z"));

            // both endpoints have a time of day
            assertTickExprMatches("$today + 6h..$today + 23h", "18\t2024-01-02T06:00:00.000000Z\t2024-01-02T23:00:00.000000Z");
            assertTickExprMatches("$now - 2h..$now", "2\t2024-01-02T09:00:00.000000Z\t2024-01-02T10:00:00.000000Z");

            // only one endpoint has a time of day
            assertTickExprMatches("$today + 18h..$tomorrow", "30\t2024-01-02T18:00:00.000000Z\t2024-01-03T23:00:00.000000Z");
            assertTickExprMatches("$now - 2h..$tomorrow", "39\t2024-01-02T09:00:00.000000Z\t2024-01-03T23:00:00.000000Z");
            assertTickExprMatches("$yesterday + 12h..$today", "36\t2024-01-01T12:00:00.000000Z\t2024-01-02T23:00:00.000000Z");
            assertTickExprMatches("$today + 6h..$today + 24h", "19\t2024-01-02T06:00:00.000000Z\t2024-01-03T00:00:00.000000Z");
            assertTickExprMatches("$today..$now", "11\t2024-01-02T00:00:00.000000Z\t2024-01-02T10:00:00.000000Z");
        });
    }

    @Test
    public void testDateVariableRangeWithDenseTimeExpansion() throws Exception {
        // The minute expansion emits 1440 intervals per day, past the incremental merge
        // threshold (256). A literal with a date variable compiles to a CompiledTickExpression,
        // which rejects a numeric expansion in the time of day, so a bind variable carries
        // these expressions to the parser.
        assertMemoryLeak(() -> {
            createHourlyTable();
            // $yesterday is Tuesday 2024-01-02 and $today Wednesday 2024-01-03
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-03T10:00:00.000000Z"));
            try {
                // the hour expansion stays below the threshold
                assertTickExprBindVariableMatches(
                        "[$yesterday..$today]T[00..23]#Tue",
                        "24\t2024-01-02T00:00:00.000000Z\t2024-01-02T23:00:00.000000Z"
                );
                assertTickExprBindVariableMatches(
                        "[$yesterday..$today]T[00..23]:[00..59]#Tue",
                        "24\t2024-01-02T00:00:00.000000Z\t2024-01-02T23:00:00.000000Z"
                );
                // +02:00 shifts both days two hours back
                assertTickExprBindVariableMatches(
                        "[$yesterday..$today]T[00..23]:[00..59]@+02:00",
                        "48\t2024-01-01T22:00:00.000000Z\t2024-01-03T21:00:00.000000Z"
                );
                // $now ends the range at 10:00 today
                assertTickExprBindVariableMatches(
                        "[$yesterday..$now]T[00..23]:[00..59]",
                        "35\t2024-01-02T00:00:00.000000Z\t2024-01-03T10:00:00.000000Z"
                );
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDateVariableRangeWithTimeListInDstGap() throws Exception {
        // On 2024-03-31 London clocks skip from 01:00 to 02:00 local time, and $now - 2h = 01:40
        // local falls in that hour. The range compares the slots with it in local time: today's
        // 01:30 is before it, 02:30 BST (01:30Z) after it. The literal compiles to a
        // CompiledTickExpression, while the parser evaluates the bind variable, and both must
        // select the same rows.
        assertMemoryLeak(() -> {
            // rows every 30 minutes from 2024-03-30T00:00 to 2024-04-01T23:30
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-03-30T00:00:00.000000Z', 1_800_000_000L) ts
                        FROM long_sequence(144)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-03-31T03:40:00.000000Z"));
            try {
                // today's 02:30 BST, and tomorrow's 00:30, 01:30 and 02:30 BST
                final String expectedRow = "4\t2024-03-31T01:30:00.000000Z\t2024-04-01T01:30:00.000000Z";
                final String tickExpr = "[$now - 2h..$tomorrow]T[00:30,01:30,02:30]@Europe/London";
                assertTickExprMatches(tickExpr, expectedRow);
                assertTickExprBindVariableMatches(tickExpr, expectedRow);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDenseTickExprBindVariable() throws Exception {
        // A bind variable goes through the parse path, which merges the expanded intervals
        // once they reach the incremental merge threshold (256 intervals). The merge must not
        // move the intervals that the clip to $now, or the timezone of an element, has yet
        // to process.
        assertMemoryLeak(() -> {
            createHourlyTable();
            // only an interval made of stale values would match this row
            execute("INSERT INTO t VALUES ('1970-01-01T00:40:00.000000Z')");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:30:00.000000Z"));

            final ObjList<BindVarTuple> cases = new ObjList<>();
            addTickExprBindCase(cases, "[$now..$tomorrow]T[00..23]:[00..59]", "37\t2024-01-02T11:00:00.000000Z\t2024-01-03T23:00:00.000000Z");
            addTickExprBindCase(cases, "[$yesterday..$now]T[00..23]:[00..59]", "35\t2024-01-01T00:00:00.000000Z\t2024-01-02T10:00:00.000000Z");
            // the first second of every minute from 09:00 to 12:59, which matches the rows at 09:00 to 12:00
            addTickExprBindCase(cases, "[$tomorrow, $yesterday..$now]T[09..12]:[00..59]:00", "10\t2024-01-01T09:00:00.000000Z\t2024-01-03T12:00:00.000000Z");
            // 2024-01-01 at +02:00 runs from 2023-12-31T22:00Z to 2024-01-01T21:59:59.999999Z
            addTickExprBindCase(cases, "[2024-01-03, 2024-01-01@+02:00]T[00..23]:[00..59]", "46\t2024-01-01T00:00:00.000000Z\t2024-01-03T23:00:00.000000Z");
            // the five minutes are 06:00Z to 10:00Z
            addTickExprBindCase(cases, "2024-01-02T[10:00,09:00,08:00,11:00,12:00]:[00..59]@+02:00", "5\t2024-01-02T06:00:00.000000Z\t2024-01-02T10:00:00.000000Z");

            // the IN on the designated timestamp becomes an interval scan
            assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .assertBinds(cases);
            // the OR keeps the IN in the row filter
            assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1 OR ts = 0")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .assertBinds(cases);
        });
    }

    @Test
    public void testDstGapIntervalBindVariable() throws Exception {
        // London springs forward from 01:00 GMT to 02:00 BST on 2024-03-31, so local times from
        // 01:00 to 01:59 do not exist: the interval starts at the transition, 01:00 UTC, and keeps
        // the five minutes from 02:00 BST that exist
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-03-31T00:00:00.000000Z', 60_000_000L) ts
                        FROM long_sequence(180)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok(
                    "starts in the gap",
                    "count\tmin\tmax\n5\t2024-03-31T01:00:00.000000Z\t2024-03-31T01:04:00.000000Z\n",
                    bindVariableService -> bindVariableService.setStr(0, "2024-03-31T01:10@Europe/London;55m")
            ));
            assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .assertBinds(cases);
        });
    }

    @Test
    public void testDstOverlapTickExpr() throws Exception {
        // On 2024-10-27 Berlin clocks fall back from 03:00 CEST to 02:00 CET at 01:00Z, so local
        // times from 02:00 to 02:59 happen twice. A literal, which compiles, and a bind variable,
        // which the parser evaluates, must both select the rows of both occurrences.
        assertMemoryLeak(() -> {
            // a row every ten minutes from 2024-10-26T20:00Z to 2024-10-28T01:50Z
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-10-26T20:00:00.000000Z', 600_000_000L) ts
                        FROM long_sequence(180)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-10-27T12:00:00.000000Z"));
            try {
                // 02:30 local time is 00:30Z in CEST and 01:30Z in CET
                final String minute = "2\t2024-10-27T00:30:00.000000Z\t2024-10-27T01:30:00.000000Z";
                assertTickExprMatches("[$today]T02:30@Europe/Berlin", minute);
                assertTickExprBindVariableMatches("[$today]T02:30@Europe/Berlin", minute);
                assertTickExprMatches("2024-10-27T02:30@Europe/Berlin", minute);
                // every minute of the day covers its 25 hours, the same as the day itself
                final String day = "150\t2024-10-26T22:00:00.000000Z\t2024-10-27T22:50:00.000000Z";
                assertTickExprMatches("[$today]T[00..23]:[00..59]@Europe/Berlin", day);
                assertTickExprBindVariableMatches("[$today]T[00..23]:[00..59]@Europe/Berlin", day);
                assertTickExprMatches("$today@Europe/Berlin", day);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testIntervalBindVariable() throws Exception {
        execute("create table test as (select rnd_int() a, timestamp_sequence(0, 1000) ts from long_sequence(10000))");

        // baseline
        assertQuery("select timestamp_floor('1s', ts), count() from test where ts in '1970-01-01T00:00:02'")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        timestamp_floor\tcount
                        1970-01-01T00:00:02.000000Z\t1000
                        """);

        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(
                "2s",
                """
                        timestamp_floor\tcount
                        1970-01-01T00:00:02.000000Z\t1000
                        """,
                bindVariableService -> bindVariableService.setStr(0, "1970-01-01T00:00:02")
        ));

        cases.add(BindVarTuple.fails(
                "int interval",
                64,
                "unsupported bind variable type [INT] expected one of [STRING or VARCHAR]",
                bindVariableService -> bindVariableService.setInt(0, 10)
        ));

        cases.add(BindVarTuple.ok(
                "2s",
                """
                        timestamp_floor\tcount
                        1970-01-01T00:00:03.000000Z\t1000
                        """,
                bindVariableService -> bindVariableService.setStr(0, "1970-01-01T00:00:03")
        ));

        assertQuery("select timestamp_floor('1s', ts), count() from test where ts in $1")
                .noLeakCheck()
                .expectSize()
                .assertBinds(cases);
    }

    @Test
    public void testIntervalLiteralCompilesToOver255Elements() throws Exception {
        // The literal compiles to 481 elements: $today and 480 half-hourly intervals that do not
        // merge, more than the compiled expression could once count
        assertMemoryLeak(() -> {
            // rows every 30 minutes from 2024-01-01T00:00 to 2024-01-10T23:30
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 1_800_000_000L) ts
                        FROM long_sequence(480)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-03-01T12:00:00.000000Z"));
            assertQuery("SELECT count() FROM t WHERE ts IN '[$today,2024-01-[01..10]T[00..23]:[00,30]]'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            480
                            """);
        });
    }

    @Test
    public void testListOfTimestamps() throws Exception {
        execute("create table test as (select rnd_int() a, timestamp_sequence(0, 1000) ts from long_sequence(100))");

        assertQuery("test where ts in ('1970-01-01T00:00:00.070000Z', 77000, '1970-01-01'::date)")
                .noLeakCheck()
                .returns("""
                        a\tts
                        -1148479920\t1970-01-01T00:00:00.000000Z
                        -2144581835\t1970-01-01T00:00:00.070000Z
                        -296610933\t1970-01-01T00:00:00.077000Z
                        """);
    }

    @Test
    public void testListOfTimestampsInvalidInput() throws Exception {
        execute("create table test as (select rnd_int() a, timestamp_sequence(0, 1000) ts from long_sequence(100))");

        assertQuery("test where ts in ('1970-01-01T00:00:0.070000Z', 'abc')")
                .fails(18, "Invalid date [str=1970-01-01T00:00:0.070000Z]");
    }

    @Test
    public void testListOfTimestampsUnsupportedType() throws Exception {
        execute("create table test as (select rnd_int() a, timestamp_sequence(0, 1000) ts from long_sequence(100))");

        assertQuery("test where ts in ('1970-01-01T00:00:00.070000Z', true)")
                .fails(49, "cannot compare TIMESTAMP with type BOOLEAN");
    }

    @Test
    public void testNestedDateAndTimeLists() throws Exception {
        assertQuery("""
                SELECT
                    '2024-01-15T09:00:00'::TIMESTAMP IN '[2024-01-15,2024-01-16]T[09:00,14:30]:[00,30]' AS micro,
                    '2024-01-16T14:30:30.999999999'::TIMESTAMP_NS IN '[2024-01-15,2024-01-16]T[09:00,14:30]:[00,30]' AS nano,
                    '2024-01-16T14:30:31'::TIMESTAMP IN '[2024-01-15,2024-01-16]T[09:00,14:30]:[00,30]' AS outside
                """).expectSize().returns("""
                micro	nano	outside
                true	true	false
                """);
    }

    @Test
    public void testNotInTickExprThatExpandsToNothing() throws Exception {
        // A tick expression that expands to no interval excludes no row. 2024-01-02 is a Tuesday,
        // and $today, 2024-01-01, is a Monday.
        assertMemoryLeak(() -> {
            createJanuaryAndSpringTable();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-01T12:00:00.000000Z"));
            try {
                final String everyRow = "408\t2024-01-01T00:00:00.000000Z\t2024-04-03T23:00:00.000000Z";
                assertIntrinsicRows("ts NOT IN '2024-01-02#Mon'", everyRow);
                assertIntrinsicRows("ts NOT IN '$today#Sat'", everyRow);
                assertIntrinsicRows("NOT (ts IN '2024-01-02#Mon')", everyRow);
                final String firstThreeDays = "72\t2024-01-01T00:00:00.000000Z\t2024-01-03T23:00:00.000000Z";
                assertIntrinsicRowsInBothOrders("ts NOT IN '$today#Sat'", " AND ", "ts IN '2024-01-[01..03]'", firstThreeDays);
                assertIntrinsicRowsInBothOrders("ts NOT IN '2024-01-02#Mon'", " AND ", "ts IN '$today..$today+2d'", firstThreeDays);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testNowInTickExprWithDateVariableReevaluatesOnCachedExecution() throws Exception {
        assertMemoryLeak(() -> {
            // T1 = 1_000_000_000 micros (1000 seconds since epoch)
            long t1 = 1_000_000_000L;
            setCurrentMicros(t1);

            // Compile the query once; the factory will be reused (cached)
            try (RecordCursorFactory factory = select(
                    "SELECT 1 AS v FROM long_sequence(1) WHERE now() IN '$now-100s..$now'"
            )) {
                // First execution: now() == T1, interval is [T1-100s, T1] — should match
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue("first execution should return a row", cursor.hasNext());
                }

                // Advance now by 200 seconds (well past the first interval)
                long t2 = t1 + 200_000_000L;
                setCurrentMicros(t2);

                // Second execution with the same factory: now() == T2,
                // interval must re-evaluate to [T2-100s, T2] — should still match
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue("second execution should return a row after now() advances", cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testTickExprAfterDynamicInterval() throws Exception {
        // An interval scan combines its conditions in the order the query lists them, so a static
        // tick expression can follow a condition that only resolves when the query runs. The
        // expression must still select the same rows as when it comes first.
        assertMemoryLeak(() -> {
            createJanuaryAndSpringTable();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-01T12:00:00.000000Z"));
            try {
                // a tick expression with several intervals intersects as one set: none of the
                // 08:00s of 2024-01-02 and 2024-01-03 falls on $today
                assertIntrinsicRowsInBothOrders("ts IN '[2024-01-02,2024-01-03]T08:00'", " AND ", "ts IN '$today'", "0\t\t");
                assertIntrinsicRowsInBothOrders(
                        "ts IN '[2024-01-01,2024-01-03]T08:00'",
                        " AND ",
                        "ts IN '$today'",
                        "1\t2024-01-01T08:00:00.000000Z\t2024-01-01T08:00:00.000000Z"
                );
                // 08:00 on 2024-01-02 at +09:00 is 2024-01-01T23:00Z, on $today
                assertIntrinsicRowsInBothOrders(
                        "ts IN '2024-01-[02..03]T08:00@+09:00'",
                        " AND ",
                        "ts IN '$today'",
                        "1\t2024-01-01T23:00:00.000000Z\t2024-01-01T23:00:00.000000Z"
                );
                // a day filter keeps the Tuesday, 2024-01-02, in local time
                assertIntrinsicRowsInBothOrders(
                        "ts IN '2024-01-[01..07]T08:00#Tue'",
                        " AND ",
                        "ts IN '$today..$today+10d'",
                        "1\t2024-01-02T08:00:00.000000Z\t2024-01-02T08:00:00.000000Z"
                );
                // 08:00 in Tokyo on Tuesday 2024-01-02 is 2024-01-01T23:00Z
                assertIntrinsicRowsInBothOrders(
                        "ts IN '2024-01-[01..07]T08:00@Asia/Tokyo#Tue'",
                        " AND ",
                        "ts IN '$today..$today+10d'",
                        "1\t2024-01-01T23:00:00.000000Z\t2024-01-01T23:00:00.000000Z"
                );
                // 20:00 in New York on Monday 2024-01-01 is 2024-01-02T01:00Z
                assertIntrinsicRowsInBothOrders(
                        "ts IN '2024-01-[01..07]T20:00@America/New_York#Mon'",
                        " AND ",
                        "ts IN '$today..$today+10d'",
                        "1\t2024-01-02T01:00:00.000000Z\t2024-01-02T01:00:00.000000Z"
                );
                // 2024-01-02 is not a Monday, so the expression selects nothing
                assertIntrinsicRowsInBothOrders("ts IN '2024-01-02#Mon'", " AND ", "ts IN '$today..$today+10d'", "0\t\t");
                // a repeating interval keeps the days that the day filter keeps
                assertIntrinsicRowsInBothOrders(
                        "ts IN '2024-01-01T09:00#Tue;1h;1d;7'",
                        " AND ",
                        "ts IN '$today..$today+10d'",
                        "1\t2024-01-02T09:00:00.000000Z\t2024-01-02T09:00:00.000000Z"
                );
                // a subtraction removes 08:00 in Tokyo on Tuesday 2024-01-02, 2024-01-01T23:00Z
                assertIntrinsicRowsInBothOrders(
                        "ts NOT IN '2024-01-02T08:00@Asia/Tokyo#Tue'",
                        " AND ",
                        "ts IN '$today..$today+3d'",
                        "95\t2024-01-01T00:00:00.000000Z\t2024-01-04T23:00:00.000000Z"
                );
                assertIntrinsicRowsInBothOrders(
                        "ts NOT IN '[2024-01-02,2024-01-03]'",
                        " AND ",
                        "ts IN '$today..$today+3d'",
                        "48\t2024-01-01T00:00:00.000000Z\t2024-01-04T23:00:00.000000Z"
                );
                // a union adds 08:00 in Tokyo on Wednesday 2024-01-03, 2024-01-02T23:00Z
                assertIntrinsicRowsInBothOrders(
                        "ts IN '2024-01-03T08:00@Asia/Tokyo#Wed'",
                        " OR ",
                        "ts IN '$today'",
                        "25\t2024-01-01T00:00:00.000000Z\t2024-01-02T23:00:00.000000Z"
                );
                assertIntrinsicRowsInBothOrders(
                        "ts IN '[2024-01-02,2024-01-03]T08:00'",
                        " OR ",
                        "ts IN '$today'",
                        "26\t2024-01-01T00:00:00.000000Z\t2024-01-03T08:00:00.000000Z"
                );

                // London clocks spring forward on 2024-03-31: the daily 09:00 is 09:00Z, then 08:00Z
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-03-30T12:00:00.000000Z"));
                assertIntrinsicRowsInBothOrders(
                        "ts IN '2024-03-30T09:00@Europe/London;1h;1d;3'",
                        " AND ",
                        "ts IN '$today..$today+10d'",
                        "3\t2024-03-30T09:00:00.000000Z\t2024-04-01T08:00:00.000000Z"
                );
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testTickExprOrWithEmptyFirstBranch() throws Exception {
        // A union starts from its first branch. When that branch selects nothing, the union is
        // the rest of the branches. 2024-01-02 is a Tuesday, not a Monday.
        assertMemoryLeak(() -> {
            createJanuaryAndSpringTable();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-01T12:00:00.000000Z"));
            try {
                final String january3 = "24\t2024-01-03T00:00:00.000000Z\t2024-01-03T23:00:00.000000Z";
                assertIntrinsicRowsInBothOrders("ts IN '2024-01-02#Mon'", " OR ", "ts IN '2024-01-03'", january3);
                assertIntrinsicRowsInBothOrders("ts IN '$today#Sat'", " OR ", "ts IN '2024-01-03'", january3);
                assertIntrinsicRowsInBothOrders(
                        "ts IN '2024-01-02#Mon'",
                        " OR ",
                        "ts IN '$today'",
                        "24\t2024-01-01T00:00:00.000000Z\t2024-01-01T23:00:00.000000Z"
                );
                assertIntrinsicRowsInBothOrders(
                        "ts IN '2024-01-02#Mon'",
                        " OR ",
                        "ts = '2024-01-03T05:00:00.000000Z'",
                        "1\t2024-01-03T05:00:00.000000Z\t2024-01-03T05:00:00.000000Z"
                );
                assertIntrinsicRowsInBothOrders(
                        "ts IN '2024-01-02#Mon'",
                        " OR ",
                        "ts = now()",
                        "1\t2024-01-01T12:00:00.000000Z\t2024-01-01T12:00:00.000000Z"
                );
                // a condition that selects nothing after the union still empties it
                assertIntrinsicRows("(ts IN '2024-01-02#Mon' OR ts IN '2024-01-03') AND ts IN '2024-01-04'", "0\t\t");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testTickExprParseErrorFails() throws Exception {
        // A tick expression that does not parse fails the query with the parser's message, at the
        // position of the expression, whether it comes as a literal, as a bind variable or as a
        // value of each row, rather than matching no row
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-02T00:00:00.000000Z', 3_600_000_000L) ts,
                               '2024-01-02T08:00@Bogus/Zone' s
                        FROM long_sequence(24)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final String tickExpr = "2024-01-02T08:00@Bogus/Zone";
            final String error = "invalid timezone: Bogus/Zone";
            assertExceptionNoLeakCheck("SELECT count() FROM t WHERE ts IN '" + tickExpr + "'", 34, error);
            bindVariableService.clear();
            bindVariableService.setStr(0, tickExpr);
            // the interval scan
            assertExceptionNoLeakCheck("SELECT count() FROM t WHERE ts IN $1", 34, error);
            assertExceptionNoLeakCheck("SELECT count() FROM t WHERE ts NOT IN $1", 38, error);
            // the row filter
            assertExceptionNoLeakCheck("SELECT count() FROM t WHERE ts IN $1 OR ts = 0", 34, error);
            // a value of each row
            assertExceptionNoLeakCheck("SELECT count() FROM t WHERE ts IN s", 34, error);
            // An error that the parser places inside the value points at the bind variable, since the
            // value is not part of the SQL text
            final String rangeError = "Invalid date range: start is after end";
            assertExceptionNoLeakCheck("SELECT count() FROM t WHERE ts IN '[$tomorrow..$today]'", 34, rangeError);
            bindVariableService.setStr(0, "[$tomorrow..$today]");
            assertExceptionNoLeakCheck("SELECT count() FROM t WHERE ts IN $1", 34, rangeError);
            assertExceptionNoLeakCheck("SELECT count() FROM t WHERE ts IN $1 OR ts = 0", 34, rangeError);
            // A null bind variable is not an error, and matches no row. Neither is an expression that
            // selects nothing: 2024-01-02 is a Tuesday.
            for (String value : new String[]{null, "2024-01-02#Mon"}) {
                bindVariableService.setStr(0, value);
                for (String sql : new String[]{"SELECT count() FROM t WHERE ts IN $1", "SELECT count() FROM t WHERE ts IN $1 OR ts = 0"}) {
                    assertQuery(sql)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\n0\n");
                }
            }
            // and excluding nothing keeps every row
            bindVariableService.setStr(0, "2024-01-02#Mon");
            assertQuery("SELECT count() FROM t WHERE ts NOT IN $1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n24\n");
        });
    }

    @Test
    public void testTickExprThatExpandsToNothingInRowFilter() throws Exception {
        // An OR with a condition that is not a timestamp interval keeps the IN in the row filter.
        // A tick expression with no interval at all, here a day filter that drops its only day,
        // never matches, and the JIT compiles it to a comparison that is always false.
        assertMemoryLeak(() -> {
            // hourly rows from 2024-01-01T00:00 to 2024-01-04T23:00, in ts and in ts2
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts,
                               timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts2
                        FROM long_sequence(96)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final int jitMode = sqlExecutionContext.getJitMode();
            try {
                for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    sqlExecutionContext.setJitMode(mode);
                    final String groupBy = mode == SqlJitMode.JIT_MODE_ENABLED && JitUtil.isJitSupported()
                            ? "Async JIT Group By"
                            : "Async Group By";
                    assertFilterRows("ts IN '2024-01-02#Mon' OR ts > '2030-01-01'", groupBy, "0\t\t");
                    assertFilterRows("ts NOT IN '2024-01-02#Mon' OR ts > '2030-01-01'", groupBy, "96\t2024-01-01T00:00:00.000000Z\t2024-01-04T23:00:00.000000Z");
                    assertFilterRows("ts2 IN '2024-01-02#Mon'", groupBy, "0\t\t");
                    assertFilterRows("ts2 NOT IN '2024-01-02#Mon'", groupBy, "96\t2024-01-01T00:00:00.000000Z\t2024-01-04T23:00:00.000000Z");
                    assertFilterRows(
                            "(ts IN '2024-01-02#Mon' OR ts IN '2024-01-03') AND ts IN '2024-01-[03..04]'",
                            "Interval forward scan",
                            "24\t2024-01-03T00:00:00.000000Z\t2024-01-03T23:00:00.000000Z"
                    );
                }
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    @Test
    public void testTimeListWithTimezoneAndDayFilter() throws Exception {
        // The day filter judges the slots of a time list by their local day. 2024-01-02 is a
        // Tuesday, and 08:00 in Tokyo on that day is 2024-01-01T23:00Z, a Monday.
        assertMemoryLeak(() -> {
            createHourlyTable();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z"));
            try {
                final String expectedRow = "1\t2024-01-01T23:00:00.000000Z\t2024-01-01T23:00:00.000000Z";
                // without a date variable, the literal goes through the parser too
                assertTickExprMatches("2024-01-[02..04]T[08:00]@Asia/Tokyo#Tue", expectedRow);
                assertTickExprBindVariableMatches("2024-01-[02..04]T[08:00]@Asia/Tokyo#Tue", expectedRow);
                // $today is Tuesday 2024-01-02
                assertTickExprMatches("[$today..$today+2d]T[08:00]@Asia/Tokyo#Tue", expectedRow);
                assertTickExprBindVariableMatches("[$today..$today+2d]T[08:00]@Asia/Tokyo#Tue", expectedRow);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    private static void addTickExprBindCase(ObjList<BindVarTuple> cases, String tickExpr, String expectedRow) {
        cases.add(BindVarTuple.ok(
                tickExpr,
                "count\tmin\tmax\n" + expectedRow + "\n",
                bindVariableService -> bindVariableService.setStr(0, tickExpr)
        ));
    }

    private static void createHourlyTable() throws SqlException {
        // hourly rows from 2024-01-01T00:00 to 2024-01-04T23:00
        execute("""
                CREATE TABLE t AS (
                    SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts
                    FROM long_sequence(96)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
    }

    private static void createJanuaryAndSpringTable() throws SqlException {
        // hourly rows from 2024-01-01T00:00 to 2024-01-10T23:00, and from 2024-03-28T00:00 to
        // 2024-04-03T23:00, across the London spring-forward change on 2024-03-31
        execute("""
                CREATE TABLE t AS (
                    SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts
                    FROM long_sequence(240)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("INSERT INTO t SELECT timestamp_sequence('2024-03-28T00:00:00.000000Z', 3_600_000_000L) ts FROM long_sequence(168)");
    }

    private static String insertEveryTenMinutes(String from, int count) {
        return "INSERT INTO t SELECT ts, ts FROM (SELECT '" + from + "'::timestamp + (x - 1) * 600_000_000 ts FROM long_sequence(" + count + "))";
    }

    /**
     * Returns an INSERT of rows at 09:00, 10:30 and 14:30 of the day, in ts and in ts2.
     */
    private static String insertSlotRows(String day) {
        return "INSERT INTO t VALUES ('" + day + "T09:00:00.000000Z', '" + day + "T09:00:00.000000Z'), ('"
                + day + "T10:30:00.000000Z', '" + day + "T10:30:00.000000Z'), ('"
                + day + "T14:30:00.000000Z', '" + day + "T14:30:00.000000Z')";
    }

    /**
     * Compiles the query once at 10:00, runs it, then runs the same factory again at 15:00 after more rows
     * arrive. The table also holds a row at 23:00 that $now never reaches.
     */
    private void assertCachedFilterFollowsNow(String query, String expectedAt10, String expectedAt15) throws Exception {
        setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
        try {
            // The JIT is the path that compiles the filter to native code, so pin it on
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            final ObjList<MutationStep> steps = new ObjList<>();
            // a row every 10 minutes from midnight to 10:00
            steps.add(MutationStep.of(insertEveryTenMinutes("2024-01-02T00:00:00.000000Z", 61), expectedAt10));
            // and from 10:10 to 15:00, written after the factory compiled
            steps.add(MutationStep.of(
                    () -> setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T15:00:00.000000Z")),
                    insertEveryTenMinutes("2024-01-02T10:10:00.000000Z", 30),
                    expectedAt15
            ));
            assertQuery(query)
                    .noRandomAccess()
                    .expectSize()
                    .ddl(
                            "CREATE TABLE t (ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                            "INSERT INTO t VALUES ('2024-01-02T23:00:00.000000Z', '2024-01-02T23:00:00.000000Z')"
                    )
                    .mutateStepwise(steps);
        } finally {
            setCurrentMicros(-1);
        }
    }

    private void assertFilterRows(String condition, String expectedPlan, String expectedRow) throws Exception {
        assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE " + condition)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlanContaining(expectedPlan)
                .returns("count\tmin\tmax\n" + expectedRow + "\n");
    }

    /**
     * Compiles the query on 2024-01-02 and runs it over the rows of that day, then runs the same
     * factory again on 2024-01-03, after the rows of that day arrive. Each day has rows at 09:00,
     * 10:30 and 14:30, see {@link #insertSlotRows}.
     */
    private void assertHeldFactoryFollowsDayChange(String query, String expectedRowOnDay1, String expectedRowOnDay2) throws Exception {
        execute("DROP TABLE IF EXISTS t");
        setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z"));
        try {
            final ObjList<MutationStep> steps = new ObjList<>();
            steps.add(MutationStep.of(insertSlotRows("2024-01-02"), "count\tmin\tmax\n" + expectedRowOnDay1 + "\n"));
            steps.add(MutationStep.of(
                    () -> setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-03T12:00:00.000000Z")),
                    insertSlotRows("2024-01-03"),
                    "count\tmin\tmax\n" + expectedRowOnDay2 + "\n"
            ));
            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .ddl("CREATE TABLE t (ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY")
                    .mutateStepwise(steps);
        } finally {
            setCurrentMicros(-1);
        }
    }

    private void assertIntrinsicRows(String condition, String expectedRow) throws Exception {
        assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE " + condition)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\tmin\tmax\n" + expectedRow + "\n");
    }

    /**
     * Asserts the rows of {@code a join b} and of {@code b join a}. Both conditions narrow the
     * interval scan, which reads no partition at all when they select nothing.
     */
    private void assertIntrinsicRowsInBothOrders(String a, String join, String b, String expectedRow) throws Exception {
        for (String condition : new String[]{a + join + b, b + join + a}) {
            assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE " + condition)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining(expectedRow.startsWith("0\t") ? "" : "Interval forward scan")
                    .returns("count\tmin\tmax\n" + expectedRow + "\n");
        }
    }

    private void assertTickExprBindVariableMatches(String tickExpr, String expectedRow) throws Exception {
        final String expected = "count\tmin\tmax\n" + expectedRow + "\n";
        bindVariableService.clear();
        bindVariableService.setStr(0, tickExpr);
        // the interval scan parses the bind variable when the query executes
        assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("Interval forward scan")
                .returns(expected);
        // the OR keeps the IN in the row filter, which parses the bind variable on its own
        assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1 OR ts > '2030-01-01'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("Frame forward scan")
                .returns(expected);
    }

    private void assertTickExprMatches(String tickExpr, String expectedRow) throws Exception {
        final String expected = "count\tmin\tmax\n" + expectedRow + "\n";
        // the IN on the designated timestamp becomes an interval scan
        assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN '" + tickExpr + "'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("Interval forward scan")
                .returns(expected);
        // WhereClauseParser folds an OR of IN and = on the designated timestamp into the interval scan,
        // but not an OR with a range, so this IN stays in the row filter and the scan reads every row.
        // The JIT serializes the filter on its own, so check with the JIT on and off.
        final String filterQuery = "SELECT count(), min(ts), max(ts) FROM t WHERE ts IN '" + tickExpr + "' OR ts > '2030-01-01'";
        final int jitMode = sqlExecutionContext.getJitMode();
        try {
            for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(mode);
                assertQuery(filterQuery)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("filter: (ts in [", "Frame forward scan")
                        .returns(expected);
            }
        } finally {
            sqlExecutionContext.setJitMode(jitMode);
        }
    }
}
