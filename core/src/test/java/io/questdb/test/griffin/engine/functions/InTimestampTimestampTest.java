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

import io.questdb.DefaultFactoryProvider;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.jit.JitUtil;
import io.questdb.mp.WorkerPool;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestTickCalendarService;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.MutationStep;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class InTimestampTimestampTest extends AbstractCairoTest {
    // a Friday that XNYS trades on from 14:30 to 21:00Z
    private static final long FRIDAY_NOON = MicrosFormatUtils.parseTimestamp("2025-01-24T12:00:00.000000Z");
    // the Monday after FRIDAY_NOON, which XNYS trades on from 14:30 to 21:00Z too
    private static final long MONDAY_NOON = MicrosFormatUtils.parseTimestamp("2025-01-27T12:00:00.000000Z");
    // 2024-01-02, before the range from 09:30 to "now" starts, and after
    private static final long NOW_0300 = MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z");
    private static final long NOW_1200 = MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z");
    private static final String RANGE_ORDER_ERROR = "Invalid date range: start is after end";

    @Test
    public void testBareDateVariableTickCalendarDurationErrorsFailQuery() throws Exception {
        // A bare date variable expression with a tick calendar and a duration that does not parse
        // fails the query: a bind variable as the query runs, at the bind variable, and a literal
        // that SQL parses when the query runs as the query compiles, at the literal. One-second
        // time slots from 00:00 to 12:59:30 are 1,560 intervals that do not join, more than the
        // 1,024 of the interval limit, before the tick calendar keeps what its sessions hold of
        // them, so they fail the query wherever it holds them, as their date list spelling does.
        useTickCalendar();
        assertMemoryLeak(() -> {
            createContextTables();
            setCurrentMicros(FRIDAY_NOON);
            try {
                bindVariableService.clear();
                bindVariableService.setStr(0, "$todayT[15:00,16:00]#XNYS;1x");
                final String bindSql = "SELECT count() FROM t WHERE ts IN $1";
                assertExceptionNoLeakCheck(bindSql, bindSql.indexOf("$1"), "Invalid duration unit: x");
                bindVariableService.clear();
                final String literalSql = "SELECT count() FROM t WHERE ts IN '$todayT[15:00@UTC]#XNYS;1x'";
                assertExceptionNoLeakCheck(literalSql, literalSql.indexOf('\''), "Invalid duration unit: x");
                // a '#' in a bracket that does not close is no tick calendar: the IR fails the literal
                // on the time list as the query compiles, and the parser the bind variable on the bracket
                final String unclosed = "$todayT[15:00#XNYS;1h";
                final String unclosedSql = "SELECT count() FROM t WHERE ts IN '" + unclosed + "'";
                assertExceptionNoLeakCheck(unclosedSql, unclosedSql.indexOf('\''), "Unclosed '[' in time list");
                bindVariableService.setStr(0, unclosed);
                assertExceptionNoLeakCheck(bindSql, bindSql.indexOf("$1"), "Unclosed '[' in interval");
                bindVariableService.clear();
                final String overLimit = "$todayT[00..12]:[00..59]:[00,30]#XNYS;30s";
                final String limitError = "Bracket expansion produces too many intervals (max 1024)";
                assertTickExprFailsInEveryContext(overLimit, limitError, overLimit, limitError);
                // the table keeps answering
                assertIntrinsicRows("ts IN '[2024-01-02]T[09:00,10:00]'", "2\t2024-01-02T09:00:00.000000Z\t2024-01-02T10:00:00.000000Z");
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testBareDateVariableTickCalendarDurationInEveryContext() throws Exception {
        // A bare date variable expression with a tick calendar and a duration selects the same rows
        // wherever a query holds it, as a literal that SQL parses when the query runs, the time list
        // slot at 15:00 with a timezone of its own, and as a bind variable, the slots at 15:00 and
        // 16:00. On Friday 2025-01-24, XNYS trades from 14:30 to 21:00Z, so the literal selects the
        // half hours from 15:00 to 16:00, three rows, one of them of symbol 'a', and the bind
        // variable those from 15:00 to 17:00, five rows, two of them 'a'. The table has the 48 half
        // hours of that day, 24 of each symbol.
        useTickCalendar();
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2025-01-24T00:00:00.000000Z', 1_800_000_000L) ts,
                               (CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END)::SYMBOL s
                        FROM long_sequence(48)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("CREATE TABLE ti (ts TIMESTAMP, s SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO ti SELECT ts, s FROM t");
            setCurrentMicros(FRIDAY_NOON);
            try {
                // ? stands for the tick expression; the rows of the literal, then of the bind variable
                final String[][] contexts = {
                        {"WITH c AS (SELECT * FROM t WHERE ts IN ?) SELECT count() FROM c", "count\n3\n", "count\n5\n"},
                        {"SELECT count() FROM (SELECT * FROM t WHERE ts IN ?)", "count\n3\n", "count\n5\n"},
                        {"SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a JOIN t b ON a.s = b.s", "count\n72\n", "count\n120\n"},
                        {"SELECT count() FROM t a JOIN (SELECT * FROM t WHERE ts IN ?) b ON a.s = b.s", "count\n72\n", "count\n120\n"},
                        {"SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a LEFT JOIN t b ON a.s = b.s", "count\n72\n", "count\n120\n"},
                        {"SELECT count() FROM t a LEFT JOIN (SELECT * FROM t WHERE ts IN ?) b ON a.s = b.s", "count\n72\n", "count\n120\n"},
                        {
                                "SELECT count(), max(b.ts) FROM (SELECT * FROM t WHERE ts IN ?) a ASOF JOIN t b",
                                "count\tmax\n3\t2025-01-24T16:00:00.000000Z\n",
                                "count\tmax\n5\t2025-01-24T17:00:00.000000Z\n"
                        },
                        {
                                "SELECT count(), max(b.ts) FROM t a ASOF JOIN (SELECT * FROM t WHERE ts IN ?) b",
                                "count\tmax\n48\t2025-01-24T16:00:00.000000Z\n",
                                "count\tmax\n48\t2025-01-24T17:00:00.000000Z\n"
                        },
                        {"SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a CROSS JOIN t b", "count\n144\n", "count\n240\n"},
                        {"SELECT count() FROM t a CROSS JOIN (SELECT * FROM t WHERE ts IN ?) b", "count\n144\n", "count\n240\n"},
                        // the two rows before 01:00 and the rows of the expression
                        {"SELECT count() FROM (SELECT ts FROM t WHERE ts IN ? UNION SELECT ts FROM t WHERE ts < '2025-01-24T01:00')", "count\n5\n", "count\n7\n"},
                        {"SELECT count() FROM (SELECT ts FROM t WHERE ts < '2025-01-24T01:00' UNION SELECT ts FROM t WHERE ts IN ?)", "count\n5\n", "count\n7\n"},
                        // the rows of the expression from 16:00 on
                        {"SELECT count() FROM (SELECT ts FROM t WHERE ts IN ? EXCEPT SELECT ts FROM t WHERE ts < '2025-01-24T16:00')", "count\n1\n", "count\n3\n"},
                        // the nine rows from 14:00 to 18:00 but those of the expression
                        {
                                "SELECT count() FROM (SELECT ts FROM t WHERE ts BETWEEN '2025-01-24T14:00' AND '2025-01-24T18:00' EXCEPT SELECT ts FROM t WHERE ts IN ?)",
                                "count\n6\n",
                                "count\n4\n"
                        },
                        {"SELECT count() FROM (SELECT ts FROM t WHERE ts IN ? INTERSECT SELECT ts FROM t WHERE ts >= '2025-01-24T16:00')", "count\n1\n", "count\n3\n"},
                        {"SELECT count() FROM (SELECT ts FROM t WHERE ts >= '2025-01-24T16:00' INTERSECT SELECT ts FROM t WHERE ts IN ?)", "count\n1\n", "count\n3\n"},
                        {"SELECT s, count() FROM t WHERE ts IN ? ORDER BY s", "s\tcount\na\t1\nb\t2\n", "s\tcount\na\t2\nb\t3\n"},
                        {
                                "SELECT ts, count() FROM t WHERE ts IN ? SAMPLE BY 1h",
                                "ts\tcount\n2025-01-24T15:00:00.000000Z\t2\n2025-01-24T16:00:00.000000Z\t1\n",
                                "ts\tcount\n2025-01-24T15:00:00.000000Z\t2\n2025-01-24T16:00:00.000000Z\t2\n2025-01-24T17:00:00.000000Z\t1\n"
                        },
                        {
                                "SELECT s, ts FROM (SELECT * FROM t WHERE ts IN ? LATEST ON ts PARTITION BY s) ORDER BY s",
                                "s\tts\na\t2025-01-24T15:30:00.000000Z\nb\t2025-01-24T16:00:00.000000Z\n",
                                "s\tts\na\t2025-01-24T16:30:00.000000Z\nb\t2025-01-24T17:00:00.000000Z\n"
                        },
                        {
                                "SELECT ts, s, row_number() OVER (PARTITION BY s ORDER BY ts) FROM t WHERE ts IN ?",
                                "ts\ts\trow_number\n2025-01-24T15:00:00.000000Z\tb\t1\n2025-01-24T15:30:00.000000Z\ta\t1\n2025-01-24T16:00:00.000000Z\tb\t2\n",
                                "ts\ts\trow_number\n2025-01-24T15:00:00.000000Z\tb\t1\n2025-01-24T15:30:00.000000Z\ta\t1\n2025-01-24T16:00:00.000000Z\tb\t2\n"
                                        + "2025-01-24T16:30:00.000000Z\ta\t2\n2025-01-24T17:00:00.000000Z\tb\t3\n"
                        },
                        {
                                "SELECT ts, s FROM t WHERE ts IN ? ORDER BY ts LIMIT -1",
                                "ts\ts\n2025-01-24T16:00:00.000000Z\tb\n",
                                "ts\ts\n2025-01-24T17:00:00.000000Z\tb\n"
                        },
                        // a symbol without an index, and one with an index
                        {"SELECT count() FROM t WHERE s = 'a' AND ts IN ?", "count\n1\n", "count\n2\n"},
                        {"SELECT count() FROM ti WHERE s = 'a' AND ts IN ?", "count\n1\n", "count\n2\n"}
                };
                for (String[] context : contexts) {
                    bindVariableService.clear();
                    assertQuery(context[0].replace("?", "'$todayT[15:00@UTC]#XNYS;1h'"))
                            .noLeakCheck()
                            .inferRandomAccess()
                            .inferTimestamp()
                            .sizeMayVary()
                            .returns(context[1]);
                    bindVariableService.setStr(0, "$todayT[15:00,16:00]#XNYS;1h");
                    assertQuery(context[0].replace("?", "$1"))
                            .noLeakCheck()
                            .inferRandomAccess()
                            .inferTimestamp()
                            .sizeMayVary()
                            .returns(context[2]);
                }
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testBareDateVariableTickCalendarDurationInEveryFilterPath() throws Exception {
        // A bare date variable expression with a tick calendar and a duration takes each time slot,
        // or the day, with its own extent, the tick calendar keeps what its sessions hold of them,
        // and the duration extends what they keep by an hour, once. On Friday 2025-01-24, XNYS trades
        // from 14:30 to 21:00Z: the minutes at 15:00 and 16:00 an hour longer hold the half hours
        // from 15:00 to 17:00, the minute at 15:00 those from 15:00 to 16:00, and the session those
        // from 14:30 to 21:30.
        useTickCalendar();
        assertMemoryLeak(() -> {
            createHalfHourTables();
            setCurrentMicros(FRIDAY_NOON);
            try {
                assertTickExprInEveryFilterPath("$todayT[15:00,16:00]#XNYS;1h", 5, "2025-01-24T15:00", "2025-01-24T17:00");
                assertTickExprInEveryFilterPath("$todayT[15:00@UTC]#XNYS;1h", 3, "2025-01-24T15:00", "2025-01-24T16:00");
                assertTickExprInEveryFilterPath("$todayT[15..16]:00#XNYS;1h", 5, "2025-01-24T15:00", "2025-01-24T17:00");
                assertTickExprInEveryFilterPath("$todayT15:00#XNYS;1h", 3, "2025-01-24T15:00", "2025-01-24T16:00");
                assertTickExprInEveryFilterPath("$today#XNYS;1h", 15, "2025-01-24T14:30", "2025-01-24T21:30");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testBareDateVariableTickCalendarDurationInView() throws Exception {
        // A view stores the literal and parses it again for each query, before and after the
        // engine releases what it holds: the minute at 15:00, which the session of Friday
        // 2025-01-24 from 14:30 to 21:00Z holds, an hour longer, holds the half hours from 15:00 to
        // 16:00.
        useTickCalendar();
        assertMemoryLeak(() -> {
            createHalfHourTables();
            setCurrentMicros(FRIDAY_NOON);
            try {
                execute("CREATE VIEW v AS (SELECT * FROM t WHERE ts IN '$todayT[15:00@UTC]#XNYS;1h')");
                drainViewQueue();
                final String expected = "count\tmin\tmax\n3\t2025-01-24T15:00:00.000000Z\t2025-01-24T16:00:00.000000Z\n";
                assertQuery("SELECT count(), min(ts), max(ts) FROM v")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
                engine.releaseInactive();
                assertQuery("SELECT count(), min(ts), max(ts) FROM v")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testBareDateVariableTickCalendarDurationPerMinute() throws Exception {
        // A bare date variable expression with a tick calendar and a duration, as a bind variable
        // and as the value of each row, over the minutes of 2025-01-24 to 2025-01-28, 7,200 rows. On
        // Friday 2025-01-24, XNYS trades from 14:30 to 21:00Z: the minutes at 15:00 and 16:00 an
        // hour longer hold the 121 minutes from 15:00 to 17:00, and the minute at 20:30 four hours
        // longer the 241 minutes from 20:30 to 00:30 on the next day, in its partition. The
        // Saturday has no session, and an empty table no row.
        useTickCalendar();
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE m AS (
                        SELECT timestamp_sequence('2025-01-24T00:00:00.000000Z', 60_000_000L) ts,
                               '$todayT[15:00,16:00]#XNYS;1h' tick_str,
                               '$todayT[15:00,16:00]#XNYS;1h'::VARCHAR tick_vc,
                               x v
                        FROM long_sequence(7_200)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("CREATE TABLE e (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            setCurrentMicros(FRIDAY_NOON);
            try {
                final String slots = "count\tmin\tmax\n121\t2025-01-24T15:00:00.000000Z\t2025-01-24T17:00:00.000000Z\n";
                bindVariableService.clear();
                bindVariableService.setStr(0, "$todayT[15:00,16:00]#XNYS;1h");
                assertCountMinMax("m", "ts IN $1", slots);
                assertCountMinMax("m", "ts NOT IN $1", "count\tmin\tmax\n7079\t2025-01-24T00:00:00.000000Z\t2025-01-28T23:59:00.000000Z\n");
                assertCountMinMax("e", "ts IN $1", "count\tmin\tmax\n0\t\t\n");
                bindVariableService.setStr(0, "$todayT20:30#XNYS;4h");
                assertCountMinMax("m", "ts IN $1", "count\tmin\tmax\n241\t2025-01-24T20:30:00.000000Z\t2025-01-25T00:30:00.000000Z\n");
                bindVariableService.clear();
                assertCountMinMax("m", "ts IN tick_str", slots);
                assertCountMinMax("m", "ts IN tick_vc", slots);

                // the minutes at 15:00 and at 17:00, each an hour longer, from both literals of an OR,
                // which the interval scan unites, and from both bind variables, in the row filter
                final String orSlots = "count\tmin\tmax\n122\t2025-01-24T15:00:00.000000Z\t2025-01-24T18:00:00.000000Z\n";
                assertQuery("SELECT count(), min(ts), max(ts) FROM m WHERE ts IN '$todayT[15:00@UTC]#XNYS;1h' OR ts IN '$todayT[17:00@UTC]#XNYS;1h'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining(
                                "Interval forward scan on: m",
                                "intervals: [(\"2025-01-24T15:00:00.000000Z\",\"2025-01-24T16:00:59.999999Z\"),(\"2025-01-24T17:00:00.000000Z\",\"2025-01-24T18:00:59.999999Z\")]"
                        )
                        .returns(orSlots);
                bindVariableService.setStr(0, "$todayT[15:00@UTC]#XNYS;1h");
                bindVariableService.setStr(1, "$todayT[17:00@UTC]#XNYS;1h");
                assertQuery("SELECT count(), min(ts), max(ts) FROM m WHERE ts IN $1 OR ts IN $2")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("filter: (ts in $0::string or ts in $1::string)")
                        .returns(orSlots);

                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-25T12:00:00.000000Z"));
                bindVariableService.clear();
                bindVariableService.setStr(0, "$todayT[15:00,16:00]#XNYS;1h");
                assertCountMinMax("m", "ts IN $1", "count\tmin\tmax\n0\t\t\n");
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

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
    public void testBindVariableDateVariableResolvesAgainstQueryClock() throws Exception {
        // A date variable in a bind variable resolves against the time the query started, as one in
        // a literal does, even when the clock moves on to the next day before the query parses the
        // bind variable. WAL apply relies on it: it fixes that time to the commit time.
        assertMemoryLeak(() -> {
            createHourlyTable();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T23:30:00.000000Z"));
            try {
                // the clock moves on, the query keeps the time it started at
                currentMicros = MicrosFormatUtils.parseTimestamp("2024-01-03T00:30:00.000000Z");
                final String day = "24\t2024-01-02T00:00:00.000000Z\t2024-01-02T23:00:00.000000Z";
                bindVariableService.clear();
                bindVariableService.setStr(0, "$today");
                // the interval scan and the row filter
                assertIntrinsicRows("ts IN $1", day);
                assertIntrinsicRows("ts IN $1 OR ts > '2030-01-01'", day);
                // and the literal
                assertIntrinsicRows("ts IN '$today'", day);
                assertIntrinsicRows("ts IN '$today' OR ts > '2030-01-01'", day);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testCompileProbeRestoredAfterJitOpen() throws Exception {
        // The JIT compiler opens the interval scan of a filter to read symbol tables as the query
        // compiles, at the "now" that the statement compiles at. The range from 09:30 to "now"
        // fails nothing there, and once the open is over, a query fails on the range when it runs
        // at 03:00: after a compile that keeps the JIT, after an open that fails with an error that
        // makes the query take the Java filter, and after an open that fails the compile, here on a
        // file of the table that a sub-query in the interval scan reads.
        final AtomicBoolean isOpenFailing = new AtomicBoolean();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (isOpenFailing.get() && Utf8s.containsAscii(name, "t2~") && Utf8s.endsWithAscii(name, "ts2.d")) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        final int jitMode = sqlExecutionContext.getJitMode();
        assertMemoryLeak(ff, () -> {
            createContextTables();
            execute("CREATE TABLE t2 (ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t2 VALUES ('2024-01-02T09:00:00.000000Z', '2024-01-02T09:30:00.000000Z')");
            final String jitFilter = JitUtil.isJitSupported() ? "Async JIT Filter" : "Async Filter";
            try {
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                // the context keeps 03:00, and the clock says 12:00
                setCurrentMicros(NOW_0300);
                currentMicros = NOW_1200;
                assertRangeQueryCompiles(jitFilter);

                final String multiRowSql = "SELECT count() FROM t WHERE ts > (SELECT ts FROM t) AND s = 'a'";
                try (RecordCursorFactory factory = select(multiRowSql)) {
                    Assert.assertFalse(sqlExecutionContext.isCompileProbe());
                    assertPlanContains(factory, "Async Filter");
                    assertRunFails(factory, multiRowSql, multiRowSql.indexOf("(SELECT") + 1, "scalar sub-query returned more than one row");
                }
                setCurrentMicros(NOW_0300);
                assertRangeQueryCompiles(jitFilter);

                final String fileSql = "SELECT count() FROM t WHERE ts > (SELECT min(ts2) FROM t2) AND s = 'a'";
                engine.releaseAllReaders();
                isOpenFailing.set(true);
                try {
                    try (RecordCursorFactory ignore = select(fileSql)) {
                        Assert.fail("compiled: " + fileSql);
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not open, file does not exist: ");
                        TestUtils.assertContains(e.getFlyweightMessage(), "ts2.d");
                    }
                    Assert.assertFalse(sqlExecutionContext.isCompileProbe());
                    // the Java filter opens nothing as the query compiles
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    try (RecordCursorFactory ignore = select(fileSql)) {
                        Assert.assertFalse(sqlExecutionContext.isCompileProbe());
                    }
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                } finally {
                    isOpenFailing.set(false);
                }
                engine.releaseAllReaders();
                setCurrentMicros(NOW_1200);
                try (RecordCursorFactory factory = select(fileSql)) {
                    assertPlanContains(factory, jitFilter);
                    // the rows with s = 'a', on odd hours, after 09:30 on 2024-01-02: 7 on that day, and
                    // 12 on each of the two days after
                    assertFactory(factory)
                            .withContext(sqlExecutionContext)
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\n31\n");
                }
                setCurrentMicros(NOW_0300);
                assertRangeQueryCompiles(jitFilter);
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDateListElementWithTimeListAndListTimeFailsAsLiteralAndAsBindVariable() throws Exception {
        // a month element with a time list of its own, in a date list with a time part: the literal
        // selected 10:00 of every day in January
        assertMalformedTickExprFails("[2024-01T[09:00,10:00], $today]T10:00", "Invalid date");
    }

    @Test
    public void testDateListElementWithTimeListAndListTimeFailsInEveryContext() throws Exception {
        // A static element with a time part of its own that starts with a bracket, under a time of
        // the list, fails the query as it compiles, wherever the query holds the literal, at the
        // position of the literal, with the error that a bind variable with the expression fails the
        // query with as it runs. The literal selected 10:00 on every day of January.
        assertMemoryLeak(() -> {
            createContextTables();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                final String tickExpr = "[2024-01T[09:00,10:00], $today]T10:00";
                final String error = "Invalid date: 2024-01-01T09:00T10:00";
                assertTickExprFailsInEveryContext(tickExpr, error, tickExpr, error);
                // NOT IN
                final String notIn = "SELECT count() FROM t WHERE ts NOT IN ";
                assertExceptionNoLeakCheck(notIn + "'" + tickExpr + "'", notIn.length(), error);
                bindVariableService.clear();
                bindVariableService.setStr(0, tickExpr);
                assertExceptionNoLeakCheck(notIn + "$1", notIn.length(), error);
                bindVariableService.clear();
                // a monotonic function of the designated timestamp, whose interval the scan inverts
                // as it opens: the element on a month, and the one on days, which the IR rejected
                for (String[] literal : new String[][]{
                        {tickExpr, error},
                        {"[2024-01-[01..03]T[09:00], $today]T10:00", "Invalid date: 2024-01-01T09:00T10:00"}
                }) {
                    final String sql = "SELECT count() FROM t WHERE to_timezone(ts, 'UTC') IN '" + literal[0] + "'";
                    assertExceptionNoLeakCheck(sql, sql.indexOf("'" + literal[0]), literal[1]);
                }
                // the table keeps answering: 10:00 on each day of January, of which it has four
                assertIntrinsicRows("ts IN '[2024-01, $today]T10:00'", "4\t2024-01-01T10:00:00.000000Z\t2024-01-04T10:00:00.000000Z");
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDateListElementWithTimeListAndListTimeFailsOnEveryTimestampType() throws Exception {
        // A static element with a time part of its own that starts with a bracket, under a time of
        // the list, fails the query on a designated nanosecond timestamp, a nullable timestamp, and a
        // DATE, which IN compares as a microsecond timestamp: as a literal as the query compiles, and
        // as a bind variable as it runs. A month element without a time part of its own selects the
        // time of the list on each of its days.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tn (ts TIMESTAMP_NS, ts2 TIMESTAMP, d DATE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tn VALUES
                    ('2024-01-02T10:00:00.000000000Z', '2024-01-02T10:00:00.000000Z', '2024-01-02T10:00:00.000Z'),
                    ('2024-01-05T10:00:30.000000000Z', '2024-01-05T10:00:30.000000Z', '2024-01-05T10:00:30.000Z'),
                    ('2024-01-05T11:00:00.000000000Z', NULL, NULL)
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                final String tickExpr = "[2024-01T[09:00,10:00], $today]T10:00";
                final String error = "Invalid date: 2024-01-01T09:00T10:00";
                // the minute of 10:00 holds the rows at 10:00 and at 10:00:30, and neither the row at
                // 11:00 nor the NULL
                for (String[] column : new String[][]{
                        {"ts", "2\t2024-01-02T10:00:00.000000000Z\t2024-01-05T10:00:30.000000000Z"},
                        {"ts2", "2\t2024-01-02T10:00:00.000000Z\t2024-01-05T10:00:30.000000Z"},
                        {"d", "2\t2024-01-02T10:00:00.000Z\t2024-01-05T10:00:30.000Z"}
                }) {
                    final String query = "SELECT count(), min(" + column[0] + "), max(" + column[0] + ") FROM tn WHERE " + column[0] + " IN ";
                    assertExceptionNoLeakCheck(query + "'" + tickExpr + "'", query.length(), error);
                    for (boolean isVarchar : new boolean[]{false, true}) {
                        setTickExprBindVariable(tickExpr, isVarchar);
                        assertExceptionNoLeakCheck(query + "$1", query.length(), error);
                    }
                    bindVariableService.clear();
                    assertQuery(query + "'[2024-01, $today]T10:00'")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\tmin\tmax\n" + column[1] + "\n");
                }
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
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
    public void testDateVariableRangeAddingNoIntervalWithMalformedElementSuffixFailsAsLiteral() throws Exception {
        // A date variable range that adds no interval when the query compiles, one that starts
        // after it ends or a range of business days that holds no business day, with a timezone or
        // day filter of its element that does not parse, fails a literal as the query compiles,
        // wherever the query holds it, at the position of the literal
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
                assertLiteralFailsWhenQueryCompiles("[$tomorrow..$today@Bogus/Zone]", "invalid timezone: Bogus/Zone");
                assertLiteralFailsWhenQueryCompiles("[$tomorrow..$today#Xyz]", "Invalid day name: Xyz");
                assertLiteralFailsWhenQueryCompiles("[$now..$now - 1h@Bogus/Zone]", "invalid timezone: Bogus/Zone");
                // a Saturday
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-06T10:00:00.000000Z"));
                assertLiteralFailsWhenQueryCompiles("[$today..$today + 0bd@Bogus/Zone]", "invalid timezone: Bogus/Zone");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDateVariableRangeAddingNoIntervalWithMalformedElementSuffixFailsInEveryContext() throws Exception {
        // A date variable range that adds no interval, with a timezone of its element that does not
        // parse, fails the query wherever the query holds it, at the position of the expression: a
        // literal as the query compiles, and a bind variable as it runs, which fails on the order
        // of a range that starts after it ends first. At 03:00 the range from 09:30 to "now" starts
        // after it ends, and on Saturday 2024-01-06 the range of business days of the day holds
        // none.
        assertMemoryLeak(() -> {
            createContextTables();
            try {
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
                assertTickExprFailsInEveryContext(
                        "[$today + 570m..$now@Bogus/Zone]",
                        "invalid timezone: Bogus/Zone",
                        "[$today + 570m..$now@Bogus/Zone]",
                        "Invalid date range: start is after end"
                );
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-06T10:00:00.000000Z"));
                assertTickExprFailsInEveryContext(
                        "[$today..$today + 0bd@Bogus/Zone]",
                        "invalid timezone: Bogus/Zone",
                        "[$today..$today + 0bd@Bogus/Zone]",
                        "invalid timezone: Bogus/Zone"
                );
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDateVariableRangeAddingNoIntervalWithMalformedElementSuffixFailsOnEveryTimestampType() throws Exception {
        // A date variable range that adds no interval, with a timezone of its element that does not
        // parse, fails the query on a designated nanosecond timestamp, a nullable timestamp, and a
        // DATE, which IN compares as a microsecond timestamp: as a literal as the query compiles,
        // and as a bind variable, in the interval scan and in the row filter, and as a value of a
        // row as it runs. At 03:00 the range from 09:30 to "now" starts after it ends, and on
        // Saturday 2024-01-06 the range of business days of the day holds none. With a timezone
        // that parses, the first range selects the rows from 09:30 to 12:00 when the query runs at
        // 12:00, the second none, and the range of business days from the Saturday to the next one
        // the rows of Monday 2024-01-08, NULL aside.
        assertMemoryLeak(() -> {
            final String noDay = "[$today..$today + 0bd@Bogus/Zone]";
            execute("CREATE TABLE tn (ts TIMESTAMP_NS, ts2 TIMESTAMP, d DATE, s STRING, vc VARCHAR) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tn VALUES "
                    + "('2024-01-02T09:30:00.000000000Z', '2024-01-02T09:30:00.000000Z', '2024-01-02T09:30:00.000Z', '" + noDay + "', '" + noDay + "'), "
                    + "('2024-01-08T09:30:00.000000000Z', '2024-01-08T09:30:00.000000Z', '2024-01-08T09:30:00.000Z', '" + noDay + "', '" + noDay + "'), "
                    + "('2024-01-08T11:00:00.000000000Z', NULL, NULL, '" + noDay + "', '" + noDay + "')");
            try {
                for (String[] column : new String[][]{
                        {"ts", "1\t2024-01-02T09:30:00.000000000Z\t2024-01-02T09:30:00.000000000Z", "2\t2024-01-08T09:30:00.000000000Z\t2024-01-08T11:00:00.000000000Z"},
                        {"ts2", "1\t2024-01-02T09:30:00.000000Z\t2024-01-02T09:30:00.000000Z", "1\t2024-01-08T09:30:00.000000Z\t2024-01-08T09:30:00.000000Z"},
                        {"d", "1\t2024-01-02T09:30:00.000Z\t2024-01-02T09:30:00.000Z", "1\t2024-01-08T09:30:00.000Z\t2024-01-08T09:30:00.000Z"}
                }) {
                    final String query = "SELECT count(), min(" + column[0] + "), max(" + column[0] + ") FROM tn WHERE " + column[0] + " IN ";
                    setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
                    final String reversed = query + "'[$today + 570m..$now@Bogus/Zone]'";
                    assertExceptionNoLeakCheck(reversed, reversed.indexOf('\''), "invalid timezone: Bogus/Zone");
                    try (RecordCursorFactory factory = select(query + "'[$today + 570m..$now@+00:00]'")) {
                        setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z"));
                        assertFactory(factory)
                                .withContext(sqlExecutionContext)
                                .noRandomAccess()
                                .expectSize()
                                .returns("count\tmin\tmax\n" + column[1] + "\n");
                    }

                    setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-06T10:00:00.000000Z"));
                    final String literal = query + "'" + noDay + "'";
                    assertExceptionNoLeakCheck(literal, literal.indexOf('\''), "invalid timezone: Bogus/Zone");
                    for (boolean isVarchar : new boolean[]{false, true}) {
                        setTickExprBindVariable(noDay, isVarchar);
                        assertExceptionNoLeakCheck(query + "$1", query.length(), "invalid timezone: Bogus/Zone");
                        assertExceptionNoLeakCheck(query + "$1 OR " + column[0] + " > '2030-01-01'", query.length(), "invalid timezone: Bogus/Zone");
                        setTickExprBindVariable("[$today..$today + 0bd@+00:00]", isVarchar);
                        assertQuery(query + "$1")
                                .noLeakCheck()
                                .noRandomAccess()
                                .expectSize()
                                .returns("count\tmin\tmax\n0\t\t\n");
                        setTickExprBindVariable("[$today..$today + 1bd@+00:00]", isVarchar);
                        assertQuery(query + "$1")
                                .noLeakCheck()
                                .noRandomAccess()
                                .expectSize()
                                .returns("count\tmin\tmax\n" + column[2] + "\n");
                    }
                    bindVariableService.clear();
                }
                for (String column : new String[]{"s", "vc"}) {
                    final String sql = "SELECT count() FROM tn WHERE ts IN " + column;
                    assertExceptionNoLeakCheck(sql, sql.lastIndexOf(' ') + 1, "invalid timezone: Bogus/Zone");
                }
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDateVariableRangeAddingNoIntervalWithMalformedElementSuffixFailsWhateverTheData() throws Exception {
        // A date variable range that adds no interval, with a timezone of its element that does not
        // parse, fails a literal as the query compiles, without reading data, on an empty table and
        // on one of a single row. A prepared query fails on the run that binds such an expression
        // on a Saturday, and keeps working on the runs before and after it, where the range of
        // business days from the Saturday to the next one selects Monday 2024-01-08.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE empty_table (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE single_row (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO single_row VALUES ('2024-01-08T09:30:00.000000Z')");
            // hourly rows from Saturday 2024-01-06 to Monday 2024-01-08
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-06T00:00:00.000000Z', 3_600_000_000L) ts
                        FROM long_sequence(72)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-06T10:00:00.000000Z"));
            try {
                for (String table : new String[]{"empty_table", "single_row"}) {
                    for (String tickExpr : new String[]{"[$today..$today + 0bd@Bogus/Zone]", "[$tomorrow..$today@Bogus/Zone]"}) {
                        final String sql = "SELECT count() FROM " + table + " WHERE ts IN '" + tickExpr + "'";
                        assertExceptionNoLeakCheck(sql, sql.indexOf('\''), "invalid timezone: Bogus/Zone");
                    }
                }
                final String monday = "count\tmin\tmax\n24\t2024-01-08T00:00:00.000000Z\t2024-01-08T23:00:00.000000Z\n";
                final String prepared = "SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1";
                bindVariableService.clear();
                bindVariableService.setStr(0, "[$today..$today + 1bd]");
                try (RecordCursorFactory factory = select(prepared)) {
                    assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns(monday);
                    bindVariableService.setStr(0, "[$today..$today + 0bd@Bogus/Zone]");
                    try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                        Assert.fail("the run with [$today..$today + 0bd@Bogus/Zone] should fail");
                    } catch (SqlException | CairoException e) {
                        TestUtils.assertEquals("invalid timezone: Bogus/Zone", e.getFlyweightMessage());
                        Assert.assertEquals(prepared.indexOf("$1"), e.getPosition());
                    }
                    bindVariableService.setStr(0, "[$today..$today + 1bd]");
                    assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns(monday);
                }
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDateVariableRangeElementDayFilterBindVariable() throws Exception {
        // the range's own day filter keeps the Mondays: 2024-01-01 and 2024-01-08
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
                .noRandomAccess()
                .expectSize()
                // hourly rows from 2024-01-01T00:00 to 2024-01-14T23:00
                .ddl("""
                        CREATE TABLE t AS (
                            SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts
                            FROM long_sequence(336)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """)
                .assertBinds(cases);
    }

    @Test
    public void testDateVariableRangeElementSuffixInRowValueWithParallelFilterOnAndOff() throws Exception {
        // A value of a row with a range and a timezone and day filter of its element selects what
        // the literal selects, whether the filter runs in parallel, on a copy of the function for
        // each worker, or on the thread of the query. On Sunday 2025-01-19 at 12:00,
        // [$now - 2h..$now#Sun] holds the rows from 10:00 to 12:00, [$now - 2h..$now#Mon] none, and
        // [$today..$tomorrow@UTC+05#Sun] the Sunday at UTC+05, from 2025-01-18T19:00Z on. The queries
        // run on a started worker pool: its workers evaluate the frames they take with their own
        // copies of the filter, and the thread of the query the frames it takes with its filter.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, context) -> {
                        // a row every 30 minutes from 2025-01-15T00:00 to 2025-01-27T23:30
                        engine.execute("""
                                CREATE TABLE t AS (
                                    SELECT timestamp_sequence('2025-01-15T00:00:00.000000Z', 1_800_000_000L) ts,
                                           '[$now - 2h..$now#Sun]'::STRING s1,
                                           '[$now - 2h..$now#Sun]'::VARCHAR vc1,
                                           '[$now - 2h..$now#Mon]'::STRING s2,
                                           '[$now - 2h..$now#Mon]'::VARCHAR vc2,
                                           '[$today..$tomorrow@UTC+05#Sun]'::STRING s3,
                                           '[$today..$tomorrow@UTC+05#Sun]'::VARCHAR vc3
                                    FROM long_sequence(624)
                                ) TIMESTAMP(ts) PARTITION BY DAY
                                """, context);
                        setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-19T12:00:00.000000Z"));
                        final boolean isParallelFilterEnabled = context.isParallelFilterEnabled();
                        try {
                            context.initNow();
                            for (boolean isParallel : new boolean[]{true, false}) {
                                context.setParallelFilterEnabled(isParallel);
                                for (String[] column : new String[][]{
                                        {"s1", "5\t2025-01-19T10:00:00.000000Z\t2025-01-19T12:00:00.000000Z"},
                                        {"vc1", "5\t2025-01-19T10:00:00.000000Z\t2025-01-19T12:00:00.000000Z"},
                                        {"s2", "0\t\t"},
                                        {"vc2", "0\t\t"},
                                        {"s3", "48\t2025-01-18T19:00:00.000000Z\t2025-01-19T18:30:00.000000Z"},
                                        {"vc3", "48\t2025-01-18T19:00:00.000000Z\t2025-01-19T18:30:00.000000Z"}
                                }) {
                                    assertQuery("SELECT ts FROM t WHERE ts IN " + column[0])
                                            .withEngine(engine)
                                            .withContext(context)
                                            .noLeakCheck()
                                            .assertsPlanContaining(isParallel ? "Async Filter workers: 4" : "Filter filter: ts in " + column[0]);
                                    assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN " + column[0])
                                            .withEngine(engine)
                                            .withContext(context)
                                            .noLeakCheck()
                                            .noRandomAccess()
                                            .expectSize()
                                            .withPlanContaining(isParallel ? "workers: 4" : "Filter filter: ts in " + column[0])
                                            .returns("count\tmin\tmax\n" + column[1] + "\n");
                                }
                            }
                        } finally {
                            context.setParallelFilterEnabled(isParallelFilterEnabled);
                            setCurrentMicros(-1);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testDateVariableRangeElementZoneBeforeDayFilterInEveryFilterPath() throws Exception {
        // The timezone of a date variable range element ends at the day filter of the element:
        // UTC+05 is an offset rather than UTC, and +05 parses, for both kinds of range, in every
        // path that a query takes. On Sunday 2025-01-19 at 12:00, 10:00 to 12:00 at UTC+05 is 05:00
        // to 07:00Z, and the Sunday at UTC+05 runs from 2025-01-18T19:00Z to 2025-01-19T19:00Z.
        assertMemoryLeak(() -> {
            createHalfHourTables();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-19T12:00:00.000000Z"));
            try {
                assertTickExprInEveryFilterPath("[$now - 2h..$now@UTC+05#Sun]", 5, "2025-01-19T05:00", "2025-01-19T07:00");
                assertTickExprInEveryFilterPath("[$today..$tomorrow@UTC+05#Sun]", 48, "2025-01-18T19:00", "2025-01-19T18:30");
                assertTickExprInEveryFilterPath("[$today..$tomorrow@+05#Sun]", 48, "2025-01-18T19:00", "2025-01-19T18:30");
            } finally {
                setCurrentMicros(-1);
            }
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
    public void testDateVariableRangeMalformedZoneWithSlotZoneFailsInEveryContext() throws Exception {
        // A slot of a time list with a timezone of its own converts the clip of its range of days,
        // which "now" clips, with the timezone of the range first, which does not parse here. The
        // query fails at the position of the expression wherever it holds it, a prepared query fails
        // on every run that binds the expression and keeps working on the runs after them, and no
        // failure keeps pooled objects of the parser.
        assertMemoryLeak(() -> {
            createContextTables();
            final String malformed = "[$now..$tomorrow@Bogus/Zone]T[09:00@UTC]";
            // London keeps UTC in January: the slots at 09:00 of the day of "now" and of the next day
            final String wellFormed = "[$now..$tomorrow@Europe/London]T[09:00@UTC]";
            try {
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
                final int inUseCount = IntervalUtils.getPooledObjectsInUseCountForTesting();
                assertTickExprFailsInEveryContext(malformed, "invalid timezone: Bogus/Zone", malformed, "invalid timezone: Bogus/Zone");
                Assert.assertEquals(inUseCount, IntervalUtils.getPooledObjectsInUseCountForTesting());

                final String expected = "count\tmin\tmax\n2\t2024-01-02T09:00:00.000000Z\t2024-01-03T09:00:00.000000Z\n";
                final String prepared = "SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1";
                bindVariableService.clear();
                bindVariableService.setStr(0, wellFormed);
                try (RecordCursorFactory factory = select(prepared)) {
                    assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns(expected);
                    bindVariableService.setStr(0, malformed);
                    for (int i = 0; i < 2; i++) {
                        try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                            Assert.fail("the run with " + malformed + " should fail");
                        } catch (SqlException | CairoException e) {
                            TestUtils.assertEquals("invalid timezone: Bogus/Zone", e.getFlyweightMessage());
                            Assert.assertEquals(prepared.indexOf("$1"), e.getPosition());
                        }
                    }
                    bindVariableService.setStr(0, wellFormed);
                    assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns(expected);
                }
                Assert.assertEquals(inUseCount, IntervalUtils.getPooledObjectsInUseCountForTesting());
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
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
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(
                "starts in the gap",
                "count\tmin\tmax\n5\t2024-03-31T01:00:00.000000Z\t2024-03-31T01:04:00.000000Z\n",
                bindVariableService -> bindVariableService.setStr(0, "2024-03-31T01:10@Europe/London;55m")
        ));
        assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1")
                .noRandomAccess()
                .expectSize()
                .ddl("""
                        CREATE TABLE t AS (
                            SELECT timestamp_sequence('2024-03-31T00:00:00.000000Z', 60_000_000L) ts
                            FROM long_sequence(180)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """)
                .assertBinds(cases);
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
    public void testInStrColumnParsesEachRow() throws Exception {
        // The tick string of each row gives the intervals that t must fall in. 2024-01-15 is a
        // Monday, so the day filter of row 5 drops the only day. A NULL string (row 7) and a NULL
        // timestamp (row 8) match nothing, so NOT IN holds for them. A malformed string (row 6)
        // fails the query at the column, as a malformed literal fails it, and the table keeps
        // answering.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k INT, t TIMESTAMP, s STRING, v VARCHAR)");
            execute("""
                    INSERT INTO x VALUES
                        (1, '2024-01-15T10:00:00.000000Z', '2024-01-15', '2024-01-15'),
                        (2, '2024-01-15T10:00:00.000000Z', '2024-01-16', '2024-01-16'),
                        (3, '2024-01-15T10:00:59.999999Z', '2024-01-[14,15]T10:00', '2024-01-[14,15]T10:00'),
                        (4, '2024-01-15T10:01:00.000000Z', '2024-01-[14,15]T10:00', '2024-01-[14,15]T10:00'),
                        (5, '2024-01-15T10:00:00.000000Z', '2024-01-15#tue', '2024-01-15#tue'),
                        (7, '2024-01-15T10:00:00.000000Z', NULL, NULL),
                        (8, NULL, '2024-01-15', '2024-01-15')
                    """);
            final String sql = "SELECT k, t IN s AS in_s, t NOT IN s AS not_in_s, t IN v AS in_v, t NOT IN v AS not_in_v FROM x";
            final String expected = """
                    k\tin_s\tnot_in_s\tin_v\tnot_in_v
                    1\ttrue\tfalse\ttrue\tfalse
                    2\tfalse\ttrue\tfalse\ttrue
                    3\ttrue\tfalse\ttrue\tfalse
                    4\tfalse\ttrue\tfalse\ttrue
                    5\tfalse\ttrue\tfalse\ttrue
                    7\tfalse\ttrue\tfalse\ttrue
                    8\tfalse\ttrue\tfalse\ttrue
                    """;
            assertQuery(sql).noLeakCheck().expectSize().returns(expected);
            execute("INSERT INTO x VALUES (6, '2024-01-15T10:00:00.000000Z', '2024-01-[15', '2024-01-[15')");
            for (String column : new String[]{"s", "v"}) {
                for (String in : new String[]{" IN ", " NOT IN "}) {
                    final String filter = "SELECT k FROM x WHERE t" + in + column;
                    assertExceptionNoLeakCheck(filter, filter.length() - 1, "Unclosed '[' in interval");
                }
            }
            // a NULL string in row 6, inserted last, matches nothing
            execute("UPDATE x SET s = NULL, v = NULL WHERE k = 6");
            assertQuery(sql).noLeakCheck().expectSize().returns(expected + "6\tfalse\ttrue\tfalse\ttrue\n");
        });
    }

    @Test
    public void testIntervalBindVariable() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testIntervalLimitAtStaleNowCompilesAndFailsWhenQueryRuns() throws Exception {
        // From "now" to 1024 days after today, 09:00 comes 1025 times at 03:00, one more than the
        // limit, and 1024 times at 12:00, after today's 09:00. SQL checks the limit as the query
        // compiles, against the clock, and again every time the query runs. A statement that
        // compiles at a stale "now", as a PostgreSQL Parse does, opens the interval scan of its
        // filter at that "now" for the JIT, and the limit does not fail it there.
        final int jitMode = sqlExecutionContext.getJitMode();
        assertMemoryLeak(() -> {
            createContextTables();
            final String sql = "SELECT count() FROM t WHERE ts IN '[$now..$today + 1024d]T09:00' AND s = 'a'";
            try {
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                // the context keeps 03:00, and the clock says 12:00
                setCurrentMicros(NOW_0300);
                currentMicros = NOW_1200;
                try (RecordCursorFactory factory = select(sql)) {
                    sqlExecutionContext.initNow();
                    assertPlanContains(factory, JitUtil.isJitSupported() ? "Async JIT Filter" : "Async Filter");
                    // 09:00 on 2024-01-03 and on 2024-01-04
                    assertFactory(factory)
                            .withContext(sqlExecutionContext)
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\n2\n");
                    setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-03T03:00:00.000000Z"));
                    assertRunFails(factory, sql, sql.indexOf('\''), "Date range expansion produces too many intervals (max 1024)");
                }
                // compiled with the clock at 03:00
                setCurrentMicros(NOW_0300);
                try (RecordCursorFactory ignore = select(sql)) {
                    Assert.fail("compiled: " + sql);
                } catch (SqlException e) {
                    TestUtils.assertEquals("Date range expansion produces too many intervals (max 1024)", e.getFlyweightMessage());
                    Assert.assertEquals(sql.indexOf('\''), e.getPosition());
                }
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testIntervalLiteralCompilesToOver255Elements() throws Exception {
        // The literal compiles to 481 elements: $today and 480 half-hourly intervals that do not
        // merge, more than the compiled expression could once count
        setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-03-01T12:00:00.000000Z"));
        assertQuery("SELECT count() FROM t WHERE ts IN '[$today,2024-01-[01..10]T[00..23]:[00,30]]'")
                .noRandomAccess()
                .expectSize()
                // rows every 30 minutes from 2024-01-01T00:00 to 2024-01-10T23:30
                .ddl("""
                        CREATE TABLE t AS (
                            SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 1_800_000_000L) ts
                            FROM long_sequence(480)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """)
                .returns("""
                        count
                        480
                        """);
    }

    @Test
    public void testInvalidDurationUnitFailsAsLiteralAndAsBindVariable() throws Exception {
        // the literal selected every row before $today
        assertMalformedTickExprFails("$today;1x", "Invalid duration unit: x");
    }

    @Test
    public void testInvertedRangeLiteralCompilesAndFailsWhenQueryRuns() throws Exception {
        // Whether a range starts after it ends depends on the time the query runs, so it fails then,
        // and not when the query compiles: a statement prepared at 03:00, when the range runs from
        // 09:30 to 03:00, runs at 12:00. With the JIT on, compiling a filter opens its interval scan
        // to read the symbol tables, which evaluates the range at 03:00: the filter keeps the JIT,
        // and the filter of a dateadd() of the designated timestamp keeps the Java filter, as the JIT
        // takes no date variable. The serial filter opens nothing as it compiles. The interval scan
        // evaluates the range at every open, also on a table without rows.
        final int jitMode = sqlExecutionContext.getJitMode();
        final boolean isParallelFilterEnabled = sqlExecutionContext.isParallelFilterEnabled();
        assertMemoryLeak(() -> {
            createContextTables();
            execute("CREATE TABLE e (ts TIMESTAMP, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    sqlExecutionContext.setJitMode(mode);
                    for (boolean isParallel : new boolean[]{true, false}) {
                        sqlExecutionContext.setParallelFilterEnabled(isParallel);
                        final String javaFilter = isParallel ? "Async Group By" : "Filter filter: ";
                        final String jitFilter = isParallel && mode == SqlJitMode.JIT_MODE_ENABLED && JitUtil.isJitSupported()
                                ? "Async JIT Group By"
                                : javaFilter;
                        // the condition, its rows at 12:00, and the plan of the query
                        for (String[] condition : new String[][]{
                                {"ts IN '$today + 570m..$now'", "3\t2024-01-02T10:00:00.000000Z\t2024-01-02T12:00:00.000000Z", "Interval forward scan"},
                                {"ts IN '$today + 570m..$now' OR ts = 0", "3\t2024-01-02T10:00:00.000000Z\t2024-01-02T12:00:00.000000Z", "Interval forward scan"},
                                // 08:30 to 11:00, an hour earlier
                                {"dateadd('h', 1, ts) IN '$today + 570m..$now'", "3\t2024-01-02T09:00:00.000000Z\t2024-01-02T11:00:00.000000Z", javaFilter},
                                // s is 'a' on odd hours
                                {"ts IN '$today + 570m..$now' AND s = 'a'", "1\t2024-01-02T11:00:00.000000Z\t2024-01-02T11:00:00.000000Z", jitFilter},
                                {"dateadd('h', 1, ts) IN '$today + 570m..$now' AND s = 'a'", "2\t2024-01-02T09:00:00.000000Z\t2024-01-02T11:00:00.000000Z", javaFilter}
                        }) {
                            final String sql = "SELECT count(), min(ts), max(ts) FROM t WHERE " + condition[0];
                            setCurrentMicros(NOW_0300);
                            try (RecordCursorFactory factory = select(sql)) {
                                assertRunFails(factory, sql, sql.indexOf("'$today"), RANGE_ORDER_ERROR);
                                setCurrentMicros(NOW_1200);
                                assertPlanContains(factory, condition[2]);
                                assertFactory(factory)
                                        .withContext(sqlExecutionContext)
                                        .noRandomAccess()
                                        .expectSize()
                                        .returns("count\tmin\tmax\n" + condition[1] + "\n");
                            }
                        }
                        final String emptyTableSql = "SELECT count() FROM e WHERE ts IN '$today + 570m..$now' AND s = 'a'";
                        setCurrentMicros(NOW_0300);
                        try (RecordCursorFactory factory = select(emptyTableSql)) {
                            assertRunFails(factory, emptyTableSql, emptyTableSql.indexOf('\''), RANGE_ORDER_ERROR);
                            setCurrentMicros(NOW_1200);
                            assertFactory(factory)
                                    .withContext(sqlExecutionContext)
                                    .noRandomAccess()
                                    .expectSize()
                                    .returns("count\n0\n");
                        }
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
                sqlExecutionContext.setParallelFilterEnabled(isParallelFilterEnabled);
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testIsoWeekElementWithBracketedTimeLiteralMatchesBindVariable() throws Exception {
        // The parser takes an ISO week with a time part of its own that starts with a bracket, under
        // a time of the list. A literal returns the rows of a bind variable with the expression, in
        // the interval scan and in the row filter.
        assertMemoryLeak(() -> {
            createHourlyTable();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                final String tickExpr = "[2024-W01T[09:00], $today]T10:00";
                bindVariableService.clear();
                bindVariableService.setStr(0, tickExpr);
                assertSqlCursors("SELECT * FROM t WHERE ts IN $1", "SELECT * FROM t WHERE ts IN '" + tickExpr + "'");
                assertSqlCursors(
                        "SELECT * FROM t WHERE ts IN $1 OR ts > '2030-01-01'",
                        "SELECT * FROM t WHERE ts IN '" + tickExpr + "' OR ts > '2030-01-01'"
                );
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testListOfTimestamps() throws Exception {
        assertQuery("test where ts in ('1970-01-01T00:00:00.070000Z', 77000, '1970-01-01'::date)")
                .ddl("create table test as (select rnd_int() a, timestamp_sequence(0, 1000) ts from long_sequence(100))")
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
    public void testLongTickDateRangeInEveryFilterPath() throws Exception {
        // Date ranges of 1,001 days, whose days merge into the intervals so far once there are 256 of
        // them: in order, and as 500 later days first, then the 501 days before them. The table has a
        // row at 09:30 and one at 10:30 of each day from 2022-04-01 to 2026-07-01, and "now" is
        // 2025-01-24T12:00Z: the ranges select 2022-04-30 to 2025-01-24, and 2023-09-12 to 2026-06-08.
        assertMemoryLeak(() -> {
            for (String timestampType : new String[]{"TIMESTAMP", "TIMESTAMP_NS"}) {
                execute("DROP TABLE IF EXISTS t");
                createLongRangeTable(timestampType);
                final String fraction = "TIMESTAMP_NS".equals(timestampType) ? "000000000" : "000000";
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-24T12:00:00.000000Z"));
                try {
                    assertLongTickDateRangeRows("[$today-1000d..$today]T09:00;1h", "2022-04-30", "2025-01-24", fraction);
                    assertLongTickDateRangeRows("[$today+1d..$today+500d, $today-500d..$today]T09:00;1h", "2023-09-12", "2026-06-08", fraction);
                } finally {
                    setCurrentMicros(-1);
                }
            }
        });
    }

    @Test
    public void testLongTickDateRangeInParallelFilter() throws Exception {
        // The row filter of each worker evaluates a copy of the expression of its own, whose date
        // ranges merge their days into the intervals so far. Every worker must select the rows that
        // the filter of the query thread selects. The query assertions share the leak check of the
        // wrapper, which spans the engine and the worker pool.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, context) -> {
                        engine.execute(longRangeTableDdl("TIMESTAMP"), context);
                        setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-24T12:00:00.000000Z"));
                        final boolean isParallelFilterEnabled = context.isParallelFilterEnabled();
                        try {
                            context.initNow();
                            for (String[] range : new String[][]{
                                    {"[$today-1000d..$today]T09:00;1h", "2022-04-30", "2025-01-24"},
                                    {"[$today+1d..$today+500d, $today-500d..$today]T09:00;1h", "2023-09-12", "2026-06-08"}
                            }) {
                                final String expected = "count\tmin\tmax\n1001\t" + range[1] + "T09:30:00.000000Z\t" + range[2] + "T09:30:00.000000Z\n";
                                for (boolean isParallel : new boolean[]{true, false}) {
                                    context.setParallelFilterEnabled(isParallel);
                                    assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN '" + range[0] + "' OR v = -1")
                                            .withEngine(engine)
                                            .withContext(context)
                                            .noLeakCheck()
                                            .noRandomAccess()
                                            .expectSize()
                                            .withPlanContaining(isParallel ? "workers: 4" : "Filter filter: (ts in [")
                                            .returns(expected);
                                }
                            }
                        } finally {
                            context.setParallelFilterEnabled(isParallelFilterEnabled);
                            setCurrentMicros(-1);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testLongTickDateRangeMergingEveryTwoIntervals() throws Exception {
        // With a merge after every 2 intervals, the days of 3 later days, then of the 4 days before
        // them, merge into the intervals so far. EXPLAIN shows the 7 intervals, a factory that the
        // query holds follows the day it runs on, and a bind variable selects the same rows.
        setProperty(PropertyKey.CAIRO_SQL_INTERVAL_INCREMENTAL_MERGE_THRESHOLD, 2);
        setProperty(PropertyKey.CAIRO_SQL_INTERVAL_MAX_INTERVALS_AFTER_MERGE, 16);
        final String tickExpr = "[$today+1d..$today+3d, $today-3d..$today]T09:00;1h";
        final String query = "SELECT count(), min(ts), max(ts) FROM t WHERE ts IN '" + tickExpr + "'";
        assertMemoryLeak(() -> {
            createLongRangeTable("TIMESTAMP");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-24T12:00:00.000000Z"));
            try {
                final StringBuilder intervals = new StringBuilder("intervals: [");
                for (int day = 21; day <= 27; day++) {
                    intervals.append(day > 21 ? "," : "")
                            .append("(\"2025-01-").append(day).append("T09:00:00.000000Z\",\"2025-01-").append(day).append("T09:59:59.999999Z\")");
                }
                assertQuery(query).noLeakCheck().assertsPlanContaining(intervals.append(']'));

                // the same factory runs a day later, after a row outside the ranges arrives each time
                final ObjList<MutationStep> steps = new ObjList<>();
                steps.add(MutationStep.of(
                        "INSERT INTO t VALUES ('2026-12-01T09:30:00.000000Z', -2)",
                        "count\tmin\tmax\n7\t2025-01-21T09:30:00.000000Z\t2025-01-27T09:30:00.000000Z\n"
                ));
                steps.add(MutationStep.of(
                        () -> setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-25T12:00:00.000000Z")),
                        "INSERT INTO t VALUES ('2026-12-02T09:30:00.000000Z', -3)",
                        "count\tmin\tmax\n7\t2025-01-22T09:30:00.000000Z\t2025-01-28T09:30:00.000000Z\n"
                ));
                assertQuery(query)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .mutateStepwise(steps);

                bindVariableService.clear();
                bindVariableService.setStr(0, tickExpr);
                assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\tmin\tmax\n7\t2025-01-22T09:30:00.000000Z\t2025-01-28T09:30:00.000000Z\n");
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testLongTickDateRangeOverIntervalLimitFailsAsQueryCompilesAndRuns() throws Exception {
        // 10 later days, then the 7 days before them up to "now", with a merge after every 8
        // intervals and a limit of 16. At 08:00 the range holds 16 intervals, as 09:00 today is still
        // to come; at 12:00 it holds 17: a query that compiled at 08:00 fails when it runs at 12:00,
        // and one that compiles at 12:00 fails as it compiles.
        setProperty(PropertyKey.CAIRO_SQL_INTERVAL_INCREMENTAL_MERGE_THRESHOLD, 8);
        setProperty(PropertyKey.CAIRO_SQL_INTERVAL_MAX_INTERVALS_AFTER_MERGE, 16);
        final String sql = "SELECT count(), min(ts), max(ts) FROM t WHERE ts IN '[$today+1d..$today+10d, $today-6d..$now]T09:00;1h'";
        final String error = "Date range expansion produces too many intervals (max 16)";
        assertMemoryLeak(() -> {
            createLongRangeTable("TIMESTAMP");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-24T08:00:00.000000Z"));
            try {
                try (RecordCursorFactory factory = select(sql)) {
                    assertFactory(factory)
                            .withContext(sqlExecutionContext)
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\tmin\tmax\n16\t2025-01-18T09:30:00.000000Z\t2025-02-03T09:30:00.000000Z\n");
                    setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-24T12:00:00.000000Z"));
                    assertRunFails(factory, sql, sql.indexOf('\''), error);
                }
                assertExceptionNoLeakCheck(sql, sql.indexOf('\''), error);
                // the table keeps answering
                assertQuery("SELECT count() FROM t")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n3106\n");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testMalformedLiteralFailsWhenQueryCompiles() throws Exception {
        // SQL parses a literal that the compiled form cannot represent every time the query runs, and
        // once as the query compiles, so a malformed one fails before a prepared statement or a WAL
        // UPDATE is acknowledged, as it did before such literals were parsed at every execution
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                assertLiteralFailsWhenQueryCompiles("[$today,garbage]T[09:00,10:00]", "Invalid date: garbage");
                assertLiteralFailsWhenQueryCompiles("$todayT[09,14]:[00,99]", "Invalid date: 2024-01-02T09:99");
                assertLiteralFailsWhenQueryCompiles("[$today]T[09,14]:[00,99]", "Invalid date: 2024-01-02T09:99");
            } finally {
                setCurrentMicros(-1);
            }
        });
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
    public void testOffsetTimestampTickExprLiteral() throws Exception {
        // The optimiser pushes a static tick expression on a dateadd() of the designated timestamp
        // below the dateadd() as an offset of the interval scan, and keeps one with a date variable
        // in the row filter, which SQL validates as it compiles. Both select the same rows.
        assertMemoryLeak(() -> {
            createHourlyTable();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                // the rows from 01:00 on 2024-01-02 to midnight, an hour later
                final String expected = "count\tmin\tmax\n24\t2024-01-02T00:00:00.000000Z\t2024-01-02T23:00:00.000000Z\n";
                for (String tickExpr : new String[]{"2024-01-02", "$today"}) {
                    assertQuery("SELECT count(), min(tt), max(tt) FROM (SELECT dateadd('h', -1, ts) tt FROM t) WHERE tt IN '" + tickExpr + "'")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns(expected);
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testOverIntervalLimitLiteralFailsWhenQueryCompiles() throws Exception {
        // SQL checks the interval limit once as the query compiles, against the time it compiles at,
        // as a WAL apply does not, and again every time the query runs
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                // the compiled form
                assertLiteralFailsWhenQueryCompiles("[$today..$today + 2000d]T09:00", "Date range expansion produces too many intervals (max 1024)");
                // the form that SQL parses on every evaluation
                assertLiteralFailsWhenQueryCompiles("[$today..$today + 600d, 2024-01-01]T[09:00,10:00]", "produces too many intervals (max 1024)");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeRangeElementDayFilterFollowsNowOfEachRun() throws Exception {
        // A query that holds a range between two points in time with a day filter of its element
        // applies the day filter on the day of each run: compiled on Sunday 2025-01-19, the query
        // with #Mon selects no row, then the rows of the range when it runs on Monday, as a literal
        // and with a bind variable, which the query binds anew to #Sun after that
        assertMemoryLeak(() -> {
            createHalfHourTables();
            final String none = "count\tmin\tmax\n0\t\t\n";
            final String monday = "count\tmin\tmax\n5\t2025-01-20T10:00:00.000000Z\t2025-01-20T12:00:00.000000Z\n";
            try {
                for (String sql : new String[]{
                        "SELECT count(), min(ts), max(ts) FROM t WHERE ts IN '[$now - 2h..$now#Mon]'",
                        "SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1",
                        "SELECT count(), min(ts), max(ts) FROM t WHERE ts2 IN $1"
                }) {
                    setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-19T12:00:00.000000Z"));
                    setTickExprBindVariable("[$now - 2h..$now#Mon]", false);
                    try (RecordCursorFactory factory = select(sql)) {
                        assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns(none);
                        setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-20T12:00:00.000000Z"));
                        assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns(monday);
                        if (sql.contains("$1")) {
                            setTickExprBindVariable("[$now - 2h..$now#Sun]", true);
                            assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns(none);
                        }
                    }
                }
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeRangeElementDayFilterInEveryContext() throws Exception {
        // A range between two points in time with a day filter of its element selects, wherever a
        // query holds it, the rows of the same query over the range when the range starts on the
        // day of the filter, and no row otherwise, as a literal and as a bind variable of STRING
        // and VARCHAR type. day_of_week() counts from Monday as 1. On Sunday 2025-01-19 at 12:00,
        // [$now - 2h..$now#Sun] holds the rows from 10:00 to 12:00, and [$now - 2h..$now#Mon] none.
        assertMemoryLeak(() -> {
            // a row every 30 minutes from 2025-01-15T00:00 to 2025-01-27T23:30, with a symbol,
            // without an index in t and with one in ti
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2025-01-15T00:00:00.000000Z', 1_800_000_000L) ts,
                               (CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END)::SYMBOL s
                        FROM long_sequence(624)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("CREATE TABLE ti (ts TIMESTAMP, s SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO ti SELECT ts, s FROM t");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-19T12:00:00.000000Z"));
            final String sundayReference = "BETWEEN dateadd('h', -2, now()) AND now() AND day_of_week(dateadd('h', -2, now())) = 7";
            try {
                assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts " + sundayReference)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\tmin\tmax\n5\t2025-01-19T10:00:00.000000Z\t2025-01-19T12:00:00.000000Z\n");
                for (String[] dayFilter : new String[][]{{"#Sun", "7"}, {"#Mon", "1"}}) {
                    final String tickExpr = "[$now - 2h..$now" + dayFilter[0] + "]";
                    final String reference = "BETWEEN dateadd('h', -2, now()) AND now() AND day_of_week(dateadd('h', -2, now())) = " + dayFilter[1];
                    // ? stands for the tick expression
                    for (String query : new String[]{
                            "WITH c AS (SELECT * FROM t WHERE ts IN ?) SELECT count() FROM c",
                            "SELECT count() FROM (SELECT * FROM t WHERE ts IN ?)",
                            "SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a JOIN t b ON a.s = b.s",
                            "SELECT count() FROM t a JOIN (SELECT * FROM t WHERE ts IN ?) b ON a.s = b.s",
                            "SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a LEFT JOIN t b ON a.s = b.s",
                            "SELECT count() FROM t a LEFT JOIN (SELECT * FROM t WHERE ts IN ?) b ON a.s = b.s",
                            "SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a ASOF JOIN t b",
                            "SELECT count() FROM t a ASOF JOIN (SELECT * FROM t WHERE ts IN ?) b",
                            "SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a CROSS JOIN t b",
                            "SELECT count() FROM t a CROSS JOIN (SELECT * FROM t WHERE ts IN ?) b",
                            "SELECT ts FROM t WHERE ts IN ? UNION SELECT ts FROM t WHERE ts < '2025-01-16'",
                            "SELECT ts FROM t WHERE ts < '2025-01-16' UNION SELECT ts FROM t WHERE ts IN ?",
                            "SELECT ts FROM t WHERE ts IN ? EXCEPT SELECT ts FROM t WHERE ts < '2025-01-19T11:00'",
                            "SELECT ts FROM t WHERE ts < '2025-01-19T11:00' EXCEPT SELECT ts FROM t WHERE ts IN ?",
                            "SELECT ts FROM t WHERE ts IN ? INTERSECT SELECT ts FROM t",
                            "SELECT ts FROM t INTERSECT SELECT ts FROM t WHERE ts IN ?",
                            "SELECT s, count() FROM t WHERE ts IN ? GROUP BY s ORDER BY s",
                            "SELECT * FROM t WHERE ts IN ? LATEST ON ts PARTITION BY s ORDER BY s",
                            "SELECT ts, count() FROM t WHERE ts IN ? SAMPLE BY 1h",
                            "SELECT ts, row_number() OVER (PARTITION BY s ORDER BY ts) FROM t WHERE ts IN ?",
                            "SELECT * FROM t WHERE ts IN ? ORDER BY ts LIMIT -1",
                            // a symbol without an index, and one with an index
                            "SELECT count() FROM t WHERE s = 'a' AND ts IN ?",
                            "SELECT count() FROM ti WHERE s = 'a' AND ts IN ?"
                    }) {
                        final String referenceSql = query.replace("IN ?", reference);
                        // the literal, and a bind variable of STRING and of VARCHAR type
                        for (int holder = 0; holder < 3; holder++) {
                            final String sql;
                            if (holder == 0) {
                                bindVariableService.clear();
                                sql = query.replace("?", "'" + tickExpr + "'");
                            } else {
                                setTickExprBindVariable(tickExpr, holder == 2);
                                sql = query.replace("?", "$1");
                            }
                            try (
                                    RecordCursorFactory factory = select(sql);
                                    RecordCursorFactory referenceFactory = select(referenceSql)
                            ) {
                                assertSameRows(referenceFactory, factory, sql);
                            }
                        }
                    }
                }
                bindVariableService.clear();
                // EXPLAIN shows the intervals of the range that the day filter keeps, in the row
                // filter, where 1737280800000000 is 2025-01-19T10:00Z, 20,107 days and 10 hours
                // after 1970 in microseconds, and in the interval scan
                assertQuery("SELECT * FROM t WHERE ts IN '[$now - 2h..$now#Mon]' OR s = 'z'")
                        .noLeakCheck()
                        .assertsPlanContaining("filter: (ts in [] or s='z')");
                assertQuery("SELECT * FROM t WHERE ts IN '[$now - 2h..$now#Sun]' OR s = 'z'")
                        .noLeakCheck()
                        .assertsPlanContaining("filter: (ts in [1737280800000000,1737288000000000] or s='z')");
                assertQuery("SELECT * FROM t WHERE ts IN '[$now - 2h..$now#Sun]'")
                        .noLeakCheck()
                        .assertsPlanContaining("intervals: [(\"2025-01-19T10:00:00.000000Z\",\"2025-01-19T12:00:00.000000Z\")]");
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeRangeElementDayFilterInEveryFilterPath() throws Exception {
        // A range between two points in time with a day filter of its element selects the rows of
        // the range where the range starts on a day of the filter, and no row otherwise, in every
        // path that a query takes, and NOT IN the other rows. 2025-01-19 is a Sunday.
        // https://github.com/questdb/questdb/issues/8225
        assertMemoryLeak(() -> {
            createHalfHourTables();
            try {
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-19T12:00:00.000000Z"));
                assertTickExprInEveryFilterPath("[$now - 2h..$now#Sun]", 5, "2025-01-19T10:00", "2025-01-19T12:00");
                assertTickExprInEveryFilterPath("[$now - 2h..$now#Mon]", 0, null, null);
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-20T12:00:00.000000Z"));
                assertTickExprInEveryFilterPath("[$now - 2h..$now#Mon]", 5, "2025-01-20T10:00", "2025-01-20T12:00");
                assertTickExprInEveryFilterPath("[$now - 2h..$now#Sun]", 0, null, null);
                // a range across the partitions of the Sunday and the Monday, which starts on the
                // Sunday
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-20T01:00:00.000000Z"));
                assertTickExprInEveryFilterPath("[$now - 2h..$now#Sun]", 5, "2025-01-19T23:00", "2025-01-20T01:00");
                assertTickExprInEveryFilterPath("[$now - 2h..$now#Mon]", 0, null, null);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeRangeElementDayFilterKeepsBothEnds() throws Exception {
        // A range between two points in time with a day filter of its element that holds its first
        // day keeps both of its ends: on Sunday 2025-01-19 at 12:00, [$now - 2h..$now#Sun] holds
        // 10:00 and 12:00, not the microsecond before 10:00 or the one after 12:00, and
        // [$now - 2h..$now#Mon] none of them. An empty table has no row in either.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE empty_table (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('2025-01-19T09:59:59.999999Z'),
                    ('2025-01-19T10:00:00.000000Z'),
                    ('2025-01-19T12:00:00.000000Z'),
                    ('2025-01-19T12:00:00.000001Z')
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-19T12:00:00.000000Z"));
            try {
                for (String[] dayFilter : new String[][]{{"#Sun", "2\t2025-01-19T10:00:00.000000Z\t2025-01-19T12:00:00.000000Z"}, {"#Mon", "0\t\t"}}) {
                    final String tickExpr = "'[$now - 2h..$now" + dayFilter[0] + "]'";
                    assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN " + tickExpr)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\tmin\tmax\n" + dayFilter[1] + "\n");
                    assertQuery("SELECT count(), min(ts), max(ts) FROM empty_table WHERE ts IN " + tickExpr)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\tmin\tmax\n0\t\t\n");
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeRangeElementDayFilterUnderTimestampFunction() throws Exception {
        // A range between two points in time with a day filter of its element, below a function
        // of the designated timestamp that the interval scan inverts: on Sunday 2025-01-19 at
        // 12:00, the range from 09:00 to 11:00 holds the rows from 08:00 to 10:00 an hour before,
        // with #Sun and none with #Mon
        assertMemoryLeak(() -> {
            createHalfHourTables();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-19T12:00:00.000000Z"));
            try {
                for (String[] dayFilter : new String[][]{{"#Sun", "5\t2025-01-19T08:00:00.000000Z\t2025-01-19T10:00:00.000000Z"}, {"#Mon", "0\t\t"}}) {
                    final String tickExpr = "[$now - 3h..$now - 1h" + dayFilter[0] + "]";
                    assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE dateadd('h', 1, ts) IN '" + tickExpr + "'")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\tmin\tmax\n" + dayFilter[1] + "\n");
                    setTickExprBindVariable(tickExpr, false);
                    assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE dateadd('h', 1, ts) IN $1")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\tmin\tmax\n" + dayFilter[1] + "\n");
                }
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeRangeElementTickCalendarInEveryFilterPath() throws Exception {
        // A range between two points in time with a tick calendar of its element selects the rows
        // of the range in the sessions, which the duration of the list extends, in every path that
        // a query takes, and NOT IN the other rows. On Friday 2025-01-24, XNYS trades from 14:30 to
        // 21:00Z, and 13:30 to 15:30 at UTC-05 is 18:30 to 20:30Z.
        factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull TickCalendarServiceFactory getTickCalendarServiceFactory() {
                return () -> TestTickCalendarService.INSTANCE;
            }
        };
        assertMemoryLeak(() -> {
            createHalfHourTables();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-24T15:30:00.000000Z"));
            try {
                assertTickExprInEveryFilterPath("[$now - 2h..$now#XNYS]", 3, "2025-01-24T14:30", "2025-01-24T15:30");
                assertTickExprInEveryFilterPath("[$now - 2h..$now#XNYS];1h", 5, "2025-01-24T14:30", "2025-01-24T16:30");
                assertTickExprInEveryFilterPath("[$now - 2h..$now@UTC-05#XNYS]", 5, "2025-01-24T18:30", "2025-01-24T20:30");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeRangeElementUnknownDayFilterFailsInEveryContext() throws Exception {
        // A day filter of the element of a range between two points in time that does not parse
        // fails the query wherever the query holds the range, with or without a timezone of the
        // element before it: a literal as the query compiles, and a bind variable as it runs
        assertMemoryLeak(() -> {
            createContextTables();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                for (String tickExpr : new String[]{"[$now - 2h..$now#Xyz]", "[$now - 2h..$now@UTC+05#Xyz]"}) {
                    assertTickExprFailsInEveryContext(tickExpr, "Invalid day name: Xyz", tickExpr, "Invalid day name: Xyz");
                }
                // the table keeps answering, and 2024-01-02 is a Tuesday
                assertIntrinsicRows("ts IN '[$now - 2h..$now#Tue]'", "3\t2024-01-02T08:00:00.000000Z\t2024-01-02T10:00:00.000000Z");
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeRangeWithMalformedDurationFailsAsLiteralAndAsBindVariable() throws Exception {
        // a range between points in time ignores the duration, which still has to be well-formed,
        // and the day filter goes before it: the bind variable selected the range
        assertMalformedTickExprFails("$now - 1h..$now;1h#Tue", "Expected number before unit '#'");
    }

    @Test
    public void testPointInTimeRangeWithMalformedDurationFailsInEveryContext() throws Exception {
        // A range between two points in time ignores its duration, but a duration that does not
        // parse fails the query as it compiles, wherever the query holds the literal, at the
        // position of the literal. A bind variable with such an expression fails the query the
        // same way when it runs.
        assertMemoryLeak(() -> {
            createContextTables();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                assertTickExprFailsInEveryContext(
                        "$now - 1h..$now;1x",
                        "Invalid duration unit: x",
                        "$now - 1h..$now;1h#Tue",
                        "Expected number before unit '#'"
                );
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeRangeWithMalformedDurationFailsOnEveryTimestampType() throws Exception {
        // A range between two points in time with a duration that does not parse fails the query on
        // a designated nanosecond timestamp, a nullable timestamp, and a DATE, which IN compares as a
        // microsecond timestamp: as a literal when the query compiles, and as a bind variable and a
        // value of a row when it runs. The bind variable and the value of a row selected the rows of
        // the range.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tn (ts TIMESTAMP_NS, ts2 TIMESTAMP, d DATE, s STRING, vc VARCHAR) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tn VALUES
                    ('2024-01-02T09:30:00.000000000Z', '2024-01-02T09:30:00.000000Z', '2024-01-02T09:30:00.000Z', '$now - 1h..$now;1h#Tue', '$now - 1h..$now;1h#Tue'),
                    ('2024-01-02T10:30:00.000000000Z', '2024-01-02T10:30:00.000000Z', '2024-01-02T10:30:00.000Z', '$now - 1h..$now;1h#Tue', '$now - 1h..$now;1h#Tue'),
                    ('2024-01-02T11:00:00.000000000Z', NULL, NULL, '$now - 1h..$now;1h#Tue', '$now - 1h..$now;1h#Tue')
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                // 09:00 to 10:00 holds the row at 09:30, and not the NULL
                for (String[] column : new String[][]{
                        {"ts", "1\t2024-01-02T09:30:00.000000000Z\t2024-01-02T09:30:00.000000000Z"},
                        {"ts2", "1\t2024-01-02T09:30:00.000000Z\t2024-01-02T09:30:00.000000Z"},
                        {"d", "1\t2024-01-02T09:30:00.000Z\t2024-01-02T09:30:00.000Z"}
                }) {
                    final String query = "SELECT count(), min(" + column[0] + "), max(" + column[0] + ") FROM tn WHERE " + column[0] + " IN ";
                    final String literal = query + "'$now - 1h..$now;1x'";
                    assertExceptionNoLeakCheck(literal, literal.indexOf('\''), "Invalid duration unit: x");
                    assertQuery(query + "'$now - 1h..$now;1h'")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\tmin\tmax\n" + column[1] + "\n");
                    for (boolean isVarchar : new boolean[]{false, true}) {
                        setTickExprBindVariable("$now - 1h..$now;1h#Tue", isVarchar);
                        assertExceptionNoLeakCheck(query + "$1", query.length(), "Expected number before unit '#'");
                        setTickExprBindVariable("$now - 1h..$now;1h", isVarchar);
                        assertQuery(query + "$1")
                                .noLeakCheck()
                                .noRandomAccess()
                                .expectSize()
                                .returns("count\tmin\tmax\n" + column[1] + "\n");
                    }
                    bindVariableService.clear();
                }
                for (String column : new String[]{"s", "vc"}) {
                    final String sql = "SELECT count() FROM tn WHERE ts IN " + column;
                    assertExceptionNoLeakCheck(sql, sql.lastIndexOf(' ') + 1, "Expected number before unit '#'");
                }
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeRangeWithMalformedDurationFailsWhateverTheData() throws Exception {
        // A range between two points in time with a duration that does not parse fails a literal as
        // the query compiles, without reading data, on an empty table and on one of a single row. A
        // prepared query fails on the run that binds such an expression, and keeps working on the
        // runs before and after it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE empty_table (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE single_row (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO single_row VALUES ('2024-01-02T09:30:00.000000Z')");
            createHourlyTable();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                for (String table : new String[]{"empty_table", "single_row"}) {
                    final String sql = "SELECT count() FROM " + table + " WHERE ts IN '$now - 1h..$now;1x'";
                    assertExceptionNoLeakCheck(sql, sql.indexOf('\''), "Invalid duration unit: x");
                }
                // 09:00 to 10:00
                final String range = "2\t2024-01-02T09:00:00.000000Z\t2024-01-02T10:00:00.000000Z";
                final String prepared = "SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1";
                bindVariableService.clear();
                bindVariableService.setStr(0, "$now - 1h..$now;1h");
                try (RecordCursorFactory factory = select(prepared)) {
                    assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns("count\tmin\tmax\n" + range + "\n");
                    bindVariableService.setStr(0, "$now - 1h..$now;1h#Tue");
                    try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                        Assert.fail("the run with $now - 1h..$now;1h#Tue should fail");
                    } catch (SqlException | CairoException e) {
                        TestUtils.assertEquals("Expected number before unit '#'", e.getFlyweightMessage());
                        Assert.assertEquals(prepared.indexOf("$1"), e.getPosition());
                    }
                    bindVariableService.setStr(0, "$now - 1h..$now;1h");
                    assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns("count\tmin\tmax\n" + range + "\n");
                }
                bindVariableService.clear();
                assertIntrinsicRows("ts IN '$now - 1h..$now;1h'", range);
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeRangeWithMalformedDurationFailsWhenQueryCompiles() throws Exception {
        // A range between two points in time ignores its duration, but a duration that does not
        // parse fails a literal as the query compiles, whether the IR represents the literal or SQL
        // parses it on every evaluation, and a bind variable as the query runs, with the error of
        // the parser. The literal on a dateadd() of the designated timestamp, and the bind variable,
        // selected the rows of the range. The repeating interval of the parser, a duration, a period
        // and a count, repeats nothing after the range, and the table keeps answering.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts,
                               timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts2
                        FROM long_sequence(96)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            final int jitMode = sqlExecutionContext.getJitMode();
            try {
                // 09:00 to 10:00
                final String range = "2\t2024-01-02T09:00:00.000000Z\t2024-01-02T10:00:00.000000Z";
                for (String[] tickExpr : new String[][]{
                        {"$now - 1h..$now;1h#Tue", "Expected number before unit '#'"},
                        {"$now - 1h..$now;1x", "Invalid duration unit: x"},
                        {"$NOW - 1h..$NOW;1H", "Invalid duration unit: H"},
                        // SQL parses these on every evaluation
                        {"[$now - 1h..$now@+00:00];1h#Tue", "Expected number before unit '#'"},
                        {"[$now - 1h..$now]T[09..10]:30;1h-", "Missing unit at end of duration"},
                        {"[$now - 1h..$now@+00:00];1h;1d", "Invalid interval format: ;1h;1d"}
                }) {
                    for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                        sqlExecutionContext.setJitMode(mode);
                        // the interval scan, NOT IN, an OR that the scan takes, another timestamp
                        // column, and a dateadd() of the designated timestamp, above it and below it
                        assertLiteralFailsWhenQueryCompiles(tickExpr[0], tickExpr[1]);
                        // an OR that leaves the IN in the row filter
                        final String sql = "SELECT count() FROM t WHERE ts IN '" + tickExpr[0] + "' OR ts > '2030-01-01'";
                        try (RecordCursorFactory ignore = select(sql)) {
                            Assert.fail("compiled: " + sql);
                        } catch (SqlException e) {
                            TestUtils.assertEquals(sql, tickExpr[1], e.getFlyweightMessage());
                            Assert.assertEquals(sql, sql.indexOf('\''), e.getPosition());
                        }
                    }
                    sqlExecutionContext.setJitMode(jitMode);
                    assertIntrinsicRows("ts IN '$now - 1h..$now;1h'", range);
                }
                // the IR rejects one ';' more with an error of its own
                assertLiteralFailsWhenQueryCompiles("$now - 1h..$now;1h;1d", "Expected number before unit ';'");
                for (String[] tickExpr : new String[][]{
                        {"$now - 1h..$now;1h#Tue", "Expected number before unit '#'"},
                        {"$now - 1h..$now;1x", "Invalid duration unit: x"},
                        {"$NOW - 1h..$NOW;1H", "Invalid duration unit: H"},
                        {"[$now - 1h..$now]T[09..10]:30;1h-", "Missing unit at end of duration"},
                        {"$now - 1h..$now;1h;1d", "Invalid interval format: ;1h;1d"}
                }) {
                    for (boolean isVarchar : new boolean[]{false, true}) {
                        setTickExprBindVariable(tickExpr[0], isVarchar);
                        for (String sql : new String[]{
                                "SELECT count() FROM t WHERE ts IN $1",
                                "SELECT count() FROM t WHERE ts IN $1 OR ts > '2030-01-01'",
                                "SELECT count() FROM t WHERE ts2 IN $1",
                                "SELECT count() FROM t WHERE dateadd('h', 1, ts) IN $1"
                        }) {
                            assertExceptionNoLeakCheck(sql, sql.indexOf("$1"), tickExpr[1]);
                        }
                    }
                    bindVariableService.clear();
                }
                // the IR rejects the repeating interval, and the form that parses on every
                // evaluation takes it
                assertTickExprBindVariableMatches("$now - 1h..$now;1h;1d;3", range);
                assertTickExprMatches("[$now - 1h..$now@+00:00];1h;1d;3", range);
                assertIntrinsicRows("ts IN '$now - 1h..$now;1h'", range);
            } finally {
                bindVariableService.clear();
                sqlExecutionContext.setJitMode(jitMode);
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeRangeWithMalformedDurationInRowFailsWithParallelFilterOnAndOff() throws Exception {
        // A value of a row, a range between two points in time with a duration that does not parse,
        // fails the query at the column, whether the filter runs in parallel, on a copy of the
        // function for each worker, or on the thread of the query. The row is in the last of four
        // partitions, and the table keeps answering after the query fails.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts,
                               (CASE WHEN x = 80 THEN '$now - 1h..$now;1h#Tue' ELSE '$now - 1h..$now;1h' END) s,
                               (CASE WHEN x = 80 THEN '$now - 1h..$now;1h#Tue' ELSE '$now - 1h..$now;1h' END)::VARCHAR vc
                        FROM long_sequence(96)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            final boolean isParallelFilterEnabled = sqlExecutionContext.isParallelFilterEnabled();
            try {
                for (boolean isParallel : new boolean[]{true, false}) {
                    sqlExecutionContext.setParallelFilterEnabled(isParallel);
                    for (String column : new String[]{"s", "vc"}) {
                        final String sql = "SELECT ts FROM t WHERE ts IN " + column;
                        assertQuery(sql)
                                .noLeakCheck()
                                .assertsPlanContaining(isParallel ? "Async Filter" : "Filter filter: ts in " + column);
                        assertExceptionNoLeakCheck(sql, sql.lastIndexOf(' ') + 1, "Expected number before unit '#'");
                        final String count = "SELECT count() FROM t WHERE ts IN " + column;
                        assertExceptionNoLeakCheck(count, count.lastIndexOf(' ') + 1, "Expected number before unit '#'");
                    }
                }
                assertIntrinsicRows("ts IN '$now - 1h..$now;1h'", "2\t2024-01-02T09:00:00.000000Z\t2024-01-02T10:00:00.000000Z");
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(isParallelFilterEnabled);
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeWithMalformedTimeFailsAsLiteralAndAsBindVariable() throws Exception {
        // a point in time ignores the time part, which still has to be well-formed: the bind variable
        // selected the row at $now
        assertMalformedTickExprFails("[$now]T[25:00]", "25:00");
    }

    @Test
    public void testPointInTimeWithMalformedTimeFailsInEveryContext() throws Exception {
        // An element that keeps its own time ignores the time part of the list, but a time part that
        // does not parse fails the query wherever the query holds it: a literal that SQL parses on
        // every evaluation as the query compiles, and a bind variable as the query runs, at the
        // position of the expression.
        assertMemoryLeak(() -> {
            createContextTables();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                assertTickExprFailsInEveryContext(
                        "[$now@+00:00]T[25:00]",
                        "Invalid date: 2024-01-02T25:00",
                        "[$now]T[25:00]",
                        "Invalid date: 2024-01-02T25:00"
                );
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeWithMalformedTimeFailsOnEveryTimestampType() throws Exception {
        // A range between two points in time with a time part that does not parse fails the query on
        // a designated nanosecond timestamp, a nullable timestamp, and a DATE, which IN compares as a
        // microsecond timestamp: as a literal that SQL parses on every evaluation when the query
        // compiles, and as a bind variable and a value of a row when it runs. The literal, the bind
        // variable and the value of a row selected the rows of the range.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tn (ts TIMESTAMP_NS, ts2 TIMESTAMP, d DATE, s STRING, vc VARCHAR) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tn VALUES
                    ('2024-01-02T09:30:00.000000000Z', '2024-01-02T09:30:00.000000Z', '2024-01-02T09:30:00.000Z', '[$now - 1h..$now]T[25:00]', '[$now - 1h..$now]T[25:00]'),
                    ('2024-01-02T10:30:00.000000000Z', '2024-01-02T10:30:00.000000Z', '2024-01-02T10:30:00.000Z', '[$now - 1h..$now]T[25:00]', '[$now - 1h..$now]T[25:00]'),
                    ('2024-01-02T11:00:00.000000000Z', NULL, NULL, '[$now - 1h..$now]T[25:00]', '[$now - 1h..$now]T[25:00]')
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                // 09:00 to 10:00 holds the row at 09:30, and not the NULL
                for (String[] column : new String[][]{
                        {"ts", "1\t2024-01-02T09:30:00.000000000Z\t2024-01-02T09:30:00.000000000Z"},
                        {"ts2", "1\t2024-01-02T09:30:00.000000Z\t2024-01-02T09:30:00.000000Z"},
                        {"d", "1\t2024-01-02T09:30:00.000Z\t2024-01-02T09:30:00.000Z"}
                }) {
                    final String query = "SELECT count(), min(" + column[0] + "), max(" + column[0] + ") FROM tn WHERE " + column[0] + " IN ";
                    final String literal = query + "'[$now - 1h..$now@+00:00]T[25:00]'";
                    assertExceptionNoLeakCheck(literal, literal.indexOf('\''), "Invalid date: 2024-01-02T25:00");
                    assertQuery(query + "'[$now - 1h..$now@+00:00]T[09:00]'")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\tmin\tmax\n" + column[1] + "\n");
                    for (boolean isVarchar : new boolean[]{false, true}) {
                        setTickExprBindVariable("[$now - 1h..$now]T[25:00]", isVarchar);
                        assertExceptionNoLeakCheck(query + "$1", query.length(), "Invalid date: 2024-01-02T25:00");
                        setTickExprBindVariable("[$now - 1h..$now]T[09:00]", isVarchar);
                        assertQuery(query + "$1")
                                .noLeakCheck()
                                .noRandomAccess()
                                .expectSize()
                                .returns("count\tmin\tmax\n" + column[1] + "\n");
                    }
                    bindVariableService.clear();
                }
                for (String column : new String[]{"s", "vc"}) {
                    final String sql = "SELECT count() FROM tn WHERE ts IN " + column;
                    assertExceptionNoLeakCheck(sql, sql.lastIndexOf(' ') + 1, "Invalid date: 2024-01-02T25:00");
                }
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeWithMalformedTimeFailsWhenQueryCompiles() throws Exception {
        // An element that keeps its own time ignores the time part of the list, but a time part that
        // does not parse fails a literal as the query compiles, whether SQL parses the literal on
        // every evaluation or the IR represents it, and a bind variable as the query runs, with the
        // error of the parser. The literal that SQL parses on every evaluation, and the bind
        // variable, selected the rows of the element. A prepared query fails on the run that binds
        // such a time part, and keeps working on the runs before and after it.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts,
                               timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts2
                        FROM long_sequence(96)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            final int jitMode = sqlExecutionContext.getJitMode();
            try {
                // 09:00 to 10:00
                final String range = "2\t2024-01-02T09:00:00.000000Z\t2024-01-02T10:00:00.000000Z";
                for (String[] tickExpr : new String[][]{
                        // SQL parses these on every evaluation
                        {"[$now@+00:00]T[25:00]", "Invalid date: 2024-01-02T25:00"},
                        {"[$now]T[09..10]:99", "Invalid date: 2024-01-02T09:99"},
                        {"[$now - 1h..$now@+00:00]T25:00;1h", "Invalid date: 2024-01-02T25:00"},
                        {"[2024-01-02T09:00, $now]T[09:00,25:00]", "Invalid date: 2024-01-02T25:00"},
                        // the IR represents these, and fails on the time part with an error of its own
                        {"[$now]T[25:00]", "Invalid time in time list: 25:00"},
                        {"$now - 1h..$nowT25:00", "Invalid time override: 25:00"}
                }) {
                    for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                        sqlExecutionContext.setJitMode(mode);
                        // the interval scan, NOT IN, an OR that the scan takes, another timestamp
                        // column, and a dateadd() of the designated timestamp, above it and below it
                        assertLiteralFailsWhenQueryCompiles(tickExpr[0], tickExpr[1]);
                    }
                }
                sqlExecutionContext.setJitMode(jitMode);
                for (String[] tickExpr : new String[][]{
                        {"[$now]T[25:00]", "Invalid date: 2024-01-02T25:00"},
                        {"$nowT09:99", "Invalid date: 2024-01-02T09:99"},
                        {"[$now - 1h..$now]T[25:00]@+02:00;1h", "Invalid date: 2024-01-02T25:00"},
                        {"$now - 1h..$nowT[0900,1430]", "Military time format not supported in bracket expansion. Use colons: [09:00,14:30] instead of [0900,1430]"},
                        {"[2024-01-02T09:00, $now]T25:00", "Invalid date: 2024-01-02T25:00"}
                }) {
                    for (boolean isVarchar : new boolean[]{false, true}) {
                        setTickExprBindVariable(tickExpr[0], isVarchar);
                        for (String sql : new String[]{
                                "SELECT count() FROM t WHERE ts IN $1",
                                "SELECT count() FROM t WHERE ts NOT IN $1",
                                "SELECT count() FROM t WHERE ts IN $1 OR ts > '2030-01-01'",
                                "SELECT count() FROM t WHERE ts2 IN $1",
                                "SELECT count() FROM t WHERE dateadd('h', 1, ts) IN $1"
                        }) {
                            assertExceptionNoLeakCheck(sql, sql.indexOf("$1"), tickExpr[1]);
                        }
                    }
                    bindVariableService.clear();
                }
                // a time part that parses leaves the range as it is
                assertTickExprBindVariableMatches("[$now - 1h..$now]T[09:00,14:30]", range);
                assertTickExprMatches("[$now - 1h..$now@+00:00]T[09..10]:30", range);
                final String prepared = "SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1";
                bindVariableService.clear();
                bindVariableService.setStr(0, "[$now - 1h..$now]T10:00");
                try (RecordCursorFactory factory = select(prepared)) {
                    assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns("count\tmin\tmax\n" + range + "\n");
                    bindVariableService.setStr(0, "[$now - 1h..$now]T25:00");
                    try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                        Assert.fail("the run with [$now - 1h..$now]T25:00 should fail");
                    } catch (SqlException | CairoException e) {
                        TestUtils.assertEquals("Invalid date: 2024-01-02T25:00", e.getFlyweightMessage());
                        Assert.assertEquals(prepared.indexOf("$1"), e.getPosition());
                    }
                    bindVariableService.setStr(0, "[$now - 1h..$now]T10:00");
                    assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns("count\tmin\tmax\n" + range + "\n");
                }
                bindVariableService.clear();
                assertIntrinsicRows("ts IN '$now - 1h..$now'", range);
            } finally {
                bindVariableService.clear();
                sqlExecutionContext.setJitMode(jitMode);
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testPointInTimeWithMalformedTimeInRowFailsWithParallelFilterOnAndOff() throws Exception {
        // A value of a row, a range between two points in time with a time part that does not parse,
        // fails the query at the column, whether the filter runs in parallel, on a copy of the
        // function for each worker, or on the thread of the query. The row is in the last of four
        // partitions, and the table keeps answering after the query fails.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts,
                               (CASE WHEN x = 80 THEN '[$now - 1h..$now]T[25:00]' ELSE '[$now - 1h..$now]T[09:00]' END) s,
                               (CASE WHEN x = 80 THEN '[$now - 1h..$now]T[25:00]' ELSE '[$now - 1h..$now]T[09:00]' END)::VARCHAR vc
                        FROM long_sequence(96)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            final boolean isParallelFilterEnabled = sqlExecutionContext.isParallelFilterEnabled();
            try {
                for (boolean isParallel : new boolean[]{true, false}) {
                    sqlExecutionContext.setParallelFilterEnabled(isParallel);
                    for (String column : new String[]{"s", "vc"}) {
                        final String sql = "SELECT ts FROM t WHERE ts IN " + column;
                        assertQuery(sql)
                                .noLeakCheck()
                                .assertsPlanContaining(isParallel ? "Async Filter" : "Filter filter: ts in " + column);
                        assertExceptionNoLeakCheck(sql, sql.lastIndexOf(' ') + 1, "Invalid date: 2024-01-02T25:00");
                        final String count = "SELECT count() FROM t WHERE ts IN " + column;
                        assertExceptionNoLeakCheck(count, count.lastIndexOf(' ') + 1, "Invalid date: 2024-01-02T25:00");
                    }
                }
                assertIntrinsicRows("ts IN '[$now - 1h..$now]T[09:00]'", "2\t2024-01-02T09:00:00.000000Z\t2024-01-02T10:00:00.000000Z");
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(isParallelFilterEnabled);
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testRowValueDateVariableResolvesAgainstQueryClock() throws Exception {
        // a date variable in a value of a row resolves against the time the query started, as one in
        // a literal does, even when the clock moves on to the next day while the query runs
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts, '$today' s, '$today'::varchar vc
                        FROM long_sequence(96)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T23:30:00.000000Z"));
            try {
                currentMicros = MicrosFormatUtils.parseTimestamp("2024-01-03T00:30:00.000000Z");
                assertIntrinsicRows("ts IN s", "24\t2024-01-02T00:00:00.000000Z\t2024-01-02T23:00:00.000000Z");
                assertIntrinsicRows("ts IN vc", "24\t2024-01-02T00:00:00.000000Z\t2024-01-02T23:00:00.000000Z");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testStatementCompiledAtStaleNowRunsInEveryContext() throws Exception {
        // A PostgreSQL Parse compiles a statement at the "now" of the last statement that the
        // connection ran, or at 1970-01-01 if it ran none, and the Execute that runs the statement
        // sets "now" first. At such a "now" the range from 09:30 to "now" starts after it ends, and
        // the JIT compiler evaluates it there, as it opens the interval scan of a filter. The
        // statement compiles all the same wherever the query holds the range, and at 12:00 it
        // returns the rows of the same query over the range without date variables. Run at 03:00,
        // the same statement fails on the range.
        final int jitMode = sqlExecutionContext.getJitMode();
        final boolean isParallelHorizonJoinEnabled = sqlExecutionContext.isParallelHorizonJoinEnabled();
        final boolean isParallelWindowJoinEnabled = sqlExecutionContext.isParallelWindowJoinEnabled();
        assertMemoryLeak(() -> {
            createContextTables();
            final String tickExpr = "$today + 570m..$now";
            final String reference = "BETWEEN '2024-01-02T09:30:00.000000Z' AND '2024-01-02T12:00:00.000000Z'";
            try {
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                // the parallel HORIZON JOIN and WINDOW JOIN bind the filters they take when they
                // read their first row, and the serial ones when they open
                for (boolean isParallel : new boolean[]{true, false}) {
                    sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                    sqlExecutionContext.setParallelWindowJoinEnabled(isParallel);
                    // ? stands for the tick expression, which the filter s = 'a' comes with
                    for (String query : new String[]{
                            "WITH c AS (SELECT * FROM t WHERE ts IN ? AND s = 'a') SELECT count() FROM c",
                            "SELECT count() FROM (SELECT * FROM t WHERE ts IN ? AND s = 'a')",
                            "SELECT count() FROM (SELECT * FROM t WHERE ts IN ? AND s = 'a') a JOIN t b ON a.s = b.s",
                            "SELECT count() FROM t a JOIN (SELECT * FROM t WHERE ts IN ? AND s = 'a') b ON a.s = b.s",
                            "SELECT count() FROM (SELECT * FROM t WHERE ts IN ? AND s = 'a') a LEFT JOIN t b ON a.s = b.s",
                            "SELECT count() FROM t a LEFT JOIN (SELECT * FROM t WHERE ts IN ? AND s = 'a') b ON a.s = b.s",
                            "SELECT count() FROM (SELECT * FROM t WHERE ts IN ? AND s = 'a') a RIGHT JOIN t b ON a.s = b.s",
                            "SELECT count() FROM t a RIGHT JOIN (SELECT * FROM t WHERE ts IN ? AND s = 'a') b ON a.s = b.s",
                            "SELECT count() FROM (SELECT * FROM t WHERE ts IN ? AND s = 'a') a FULL OUTER JOIN t b ON a.s = b.s",
                            "SELECT count() FROM t a FULL OUTER JOIN (SELECT * FROM t WHERE ts IN ? AND s = 'a') b ON a.s = b.s",
                            "SELECT count() FROM (SELECT * FROM t WHERE ts IN ? AND s = 'a') a ASOF JOIN t b",
                            "SELECT count() FROM t a ASOF JOIN (SELECT * FROM t WHERE ts IN ? AND s = 'a') b",
                            "SELECT count() FROM (SELECT * FROM t WHERE ts IN ? AND s = 'a') a LT JOIN t b",
                            "SELECT count() FROM t a LT JOIN (SELECT * FROM t WHERE ts IN ? AND s = 'a') b",
                            "SELECT count() FROM (SELECT * FROM t WHERE ts IN ? AND s = 'a') a SPLICE JOIN t b",
                            "SELECT count() FROM t a SPLICE JOIN (SELECT * FROM t WHERE ts IN ? AND s = 'a') b",
                            "SELECT count() FROM (SELECT * FROM t WHERE ts IN ? AND s = 'a') a CROSS JOIN t b",
                            "SELECT count() FROM t a CROSS JOIN (SELECT * FROM t WHERE ts IN ? AND s = 'a') b",
                            "SELECT a.ts, count() c FROM (SELECT * FROM t WHERE ts IN ? AND s = 'a') a WINDOW JOIN t b ON (a.s = b.s) RANGE BETWEEN 1 hour PRECEDING AND 1 hour FOLLOWING",
                            // the right side of a WINDOW JOIN takes an interval scan, but no filter
                            "SELECT a.ts, count() c FROM t a WINDOW JOIN (SELECT * FROM t WHERE ts IN ?) b ON (a.s = b.s) RANGE BETWEEN 1 hour PRECEDING AND 1 hour FOLLOWING",
                            "SELECT a.ts, count() c FROM t a WINDOW JOIN t b ON (a.s = b.s AND (b.ts IN ? OR b.s = 'z')) RANGE BETWEEN 1 hour PRECEDING AND 1 hour FOLLOWING",
                            "SELECT count() c FROM (SELECT * FROM t WHERE ts IN ? AND s = 'a') a HORIZON JOIN t b ON (a.s = b.s) RANGE FROM 0s TO 0s STEP 1s AS h",
                            "SELECT count() c, max(b.ts) m FROM t a HORIZON JOIN (SELECT * FROM t WHERE ts IN ? AND s = 'a') b ON (a.s = b.s) RANGE FROM 0s TO 0s STEP 1s AS h",
                            "SELECT ts FROM t WHERE ts IN ? AND s = 'a' UNION SELECT ts FROM t",
                            "SELECT ts FROM t UNION SELECT ts FROM t WHERE ts IN ? AND s = 'a'",
                            "SELECT ts FROM t WHERE ts IN ? AND s = 'a' EXCEPT SELECT ts FROM t",
                            "SELECT ts FROM t EXCEPT SELECT ts FROM t WHERE ts IN ? AND s = 'a'",
                            "SELECT ts FROM t WHERE ts IN ? AND s = 'a' INTERSECT SELECT ts FROM t",
                            "SELECT ts FROM t INTERSECT SELECT ts FROM t WHERE ts IN ? AND s = 'a'",
                            "SELECT s, count() FROM t WHERE ts IN ? AND s = 'a' GROUP BY s",
                            "SELECT * FROM t WHERE ts IN ? AND s = 'a' LATEST ON ts PARTITION BY s",
                            "SELECT ts, count() FROM t WHERE ts IN ? AND s = 'a' SAMPLE BY 1h",
                            "SELECT ts, row_number() OVER (PARTITION BY s ORDER BY ts) FROM t WHERE ts IN ? AND s = 'a'",
                            "SELECT * FROM t WHERE ts IN ? AND s = 'a' ORDER BY ts LIMIT -1",
                            // a symbol without an index, and one with an index
                            "SELECT count() FROM t WHERE s = 'a' AND ts IN ?",
                            "SELECT count() FROM ti WHERE s = 'a' AND ts IN ?"
                    }) {
                        final String referenceSql = query.replace("IN ?", reference);
                        // a connection that ran no statement, one that ran its last at 03:00, and one
                        // that ran it just before the range starts
                        for (String compileNow : new String[]{"1970-01-01T00:00:00.000000Z", "2024-01-02T03:00:00.000000Z", "2024-01-02T09:29:59.999999Z"}) {
                            // the literal, and a bind variable of STRING and of VARCHAR type
                            for (int holder = 0; holder < 3; holder++) {
                                final String sql;
                                final int position;
                                if (holder == 0) {
                                    bindVariableService.clear();
                                    sql = query.replace("?", "'" + tickExpr + "'");
                                    position = sql.indexOf('\'' + tickExpr);
                                } else {
                                    setTickExprBindVariable(tickExpr, holder == 2);
                                    sql = query.replace("?", "$1");
                                    // the parallel WINDOW JOIN binds its join filter as it reads its
                                    // first row, and the error that a bind variable raises there
                                    // carries no position
                                    position = isParallel && query.contains("b.ts IN ?") ? -1 : sql.indexOf("$1");
                                }
                                // the context keeps the stale "now", and the clock says 12:00
                                setCurrentMicros(MicrosFormatUtils.parseTimestamp(compileNow));
                                currentMicros = NOW_1200;
                                try (RecordCursorFactory factory = select(sql)) {
                                    sqlExecutionContext.initNow();
                                    try (RecordCursorFactory referenceFactory = select(referenceSql)) {
                                        assertSameRows(referenceFactory, factory, sql + " compiled at " + compileNow);
                                    }
                                    setCurrentMicros(NOW_0300);
                                    assertRunFails(factory, sql, position, RANGE_ORDER_ERROR);
                                }
                            }
                        }
                    }
                }
                bindVariableService.clear();

                // EXPLAIN compiles the query, and renders its plan when it runs
                final String explain = "EXPLAIN SELECT * FROM t WHERE ts IN '" + tickExpr + "' AND s = 'a'";
                setCurrentMicros(NOW_0300);
                currentMicros = NOW_1200;
                try (RecordCursorFactory factory = select(explain)) {
                    sqlExecutionContext.initNow();
                    final StringSink plan = new StringSink();
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        CursorPrinter.println(cursor, factory.getMetadata(), plan);
                    }
                    TestUtils.assertContains(plan, JitUtil.isJitSupported() ? "Async JIT Filter" : "Async Filter");
                    TestUtils.assertContains(plan, "intervals: [(\"2024-01-02T09:30:00.000000Z\",\"2024-01-02T12:00:00.000000Z\")]");
                }
            } finally {
                bindVariableService.clear();
                sqlExecutionContext.setJitMode(jitMode);
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallelHorizonJoinEnabled);
                sqlExecutionContext.setParallelWindowJoinEnabled(isParallelWindowJoinEnabled);
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testStaticDateTickCalendarDurationInEveryContext() throws Exception {
        // A literal with a fixed date, a date variable, a tick calendar and a duration selects the same
        // rows wherever a query holds it, as does its bind variable: on Monday 2025-01-27, the sessions of
        // Friday 2025-01-24 and of the Monday, from 14:30 to 21:00Z, each an hour longer, which hold the
        // 15 half hours from 14:30 to 21:30 of each day, 8 of symbol 'a' and 7 of symbol 'b'. The table
        // has the 192 half hours of 2025-01-24 to 2025-01-27, 96 of each symbol.
        useTickCalendar();
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2025-01-24T00:00:00.000000Z', 1_800_000_000L) ts,
                               (CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END)::SYMBOL s
                        FROM long_sequence(192)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("CREATE TABLE ti (ts TIMESTAMP, s SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO ti SELECT ts, s FROM t");
            setCurrentMicros(MONDAY_NOON);
            try {
                final String tickExpr = "[$today, 2025-01-24]#XNYS;1h";
                // ? stands for the tick expression
                final String[][] contexts = {
                        {"WITH c AS (SELECT * FROM t WHERE ts IN ?) SELECT count() FROM c", "count\n30\n"},
                        {"SELECT count() FROM (SELECT * FROM t WHERE ts IN ?)", "count\n30\n"},
                        {"SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a JOIN t b ON a.s = b.s", "count\n2880\n"},
                        {"SELECT count() FROM t a JOIN (SELECT * FROM t WHERE ts IN ?) b ON a.s = b.s", "count\n2880\n"},
                        {"SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a LEFT JOIN t b ON a.s = b.s", "count\n2880\n"},
                        {"SELECT count() FROM t a LEFT JOIN (SELECT * FROM t WHERE ts IN ?) b ON a.s = b.s", "count\n2880\n"},
                        {"SELECT count(), max(b.ts) FROM (SELECT * FROM t WHERE ts IN ?) a ASOF JOIN t b", "count\tmax\n30\t2025-01-27T21:30:00.000000Z\n"},
                        {"SELECT count(), max(b.ts) FROM t a ASOF JOIN (SELECT * FROM t WHERE ts IN ?) b", "count\tmax\n192\t2025-01-27T21:30:00.000000Z\n"},
                        {"SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a CROSS JOIN t b", "count\n5760\n"},
                        {"SELECT count() FROM t a CROSS JOIN (SELECT * FROM t WHERE ts IN ?) b", "count\n5760\n"},
                        // the two rows before 01:00 and the rows of the expression
                        {"SELECT count() FROM (SELECT ts FROM t WHERE ts IN ? UNION SELECT ts FROM t WHERE ts < '2025-01-24T01:00')", "count\n32\n"},
                        {"SELECT count() FROM (SELECT ts FROM t WHERE ts < '2025-01-24T01:00' UNION SELECT ts FROM t WHERE ts IN ?)", "count\n32\n"},
                        // the rows of the expression from 16:00 on 2025-01-24 on
                        {"SELECT count() FROM (SELECT ts FROM t WHERE ts IN ? EXCEPT SELECT ts FROM t WHERE ts < '2025-01-24T16:00')", "count\n27\n"},
                        // the nine rows from 14:00 to 18:00 but those of the expression
                        {
                                "SELECT count() FROM (SELECT ts FROM t WHERE ts BETWEEN '2025-01-24T14:00' AND '2025-01-24T18:00' EXCEPT SELECT ts FROM t WHERE ts IN ?)",
                                "count\n1\n"
                        },
                        {"SELECT count() FROM (SELECT ts FROM t WHERE ts IN ? INTERSECT SELECT ts FROM t WHERE ts >= '2025-01-24T16:00')", "count\n27\n"},
                        {"SELECT count() FROM (SELECT ts FROM t WHERE ts >= '2025-01-24T16:00' INTERSECT SELECT ts FROM t WHERE ts IN ?)", "count\n27\n"},
                        {"SELECT s, count() FROM t WHERE ts IN ? ORDER BY s", "s\tcount\na\t16\nb\t14\n"},
                        {
                                "SELECT ts, count() FROM t WHERE ts IN ? SAMPLE BY 1h",
                                """
                                        ts\tcount
                                        2025-01-24T14:00:00.000000Z\t1
                                        2025-01-24T15:00:00.000000Z\t2
                                        2025-01-24T16:00:00.000000Z\t2
                                        2025-01-24T17:00:00.000000Z\t2
                                        2025-01-24T18:00:00.000000Z\t2
                                        2025-01-24T19:00:00.000000Z\t2
                                        2025-01-24T20:00:00.000000Z\t2
                                        2025-01-24T21:00:00.000000Z\t2
                                        2025-01-27T14:00:00.000000Z\t1
                                        2025-01-27T15:00:00.000000Z\t2
                                        2025-01-27T16:00:00.000000Z\t2
                                        2025-01-27T17:00:00.000000Z\t2
                                        2025-01-27T18:00:00.000000Z\t2
                                        2025-01-27T19:00:00.000000Z\t2
                                        2025-01-27T20:00:00.000000Z\t2
                                        2025-01-27T21:00:00.000000Z\t2
                                        """
                        },
                        {
                                "SELECT s, ts FROM (SELECT * FROM t WHERE ts IN ? LATEST ON ts PARTITION BY s) ORDER BY s",
                                "s\tts\na\t2025-01-27T21:30:00.000000Z\nb\t2025-01-27T21:00:00.000000Z\n"
                        },
                        {
                                "SELECT count(), max(rn) FROM (SELECT ts, s, row_number() OVER (PARTITION BY s ORDER BY ts) rn FROM t WHERE ts IN ?)",
                                "count\tmax\n30\t16\n"
                        },
                        {"SELECT ts, s FROM t WHERE ts IN ? ORDER BY ts LIMIT -1", "ts\ts\n2025-01-27T21:30:00.000000Z\ta\n"},
                        // a symbol without an index, and one with an index
                        {"SELECT count() FROM t WHERE s = 'a' AND ts IN ?", "count\n16\n"},
                        {"SELECT count() FROM ti WHERE s = 'a' AND ts IN ?", "count\n16\n"}
                };
                for (String[] context : contexts) {
                    bindVariableService.clear();
                    assertQuery(context[0].replace("?", "'" + tickExpr + "'"))
                            .noLeakCheck()
                            .inferRandomAccess()
                            .inferTimestamp()
                            .sizeMayVary()
                            .returns(context[1]);
                    bindVariableService.setStr(0, tickExpr);
                    assertQuery(context[0].replace("?", "$1"))
                            .noLeakCheck()
                            .inferRandomAccess()
                            .inferTimestamp()
                            .sizeMayVary()
                            .returns(context[1]);
                }
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testStaticDateTickCalendarDurationInEveryFilterPath() throws Exception {
        // A literal with a fixed date, a date variable, a tick calendar and a duration takes the fixed
        // date with its own extent, the tick calendar keeps what its sessions hold of it, and the duration
        // extends what they keep, once, in every filter path, as its bind variable does. On Monday
        // 2025-01-27, XNYS trades from 14:30 to 21:00Z, as on Friday 2025-01-24: each session an hour
        // longer holds the half hours from 14:30 to 21:30, the minute at 15:00 an hour longer those from
        // 15:00 to 16:00, and the minute at 15:00Z alone the half hour at 15:00.
        useTickCalendar();
        assertMemoryLeak(() -> {
            createHalfHourTables();
            setCurrentMicros(MONDAY_NOON);
            try {
                assertTickExprInEveryFilterPath("[$today, 2025-01-24]#XNYS;1h", 30, "2025-01-24T14:30", "2025-01-27T21:30");
                assertTickExprInEveryFilterPath("[2025-01-24, $today]@America/New_York#XNYS;1h", 30, "2025-01-24T14:30", "2025-01-27T21:30");
                assertTickExprInEveryFilterPath("[$today, 2025-01-24T15:00]#XNYS;1h", 18, "2025-01-24T15:00", "2025-01-27T21:30");
                assertTickExprInEveryFilterPath("[$today, 2025-01-24T[09:30@America/New_York]]@Europe/Paris#XNYS;1h", 18, "2025-01-24T14:30", "2025-01-27T21:30");
                assertTickExprInEveryFilterPath("[$today, 2025-01-24T[15:00@UTC]]#XNYS;0h", 14, "2025-01-24T15:00", "2025-01-27T20:30");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testStaticDateTickCalendarDurationInView() throws Exception {
        // A view stores the literal and parses it again for each query, before and after the engine
        // releases what it holds, and EXPLAIN shows its intervals: on Monday 2025-01-27, the sessions of
        // Friday 2025-01-24 and of the Monday, from 14:30 to 21:00Z, each an hour longer.
        useTickCalendar();
        assertMemoryLeak(() -> {
            createMinuteTables();
            setCurrentMicros(MONDAY_NOON);
            try {
                execute("CREATE VIEW v AS (SELECT * FROM m WHERE ts IN '[$today, 2025-01-24]#XNYS;1h')");
                drainViewQueue();
                final String expected = "count\tmin\tmax\n900\t2025-01-24T14:30:00.000000Z\t2025-01-27T21:59:00.000000Z\n";
                assertQuery("SELECT count(), min(ts), max(ts) FROM v")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
                engine.releaseInactive();
                assertQuery("SELECT count(), min(ts), max(ts) FROM v")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
                try (RecordCursorFactory factory = select("SELECT * FROM m WHERE ts IN '[$today, 2025-01-24]#XNYS;1h'")) {
                    assertPlanContains(
                            factory,
                            "intervals: [(\"2025-01-24T14:30:00.000000Z\",\"2025-01-24T21:59:59.999999Z\"),(\"2025-01-27T14:30:00.000000Z\",\"2025-01-27T21:59:59.999999Z\")]"
                    );
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testStaticDateTickCalendarDurationPerMinute() throws Exception {
        // A literal with a fixed date, a date variable, a tick calendar and a duration, over the minutes
        // of 2025-01-24 to 2025-01-27, 5,760 rows, in the interval scan, the row filter with the JIT on
        // and off, on another timestamp column, under NOT IN, on a nanosecond timestamp, and through a
        // dateadd() of the designated timestamp, which selects the minutes an hour before. On Monday
        // 2025-01-27, XNYS trades from 14:30 to 21:00Z, as on Friday 2025-01-24: a session holds 390
        // minutes, and an hour longer 450. Where the parser reads the list alike, its bind variable selects
        // the same rows, also where a fixed date holds another, as the tick calendar unites the intervals
        // before it cuts them; the parser does not take the duration of a fixed date in a list with a tick
        // calendar, see bug-date-list-calendar-cut-inside-list.
        useTickCalendar();
        assertMemoryLeak(() -> {
            createMinuteTables();
            execute("CREATE TABLE e (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            setCurrentMicros(MONDAY_NOON);
            try {
                // each session, an hour longer
                assertMinuteRowsInEveryFilterPath("[$today, 2025-01-24]#XNYS;1h", 900, "2025-01-24T14:30", "2025-01-27T21:59", 900);
                // the minute at 15:00, an hour longer, and the session of the Monday, an hour longer
                assertMinuteRowsInEveryFilterPath("[$today, 2025-01-24T15:00]#XNYS;1h", 511, "2025-01-24T15:00", "2025-01-27T21:59", 511);
                // the hour of the fixed date from 15:00, its own duration, and the session of the Monday;
                // 15:00 in New York is 20:00Z, and 15:00 in New York on the days of the first 24 of January
                // and of the Monday, which the parser does not read either
                assertMinuteRowsInEveryFilterPath("[$today, 2025-01-24T15:00;1h]#XNYS", 450, "2025-01-24T15:00", "2025-01-27T20:59", -1);
                assertMinuteRowsInEveryFilterPath("[$today, 2025-01-24T15:00;1h]@America/New_York#XNYS", 450, "2025-01-24T20:00", "2025-01-27T20:59", -1);
                assertMinuteRowsInEveryFilterPath("[$today, 2025-01;24d]T15:00@America/New_York#XNYS", 2, "2025-01-24T20:00", "2025-01-27T20:00", -1);
                // each session as it is
                assertMinuteRowsInEveryFilterPath("[$today, 2025-01-24]#XNYS;", 780, "2025-01-24T14:30", "2025-01-27T20:59", 780);
                // 09:30 in New York is 14:30Z, and today in Paris holds the session of the Monday
                assertMinuteRowsInEveryFilterPath("[$today, 2025-01-24T[09:30@America/New_York]]@Europe/Paris#XNYS;1h", 511, "2025-01-24T14:30", "2025-01-27T21:59", 511);
                assertMinuteRowsInEveryFilterPath("[$today, 2025-01-24T[09:30@America/New_York]]@Europe/Paris#XNYS;1h1m", 513, "2025-01-24T14:30", "2025-01-27T22:00", 513);
                // 15:00:00Z in Paris is 14:00Z, before the session
                assertMinuteRowsInEveryFilterPath("[$today, 2025-01-24T15:00:00Z]@Europe/Paris#XNYS;1h", 450, "2025-01-27T14:30", "2025-01-27T21:59", 450);
                // today in Hong Kong ends at 16:00Z and holds its minute at 23:00, 15:00Z
                assertMinuteRowsInEveryFilterPath("[$today, 2025-01-27T23:00]@Asia/Hong_Kong#XNYS;1h", 150, "2025-01-27T14:30", "2025-01-27T16:59", 150);
                assertMinuteRowsInEveryFilterPath("[$today, 2025-01-27T23:00]@Asia/Hong_Kong#XNYS;", 90, "2025-01-27T14:30", "2025-01-27T15:59", 90);
                // the minute at 15:00Z, or at 09:30 in New York, as it is
                assertMinuteRowsInEveryFilterPath("[$today, 2025-01-24T[15:00@UTC]]#XNYS;0h", 391, "2025-01-24T15:00", "2025-01-27T20:59", 391);
                assertMinuteRowsInEveryFilterPath("[$today, 2025-01-24T[15:00@UTC]]#XNYS;", 391, "2025-01-24T15:00", "2025-01-27T20:59", 391);
                assertMinuteRowsInEveryFilterPath("[$today, 2025-01-24T[09:30@America/New_York]]#XNYS;0h", 391, "2025-01-24T14:30", "2025-01-27T20:59", 391);
                // an empty table
                assertCountMinMax("e", "ts IN '[$today, 2025-01-24]#XNYS;1h'", "count\tmin\tmax\n0\t\t\n");
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testStaticDateTickCalendarFixedDateSemicolonFailsInEveryPath() throws Exception {
        // A fixed date with a ';' of its own before the ';' of the list makes its parse read a
        // repeating interval, so the literal fails as the query compiles, at its position, wherever the
        // query holds it, as it always has; its bind variable fails on the list, which the parser takes
        // to the first ';' under a tick calendar.
        useTickCalendar();
        assertMemoryLeak(() -> {
            createMinuteTables();
            setCurrentMicros(MONDAY_NOON);
            try {
                final String tickExpr = "[$today, 2025-01-24;1h;1d]#XNYS;1h";
                for (String condition : new String[]{
                        "ts IN '" + tickExpr + "'",
                        "ts2 IN '" + tickExpr + "'",
                        "ts NOT IN '" + tickExpr + "'",
                        "dateadd('h', 1, ts) IN '" + tickExpr + "'"
                }) {
                    final String sql = "SELECT count() FROM m WHERE " + condition;
                    assertExceptionNoLeakCheck(sql, sql.indexOf("'["), "Count not a number");
                }
                bindVariableService.clear();
                bindVariableService.setStr(0, tickExpr);
                final String bindSql = "SELECT count() FROM m WHERE ts IN $1";
                assertExceptionNoLeakCheck(bindSql, bindSql.indexOf("$1"), "Unclosed '[' in date list");
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testStaticDateTickCalendarOverflowAfterOtherCondition() throws Exception {
        // The interval scan applies the conditions of an AND from the last one: it evaluates a literal
        // listed first into the intervals of the condition after it, and a literal listed last into no
        // intervals. A literal whose fixed date alone has more own extents than the interval limit
        // parses as its bind variable on each run, which merges only the intervals that it adds. On
        // Monday 2025-01-27, today holds the 1,440 seconds at the minutes of the day, the session of
        // XNYS, from 14:30 to 21:00Z, holds none of the 840 seconds at the minutes from 00:00 to 13:59 of
        // Friday 2025-01-24, and the duration extends the session of the Monday by a minute: the 391
        // minutes from 14:30 to 21:00, which the days from 2025-01-24 on and today hold, and the 361 of
        // them from 15:00 on.
        useTickCalendar();
        assertMemoryLeak(() -> {
            createMinuteTables();
            setCurrentMicros(MONDAY_NOON);
            try {
                final String literal = "ts IN '[$today, 2025-01-27T[00..23]:[00..59]:00, 2025-01-24T[00..13]:[00..59]:00]#XNYS;1m'";
                // the condition, then the count and the first minute of the rows
                final String[][] cases = {
                        {"ts >= '2025-01-24'", "391", "2025-01-27T14:30"},
                        {"ts IN '$today'", "391", "2025-01-27T14:30"},
                        {"ts >= '2025-01-27T15:00'", "361", "2025-01-27T15:00"}
                };
                for (String[] c : cases) {
                    for (String condition : new String[]{c[0] + " AND " + literal, literal + " AND " + c[0]}) {
                        // the table, and the seconds and fraction of a second that it prints
                        for (String[] table : new String[][]{{"m", ":00.000000Z"}, {"mn", ":00.000000000Z"}}) {
                            assertQuery("SELECT count(), min(ts), max(ts) FROM " + table[0] + " WHERE " + condition)
                                    .noLeakCheck()
                                    .noRandomAccess()
                                    .expectSize()
                                    .withPlanContaining("Interval forward scan")
                                    .returns("count\tmin\tmax\n" + c[1] + '\t' + c[2] + table[1] + "\t2025-01-27T21:00" + table[1] + '\n');
                        }
                    }
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testStaticDateTickCalendarOverflowFailsInEveryContext() throws Exception {
        // A fixed date whose own extents alone exceed the interval limit, the 1,440 seconds at the
        // minutes of 2025-01-24, which nothing else in the list holds on Monday 2025-01-27, fails a query
        // wherever it holds the list: the literal as the query compiles, and its bind variable as the
        // query runs, each at its position. The table keeps answering.
        useTickCalendar();
        assertMemoryLeak(() -> {
            createContextTables();
            setCurrentMicros(MONDAY_NOON);
            try {
                final String overLimit = "[$today, 2025-01-24T[00..23]:[00..59]:00]#XNYS;1m";
                final String limitError = "Bracket expansion produces too many intervals (max 1024)";
                assertTickExprFailsInEveryContext(overLimit, limitError, overLimit, limitError);
                assertIntrinsicRows("ts IN '[2024-01-02]T[09:00,10:00]'", "2\t2024-01-02T09:00:00.000000Z\t2024-01-02T10:00:00.000000Z");
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testStaticDateTickCalendarOverflowRangeOrderFailsAsBindVariable() throws Exception {
        // A literal whose fixed date alone has more own extents than the interval limit parses as its
        // bind variable on each run, so a range that starts after it ends fails it with the error of the
        // parser, at the position of the parser: the literal, its quote, plus the 6 characters to the
        // '..' in the expression, see bug-range-order-error-position-by-form. A bind variable fails at
        // the bind variable, as the position of the parser points into its value. Compiled at 14:00Z on
        // Monday 2025-01-27, the range from today to 13 hours before now ends at 01:00Z, and today holds
        // the 1,440 seconds of the fixed date; at 12:00Z the range ends on the Sunday before.
        useTickCalendar();
        assertMemoryLeak(() -> {
            createHalfHourTables();
            final String tickExpr = "[$today..$now-13h, $today, 2025-01-27T[00..23]:[00..59]:00]#XNYS;1m";
            final String literalSql = "SELECT count() FROM t WHERE ts IN '" + tickExpr + "'";
            final String bindSql = "SELECT count() FROM t WHERE ts IN $1";
            try {
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-27T14:00:00.000000Z"));
                try (RecordCursorFactory factory = select(literalSql)) {
                    setCurrentMicros(MONDAY_NOON);
                    assertRunFails(factory, literalSql, literalSql.indexOf('\'') + 6, RANGE_ORDER_ERROR);
                }
                bindVariableService.clear();
                bindVariableService.setStr(0, tickExpr);
                assertExceptionNoLeakCheck(bindSql, bindSql.indexOf("$1"), RANGE_ORDER_ERROR);
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
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
        // A tick expression with no interval at all, here a day filter that drops its only day,
        // never matches: IN folds to false and NOT IN to true, and the enclosing OR folds around
        // them, with the JIT on and off. IN alone leaves no row to scan, NOT IN and the OR of NOT IN
        // no filter, and the OR of IN only the other condition in the row filter.
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
                    assertFilterRows("ts IN '2024-01-02#Mon' OR ts > '2030-01-01'", "filter: 2030-01-01T00:00:00.000000Z<ts", "0\t\t");
                    assertFilterRows("ts NOT IN '2024-01-02#Mon' OR ts > '2030-01-01'", "filter: null", "96\t2024-01-01T00:00:00.000000Z\t2024-01-04T23:00:00.000000Z");
                    assertFilterRows("ts2 IN '2024-01-02#Mon'", "Empty table", "0\t\t");
                    assertFilterRows("ts2 NOT IN '2024-01-02#Mon'", "filter: null", "96\t2024-01-01T00:00:00.000000Z\t2024-01-04T23:00:00.000000Z");
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
    public void testTickValueThatFailsWhenQueryCompilesKeepsJit() throws Exception {
        // The JIT compiler opens the interval scan of a filter as the query compiles, which parses a
        // bind variable that is set by then, and runs a scalar sub-query, with the filters of its
        // own. A value that does not parse, one with more intervals than the limit, and a range that
        // starts after it ends at the "now" of the compile fail nothing there, and the filter keeps
        // the JIT. The query fails on such a value when it runs, and returns the rows of the value
        // it runs with.
        final int jitMode = sqlExecutionContext.getJitMode();
        assertMemoryLeak(() -> {
            createContextTables();
            final String jitFilter = JitUtil.isJitSupported() ? "Async JIT Filter" : "Async Filter";
            final String sql = "SELECT count() FROM t WHERE ts IN $1 AND s = 'a'";
            final String subQuerySql = "SELECT count() FROM t WHERE ts > (SELECT min(ts) FROM t WHERE now() IN $1) AND s = 'a'";
            try {
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                for (boolean isVarchar : new boolean[]{false, true}) {
                    // the value at the compile, the error of the query that runs with it at 12:00, if
                    // any, and the count at 12:00 with the value, or after the error, with the range
                    // from 09:30 to "now", which holds 11:00, on an odd hour
                    for (String[] value : new String[][]{
                            {"garbage", "Invalid date: garbage", "1"},
                            {"[$today..$today + 2000d]T09:00", "Date range expansion produces too many intervals (max 1024)", "1"},
                            {"$today + 570m..$now", null, "1"},
                            {null, null, "0"}
                    }) {
                        setTickExprBindVariable(value[0], isVarchar);
                        // the context keeps 03:00, and the clock says 12:00
                        setCurrentMicros(NOW_0300);
                        currentMicros = NOW_1200;
                        try (RecordCursorFactory factory = select(sql)) {
                            sqlExecutionContext.initNow();
                            if (value[1] != null) {
                                assertRunFails(factory, sql, sql.indexOf("$1"), value[1]);
                                setTickExprBindVariable("$today + 570m..$now", isVarchar);
                            }
                            assertFactory(factory)
                                    .withContext(sqlExecutionContext)
                                    .noRandomAccess()
                                    .expectSize()
                                    .returns("count\n" + value[2] + "\n");
                            assertPlanContains(factory, jitFilter);
                        }
                    }

                    // the sub-query runs as the JIT compiler opens the interval scan
                    for (String value : new String[]{"garbage", "$today + 570m..$now"}) {
                        setTickExprBindVariable(value, isVarchar);
                        setCurrentMicros(NOW_0300);
                        currentMicros = NOW_1200;
                        try (RecordCursorFactory factory = select(subQuerySql)) {
                            sqlExecutionContext.initNow();
                            setTickExprBindVariable("$today + 570m..$now", isVarchar);
                            assertPlanContains(factory, jitFilter);
                            // "now" is in the range at 12:00, so the sub-query returns the first
                            // row, at 00:00 with s = 'b', and the query counts every row with
                            // s = 'a', one on each odd hour of the four days
                            assertFactory(factory)
                                    .withContext(sqlExecutionContext)
                                    .noRandomAccess()
                                    .expectSize()
                                    .returns("count\n48\n");
                        }
                    }
                }
            } finally {
                bindVariableService.clear();
                sqlExecutionContext.setJitMode(jitMode);
                setCurrentMicros(-1);
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

    @Test
    public void testUnknownDurationUnitFailsInEveryContext() throws Exception {
        // A literal with a duration unit that the timestamp drivers do not add fails the query as it
        // compiles, wherever the query holds it, at the position of the literal. A bind variable
        // with the expression fails the query the same way when it runs.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts,
                               (CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END)::SYMBOL s
                        FROM long_sequence(96)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                // ? stands for the tick expression
                for (String query : new String[]{
                        "WITH c AS (SELECT * FROM t WHERE ts IN ?) SELECT count() FROM c",
                        "SELECT count() FROM (SELECT * FROM t WHERE ts IN ?)",
                        "SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a JOIN t b ON a.s = b.s",
                        "SELECT count() FROM t a JOIN (SELECT * FROM t WHERE ts IN ?) b ON a.s = b.s",
                        "SELECT ts FROM t WHERE ts IN ? UNION SELECT ts FROM t",
                        "SELECT ts FROM t UNION SELECT ts FROM t WHERE ts IN ?",
                        "SELECT * FROM t WHERE ts IN ? LATEST ON ts PARTITION BY s",
                        "SELECT ts, count() FROM t WHERE ts IN ? SAMPLE BY 1h",
                        "SELECT ts, row_number() OVER (PARTITION BY s ORDER BY ts) FROM t WHERE ts IN ?",
                        "SELECT * FROM t WHERE ts IN ? ORDER BY ts LIMIT -1"
                }) {
                    final String literal = query.replace("?", "'$today;1x'");
                    assertExceptionNoLeakCheck(literal, literal.indexOf('\''), "Invalid duration unit: x");
                    final String bind = query.replace("?", "$1");
                    bindVariableService.clear();
                    bindVariableService.setStr(0, "$today;1x");
                    assertExceptionNoLeakCheck(bind, bind.indexOf("$1"), "Invalid duration unit: x");
                    bindVariableService.clear();
                }
                // EXPLAIN compiles the query without running it
                final String explain = "EXPLAIN SELECT * FROM t WHERE ts IN '$today;1x'";
                assertExceptionNoLeakCheck(explain, explain.indexOf('\''), "Invalid duration unit: x");
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testUnknownDurationUnitFailsOnEveryTimestampType() throws Exception {
        // A literal with a duration unit that the timestamp drivers do not add fails the query on a
        // designated nanosecond timestamp, a nullable timestamp, and a DATE, which IN compares as a
        // microsecond timestamp. The literal matched every row before $today, and the rows where
        // the nullable column is NULL, which a timestamp stores as its smallest value. A bind
        // variable, and a value of a row, with the expression fail the query when it runs.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tn (ts TIMESTAMP_NS, ts2 TIMESTAMP, d DATE, s STRING, vc VARCHAR) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tn VALUES
                    ('2024-01-01T09:00:00.000000000Z', '2024-01-01T09:00:00.000000Z', '2024-01-01T09:00:00.000Z', '$today;1x', '$today;1x'),
                    ('2024-01-02T00:30:00.000000000Z', '2024-01-02T00:30:00.000000Z', '2024-01-02T00:30:00.000Z', '$today;1x', '$today;1x'),
                    ('2024-01-02T09:00:00.000000000Z', NULL, NULL, '$today;1x', '$today;1x')
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                // the first hour of $today holds the row at 00:30, and not the NULL
                for (String[] column : new String[][]{
                        {"ts", "1\t2024-01-02T00:30:00.000000000Z\t2024-01-02T00:30:00.000000000Z"},
                        {"ts2", "1\t2024-01-02T00:30:00.000000Z\t2024-01-02T00:30:00.000000Z"},
                        {"d", "1\t2024-01-02T00:30:00.000Z\t2024-01-02T00:30:00.000Z"}
                }) {
                    final String query = "SELECT count(), min(" + column[0] + "), max(" + column[0] + ") FROM tn WHERE " + column[0] + " IN ";
                    final String literal = query + "'$today;1x'";
                    assertExceptionNoLeakCheck(literal, literal.indexOf('\''), "Invalid duration unit: x");
                    assertQuery(query + "'$today;1h'")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\tmin\tmax\n" + column[1] + "\n");
                    for (boolean isVarchar : new boolean[]{false, true}) {
                        setTickExprBindVariable("$today;1x", isVarchar);
                        assertExceptionNoLeakCheck(query + "$1", query.length(), "Invalid duration unit: x");
                        setTickExprBindVariable("$today;1h", isVarchar);
                        assertQuery(query + "$1")
                                .noLeakCheck()
                                .noRandomAccess()
                                .expectSize()
                                .returns("count\tmin\tmax\n" + column[1] + "\n");
                    }
                    bindVariableService.clear();
                }
                for (String column : new String[]{"s", "vc"}) {
                    final String sql = "SELECT count() FROM tn WHERE ts IN " + column;
                    assertExceptionNoLeakCheck(sql, sql.lastIndexOf(' ') + 1, "Invalid duration unit: x");
                }
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testUnknownDurationUnitFailsWhateverTheData() throws Exception {
        // A literal with a duration unit that the timestamp drivers do not add fails the query as it
        // compiles, without reading data, on an empty table and on one of a single row, and before
        // the interval limit. A prepared query fails on the run that binds such an expression, and
        // keeps working on the runs before and after it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE empty_table (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE single_row (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO single_row VALUES ('2024-01-01T09:00:00.000000Z')");
            createHourlyTable();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                for (String table : new String[]{"empty_table", "single_row"}) {
                    final String sql = "SELECT count() FROM " + table + " WHERE ts IN '$today;1x'";
                    assertExceptionNoLeakCheck(sql, sql.indexOf('\''), "Invalid duration unit: x");
                }
                // 09:00 on 2,001 days, more intervals than the limit of 1,024
                final String overLimit = "[$today..$today + 2000d]T09:00;1x";
                assertQueryFails("SELECT count() FROM t WHERE ts IN '" + overLimit + "'", overLimit, "Invalid duration unit: x");
                bindVariableService.clear();
                bindVariableService.setStr(0, overLimit);
                assertQueryFails("SELECT count() FROM t WHERE ts IN $1", overLimit, "Invalid duration unit: x");

                final String firstHour = "count\tmin\tmax\n1\t2024-01-02T00:00:00.000000Z\t2024-01-02T00:00:00.000000Z\n";
                final String prepared = "SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1";
                bindVariableService.setStr(0, "$today;1h");
                try (RecordCursorFactory factory = select(prepared)) {
                    assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns(firstHour);
                    bindVariableService.setStr(0, "$today;1x");
                    try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                        Assert.fail("the run with $today;1x should fail");
                    } catch (SqlException | CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Invalid duration unit: x");
                        Assert.assertEquals(prepared.indexOf("$1"), e.getPosition());
                    }
                    bindVariableService.setStr(0, "$today;1h");
                    assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns(firstHour);
                }
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testUnknownDurationUnitLiteralFailsWhenQueryCompiles() throws Exception {
        // A duration unit that the timestamp drivers do not add, such as a typo of one that they do,
        // fails a literal as the query compiles, with the error that a bind variable with the same
        // expression fails the query with when it runs. The literal selected every row before the
        // start of its time part. The units that the drivers add keep their meaning, and the table
        // keeps answering after each failure.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts,
                               timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts2
                        FROM long_sequence(96)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            final int jitMode = sqlExecutionContext.getJitMode();
            try {
                // h is hours, d days, M months and m minutes, s seconds
                final String firstHour = "1\t2024-01-02T00:00:00.000000Z\t2024-01-02T00:00:00.000000Z";
                for (String[] known : new String[][]{
                        {"$today;1h", firstHour},
                        {"$today;2d", "48\t2024-01-02T00:00:00.000000Z\t2024-01-03T23:00:00.000000Z"},
                        {"$today;1M", "72\t2024-01-02T00:00:00.000000Z\t2024-01-04T23:00:00.000000Z"},
                        {"$today;1m", firstHour},
                        {"$NOW;1s", "1\t2024-01-02T10:00:00.000000Z\t2024-01-02T10:00:00.000000Z"}
                }) {
                    assertTickExprMatches(known[0], known[1]);
                    assertTickExprBindVariableMatches(known[0], known[1]);
                }
                for (String[] tickExpr : new String[][]{
                        {"$today;1x", "x"},
                        {"$now;1H", "H"},
                        {"$today;2D", "D"},
                        {"[$today]T09:00;1x", "x"},
                        {"$today@Europe/London;1x", "x"},
                        {"$today#Tue;1x", "x"},
                        {"[$yesterday..$today];1x", "x"},
                        {"$NOW;1S", "S"}
                }) {
                    final String expectedError = "Invalid duration unit: " + tickExpr[1];
                    for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                        sqlExecutionContext.setJitMode(mode);
                        // the interval scan, NOT IN, an OR that the scan takes, another timestamp
                        // column, and a dateadd() of the designated timestamp, above it and below it
                        assertLiteralFailsWhenQueryCompiles(tickExpr[0], expectedError);
                        // an OR that leaves the IN in the row filter
                        final String sql = "SELECT count() FROM t WHERE ts IN '" + tickExpr[0] + "' OR ts > '2030-01-01'";
                        try (RecordCursorFactory ignore = select(sql)) {
                            Assert.fail("compiled: " + sql);
                        } catch (SqlException e) {
                            TestUtils.assertEquals(sql, expectedError, e.getFlyweightMessage());
                            Assert.assertEquals(sql, sql.indexOf('\''), e.getPosition());
                        }
                    }
                    sqlExecutionContext.setJitMode(jitMode);
                    for (boolean isVarchar : new boolean[]{false, true}) {
                        setTickExprBindVariable(tickExpr[0], isVarchar);
                        for (String sql : new String[]{
                                "SELECT count() FROM t WHERE ts IN $1",
                                "SELECT count() FROM t WHERE ts IN $1 OR ts > '2030-01-01'",
                                "SELECT count() FROM t WHERE ts2 IN $1"
                        }) {
                            assertExceptionNoLeakCheck(sql, sql.indexOf("$1"), expectedError);
                        }
                    }
                    bindVariableService.clear();
                    assertIntrinsicRows("ts IN '$today;1h'", firstHour);
                }
            } finally {
                bindVariableService.clear();
                sqlExecutionContext.setJitMode(jitMode);
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

    private static void assertPlanContains(RecordCursorFactory factory, String expected) {
        planSink.of(factory, sqlExecutionContext);
        TestUtils.assertContains(planSink.getSink(), expected);
    }

    /**
     * Asserts that the query fails when it runs, with the error at the position, unless that is -1.
     */
    private static void assertRunFails(RecordCursorFactory factory, String sql, int position, String error) {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            TestUtils.drainCursor(cursor);
            Assert.fail("the query should fail: " + sql);
        } catch (CairoException | SqlException e) {
            TestUtils.assertEquals(sql, error, e.getFlyweightMessage());
            if (position != -1) {
                Assert.assertEquals(sql, position, e.getPosition());
            }
        }
    }

    private static void assertSameRows(RecordCursorFactory expected, RecordCursorFactory actual, String message) throws SqlException {
        try (
                RecordCursor expectedCursor = expected.getCursor(sqlExecutionContext);
                RecordCursor actualCursor = actual.getCursor(sqlExecutionContext)
        ) {
            TestUtils.assertEquals(expectedCursor, expected.getMetadata(), actualCursor, actual.getMetadata(), false);
        } catch (AssertionError e) {
            throw new AssertionError(message, e);
        }
    }

    /**
     * Creates table t with a row every hour from 2024-01-01T00:00 to 2024-01-04T23:00 and a symbol
     * column, and table ti with the same rows and an indexed symbol column.
     */
    private static void createContextTables() throws SqlException {
        execute("""
                CREATE TABLE t AS (
                    SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts,
                           (CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END)::SYMBOL s
                    FROM long_sequence(96)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("CREATE TABLE ti (ts TIMESTAMP, s SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO ti SELECT ts, s FROM t");
    }

    /**
     * Creates table t with a row every 30 minutes from 2025-01-15T00:00 to 2025-01-27T23:30, 624
     * rows, in the designated timestamp ts, in timestamp ts2 and in DATE d, and v of 1 to 624, table
     * tn with the same rows in a designated nanosecond timestamp, and table tu with them in a table
     * without partitions.
     */
    private static void createHalfHourTables() throws SqlException {
        execute("""
                CREATE TABLE t AS (
                    SELECT timestamp_sequence('2025-01-15T00:00:00.000000Z', 1_800_000_000L) ts,
                           timestamp_sequence('2025-01-15T00:00:00.000000Z', 1_800_000_000L) ts2,
                           timestamp_sequence('2025-01-15T00:00:00.000000Z', 1_800_000_000L)::DATE d,
                           x v
                    FROM long_sequence(624)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("CREATE TABLE tn (ts TIMESTAMP_NS, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO tn SELECT ts::TIMESTAMP_NS, v FROM t");
        execute("CREATE TABLE tu (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY NONE");
        execute("INSERT INTO tu SELECT ts, v FROM t");
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

    /**
     * Creates table m with a row every minute of 2025-01-24 to 2025-01-27, 5,760 rows, in the
     * designated timestamp ts and in timestamp ts2, and v of 1 to 5,760, and table mn with the same rows
     * in a designated nanosecond timestamp.
     */
    private static void createMinuteTables() throws SqlException {
        execute("""
                CREATE TABLE m AS (
                    SELECT timestamp_sequence('2025-01-24T00:00:00.000000Z', 60_000_000L) ts,
                           timestamp_sequence('2025-01-24T00:00:00.000000Z', 60_000_000L) ts2,
                           x v
                    FROM long_sequence(5_760)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("CREATE TABLE mn (ts TIMESTAMP_NS, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO mn SELECT ts::TIMESTAMP_NS, v FROM m");
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

    /**
     * Creates table t of {@link #longRangeTableDdl}.
     */
    private static void createLongRangeTable(String timestampType) throws SqlException {
        execute(longRangeTableDdl(timestampType));
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
     * Returns the DDL of table t, with a row at 09:30 and one at 10:30 of each day from 2022-04-01 to
     * 2026-07-01, 3,106 rows, in a designated timestamp of the type, and a column v of 1 to 3,106.
     */
    private static String longRangeTableDdl(String timestampType) {
        return "CREATE TABLE t AS (SELECT dateadd('h', ((x - 1) % 2)::INT, dateadd('d', ((x - 1) / 2)::INT, '2022-04-01T09:30:00.000000Z'::"
                + timestampType + ")) ts, x v FROM long_sequence(3_106)) TIMESTAMP(ts) PARTITION BY MONTH";
    }

    /**
     * Makes the engine use the test tick calendars, XNYS and XHKG, see {@link TestTickCalendarService}.
     */
    private static void useTickCalendar() {
        factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull TickCalendarServiceFactory getTickCalendarServiceFactory() {
                return () -> TestTickCalendarService.INSTANCE;
            }
        };
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

    /**
     * Asserts the count, first and last timestamp of the rows of the table that the condition
     * selects.
     */
    private void assertCountMinMax(String table, String condition, String expected) throws Exception {
        assertQuery("SELECT count(), min(ts), max(ts) FROM " + table + " WHERE " + condition)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns(expected);
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

    /**
     * Asserts that the tick expression fails the query at its position, in the interval scan and in
     * the row filter, both as a literal and as a bind variable. SQL compiles a literal with a date
     * variable and parses a bind variable, so a malformed expression must fail on both paths rather
     * than select rows on one of them. The table has a row every hour from 2024-01-01 to 2024-01-04,
     * and "now" is 2024-01-02T10:00Z.
     */
    /**
     * Asserts that a query with the tick expression fails when it compiles, before it runs, at the
     * position of the expression: in the interval scan, under NOT IN, in the row filter, on another
     * timestamp column, through a function of the designated timestamp, and below a dateadd() of it.
     */
    private void assertLiteralFailsWhenQueryCompiles(String tickExpr, String expectedError) {
        for (String sql : new String[]{
                "SELECT count() FROM t WHERE ts IN '" + tickExpr + "'",
                "SELECT count() FROM t WHERE ts NOT IN '" + tickExpr + "'",
                "SELECT count() FROM t WHERE ts IN '" + tickExpr + "' OR ts = 0",
                "SELECT count() FROM t WHERE ts2 IN '" + tickExpr + "'",
                "SELECT count() FROM t WHERE dateadd('h', 1, ts) IN '" + tickExpr + "'",
                // the optimiser pushes the condition below the dateadd() as an offset of the scan
                "SELECT count() FROM (SELECT dateadd('h', -1, ts) tt FROM t) WHERE tt IN '" + tickExpr + "'"
        }) {
            try (RecordCursorFactory ignore = select(sql)) {
                Assert.fail("compiled: " + sql);
            } catch (SqlException e) {
                TestUtils.assertContains(sql, e.getFlyweightMessage(), expectedError);
                Assert.assertEquals(sql, sql.indexOf("'" + tickExpr), e.getPosition());
            }
        }
    }

    /**
     * Asserts the 1,001 rows at 09:30 that the tick expression selects from table t of
     * {@link #longRangeTableDdl}, from the first day to the last, in the interval scan, in the row
     * filter with the JIT on and off, through an offset of the designated timestamp, and with a bind
     * variable, and the 2,105 other rows under NOT IN. The fraction of a second has the digits of the
     * timestamp type.
     */
    private void assertLongTickDateRangeRows(String tickExpr, String firstDay, String lastDay, String fraction) throws Exception {
        final String selected = "count\tmin\tmax\n1001\t" + firstDay + "T09:30:00." + fraction + "Z\t" + lastDay + "T09:30:00." + fraction + "Z\n";
        assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN '" + tickExpr + "'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("Interval forward scan")
                .returns(selected);
        final int jitMode = sqlExecutionContext.getJitMode();
        try {
            for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(mode);
                assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN '" + tickExpr + "' OR v = -1")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("filter: (ts in [")
                        .returns(selected);
            }
        } finally {
            sqlExecutionContext.setJitMode(jitMode);
        }
        // the rows outside the ranges, from the first row to the last
        assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts NOT IN '" + tickExpr + "'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\tmin\tmax\n2105\t2022-04-01T09:30:00." + fraction + "Z\t2026-07-01T10:30:00." + fraction + "Z\n");
        // an hour before the rows at 10:30
        assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE dateadd('h', -1, ts) IN '" + tickExpr + "'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\tmin\tmax\n1001\t" + firstDay + "T10:30:00." + fraction + "Z\t" + lastDay + "T10:30:00." + fraction + "Z\n");
        // the parser, which a bind variable takes
        bindVariableService.clear();
        bindVariableService.setStr(0, tickExpr);
        try {
            assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN $1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(selected);
        } finally {
            bindVariableService.clear();
        }
    }

    /**
     * Asserts the count, first and last minute of the rows of the tables of {@link #createMinuteTables}
     * that the literal selects, in the interval scan, in the row filter of an OR with the JIT on and
     * off, on ts2, on the nanosecond timestamp, and an hour earlier through dateadd('h', 1, ts); the
     * other rows under NOT IN; and the count of the rows of its STRING bind variable, which fails on the
     * list where bindCount is -1.
     */
    private void assertMinuteRowsInEveryFilterPath(String tickExpr, int count, String first, String last, int bindCount) throws Exception {
        final String literal = "'" + tickExpr + "'";
        final String rows = count + "\t" + first + ":00.000000Z\t" + last + ":00.000000Z";
        final String expected = "count\tmin\tmax\n" + rows + "\n";
        assertCountMinMax("m", "ts IN " + literal, expected);
        assertCountMinMax("m", "ts2 IN " + literal, expected);
        final int jitMode = sqlExecutionContext.getJitMode();
        try {
            for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(mode);
                assertCountMinMax("m", "ts IN " + literal + " OR v = -1", expected);
            }
        } finally {
            sqlExecutionContext.setJitMode(jitMode);
        }
        assertCountMinMax("m", "ts NOT IN " + literal, "count\tmin\tmax\n" + (5_760 - count) + "\t2025-01-24T00:00:00.000000Z\t2025-01-27T23:59:00.000000Z\n");
        assertCountMinMax("mn", "ts IN " + literal, "count\tmin\tmax\n" + count + "\t" + first + ":00.000000000Z\t" + last + ":00.000000000Z\n");
        final long hour = 3_600_000_000L;
        final StringSink shifted = new StringSink();
        shifted.put("count\tmin\tmax\n").put(count).put('\t');
        MicrosFormatUtils.appendDateTimeUSec(shifted, MicrosFormatUtils.parseTimestamp(first + ":00.000000Z") - hour);
        shifted.put('\t');
        MicrosFormatUtils.appendDateTimeUSec(shifted, MicrosFormatUtils.parseTimestamp(last + ":00.000000Z") - hour);
        shifted.put('\n');
        assertCountMinMax("m", "dateadd('h', 1, ts) IN " + literal, shifted.toString());
        bindVariableService.clear();
        bindVariableService.setStr(0, tickExpr);
        try {
            final String bindSql = "SELECT count() FROM m WHERE ts IN $1";
            if (bindCount < 0) {
                assertExceptionNoLeakCheck(bindSql, bindSql.indexOf("$1"), "Unclosed '[' in date list");
            } else {
                assertQuery(bindSql)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n" + bindCount + "\n");
            }
        } finally {
            bindVariableService.clear();
        }
    }

    private void assertMalformedTickExprFails(String tickExpr, String expectedError) throws Exception {
        assertMemoryLeak(() -> {
            createHourlyTable();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            try {
                assertQueryFails("SELECT count() FROM t WHERE ts IN '" + tickExpr + "'", tickExpr, expectedError);
                assertQueryFails("SELECT count() FROM t WHERE ts IN '" + tickExpr + "' OR ts = 0", tickExpr, expectedError);
                bindVariableService.clear();
                bindVariableService.setStr(0, tickExpr);
                assertQueryFails("SELECT count() FROM t WHERE ts IN $1", tickExpr, expectedError);
                assertQueryFails("SELECT count() FROM t WHERE ts IN $1 OR ts = 0", tickExpr, expectedError);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    private void assertQueryFails(String sql, String tickExpr, String expectedError) throws Exception {
        try {
            assertExceptionNoLeakCheck(sql, 34, expectedError);
        } catch (AssertionError e) {
            throw new AssertionError(sql + " with tick expression " + tickExpr, e);
        }
    }

    /**
     * Compiles a query with the range from 09:30 to "now" at the "now" of the context, and asserts
     * that the compile leaves the compile probe flag down, that the query fails on the range when
     * it runs at 03:00, and that it counts the row at 11:00, with the filter, when it runs at 12:00.
     * It runs on the tables of {@link #createContextTables}.
     */
    private void assertRangeQueryCompiles(String expectedFilter) throws Exception {
        final String sql = "SELECT count() FROM t WHERE ts IN '$today + 570m..$now' AND s = 'a'";
        try (RecordCursorFactory factory = select(sql)) {
            Assert.assertFalse(sqlExecutionContext.isCompileProbe());
            setCurrentMicros(NOW_0300);
            assertRunFails(factory, sql, sql.indexOf('\''), RANGE_ORDER_ERROR);
            setCurrentMicros(NOW_1200);
            assertPlanContains(factory, expectedFilter);
            assertFactory(factory)
                    .withContext(sqlExecutionContext)
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n1\n");
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

    /**
     * Asserts that a query fails at the position of the tick expression wherever it holds it: with
     * the literal as it compiles, and with a bind variable of the bind value as it runs. It runs on
     * the tables of {@link #createContextTables}.
     */
    private void assertTickExprFailsInEveryContext(String literal, String literalError, String bindValue, String bindError) throws Exception {
        // ? stands for the tick expression
        for (String query : new String[]{
                "WITH c AS (SELECT * FROM t WHERE ts IN ?) SELECT count() FROM c",
                "SELECT count() FROM (SELECT * FROM t WHERE ts IN ?)",
                "SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a JOIN t b ON a.s = b.s",
                "SELECT count() FROM t a JOIN (SELECT * FROM t WHERE ts IN ?) b ON a.s = b.s",
                "SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a LEFT JOIN t b ON a.s = b.s",
                "SELECT count() FROM t a LEFT JOIN (SELECT * FROM t WHERE ts IN ?) b ON a.s = b.s",
                "SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a ASOF JOIN t b",
                "SELECT count() FROM t a ASOF JOIN (SELECT * FROM t WHERE ts IN ?) b",
                "SELECT count() FROM (SELECT * FROM t WHERE ts IN ?) a CROSS JOIN t b",
                "SELECT count() FROM t a CROSS JOIN (SELECT * FROM t WHERE ts IN ?) b",
                "SELECT ts FROM t WHERE ts IN ? UNION SELECT ts FROM t",
                "SELECT ts FROM t UNION SELECT ts FROM t WHERE ts IN ?",
                "SELECT ts FROM t WHERE ts IN ? EXCEPT SELECT ts FROM t",
                "SELECT ts FROM t EXCEPT SELECT ts FROM t WHERE ts IN ?",
                "SELECT ts FROM t WHERE ts IN ? INTERSECT SELECT ts FROM t",
                "SELECT ts FROM t INTERSECT SELECT ts FROM t WHERE ts IN ?",
                "SELECT s, count() FROM t WHERE ts IN ? GROUP BY s",
                "SELECT * FROM t WHERE ts IN ? LATEST ON ts PARTITION BY s",
                "SELECT ts, count() FROM t WHERE ts IN ? SAMPLE BY 1h",
                "SELECT ts, row_number() OVER (PARTITION BY s ORDER BY ts) FROM t WHERE ts IN ?",
                "SELECT * FROM t WHERE ts IN ? ORDER BY ts LIMIT -1",
                // a symbol without an index, and one with an index
                "SELECT count() FROM t WHERE s = 'a' AND ts IN ?",
                "SELECT count() FROM ti WHERE s = 'a' AND ts IN ?"
        }) {
            final String literalSql = query.replace("?", "'" + literal + "'");
            assertExceptionNoLeakCheck(literalSql, literalSql.indexOf("'" + literal), literalError);
            final String bindSql = query.replace("?", "$1");
            bindVariableService.clear();
            bindVariableService.setStr(0, bindValue);
            assertExceptionNoLeakCheck(bindSql, bindSql.indexOf("$1"), bindError);
            bindVariableService.clear();
        }
        // EXPLAIN compiles the query without running it
        final String explain = "EXPLAIN SELECT * FROM t WHERE ts IN '" + literal + "'";
        assertExceptionNoLeakCheck(explain, explain.indexOf('\''), literalError);
    }

    /**
     * Asserts that the tick expression selects the given number of rows of the tables of
     * {@link #createHalfHourTables}, from the first minute to the last, and NOT IN the other rows,
     * on the designated timestamp, which takes the interval scan, on another timestamp, on a DATE,
     * on a designated nanosecond timestamp and in a table without partitions, in the row filter of
     * an OR with the JIT on and off, as a literal, and as a bind variable of STRING and of VARCHAR
     * type.
     */
    private void assertTickExprInEveryFilterPath(String tickExpr, int count, String firstMinute, String lastMinute) throws Exception {
        // the column, its table, and the seconds and fraction of a second that it prints
        final String[][] columns = {
                {"ts", "t", ":00.000000Z"},
                {"ts2", "t", ":00.000000Z"},
                {"d", "t", ":00.000Z"},
                {"ts", "tn", ":00.000000000Z"},
                {"ts", "tu", ":00.000000Z"}
        };
        final int jitMode = sqlExecutionContext.getJitMode();
        try {
            for (String[] column : columns) {
                final String in = "count\tmin\tmax\n" + count + '\t'
                        + (count > 0 ? firstMinute + column[2] : "") + '\t'
                        + (count > 0 ? lastMinute + column[2] : "") + '\n';
                final String notIn = "count\tmin\tmax\n" + (624 - count) + "\t2025-01-15T00:00" + column[2] + "\t2025-01-27T23:30" + column[2] + '\n';
                final String select = "SELECT count(), min(" + column[0] + "), max(" + column[0] + ") FROM " + column[1] + " WHERE " + column[0];
                for (int holder = 0; holder < 3; holder++) {
                    final String operand;
                    if (holder == 0) {
                        bindVariableService.clear();
                        operand = "'" + tickExpr + "'";
                    } else {
                        setTickExprBindVariable(tickExpr, holder == 2);
                        operand = "$1";
                    }
                    for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                        sqlExecutionContext.setJitMode(mode);
                        final String context = tickExpr + " in " + column[1] + '.' + column[0] + " [holder=" + holder + ", jitMode=" + mode + ']';
                        for (String[] query : new String[][]{
                                {select + " IN " + operand, in},
                                {select + " IN " + operand + " OR v = -1", in},
                                {select + " NOT IN " + operand, notIn}
                        }) {
                            try {
                                assertQuery(query[0])
                                        .noLeakCheck()
                                        .noRandomAccess()
                                        .expectSize()
                                        .returns(query[1]);
                            } catch (AssertionError e) {
                                throw new AssertionError(context, e);
                            }
                        }
                    }
                }
            }
        } finally {
            bindVariableService.clear();
            sqlExecutionContext.setJitMode(jitMode);
        }
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

    private void setTickExprBindVariable(String tickExpr, boolean isVarchar) throws SqlException {
        bindVariableService.clear();
        if (isVarchar) {
            bindVariableService.setVarchar(0, tickExpr != null ? new Utf8String(tickExpr) : null);
        } else {
            bindVariableService.setStr(0, tickExpr);
        }
    }
}
