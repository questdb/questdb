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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVarTuple;
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

    private static void createHourlyTable() throws SqlException {
        // hourly rows from 2024-01-01T00:00 to 2024-01-04T23:00
        execute("""
                CREATE TABLE t AS (
                    SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts
                    FROM long_sequence(96)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
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
        // the OR keeps the IN in the row filter
        assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE ts IN '" + tickExpr + "' OR ts = 0")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("filter: ")
                .returns(expected);
    }
}
