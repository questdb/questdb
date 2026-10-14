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
import io.questdb.std.ObjList;
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
    public void testIntervalMustBeOnlyListElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT 0::timestamp ts, 0::timestamp_ns tn FROM long_sequence(1))");
            String sql = "SELECT ('2020-01-01T00:00:00.000000000Z'::timestamp_ns) in (null::varchar, "
                    + "interval('2020-01-01T00:00:00.000000Z'::timestamp, '2020-01-02T00:00:00.000000Z'::timestamp), "
                    + "1.5::decimal(4,1)) FROM t";
            assertQuery(sql).fails(sql.indexOf("interval("), "cannot compare TIMESTAMP with type INTERVAL");

            assertQuery("SELECT tn IN (interval(0, 1), null) FROM t")
                    .fails(14, "cannot compare TIMESTAMP with type INTERVAL");
            assertQuery("SELECT tn IN (ts, interval(0, 1)) FROM t")
                    .fails(18, "cannot compare TIMESTAMP with type INTERVAL");
            bindVariableService.setTimestamp(0, 0);
            assertQuery("SELECT tn IN ($1, interval(0, 1)) FROM t")
                    .fails(18, "cannot compare TIMESTAMP with type INTERVAL");

            assertQuery("SELECT ts IN interval(0, 1) micro, tn IN interval(0, 1) nano FROM t")
                    .expectSize()
                    .returns("micro\tnano\ntrue\ttrue\n");
            assertQuery("SELECT tn IN (null, null::varchar, ts, $1) matched, tn IN null::interval empty FROM t")
                    .expectSize()
                    .returns("matched\tempty\ntrue\tfalse\n");
            assertQuery("SELECT null::timestamp IN (null, null::timestamp, null::date) matched")
                    .expectSize()
                    .returns("matched\ntrue\n");
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
                .fails(48, "Invalid date");
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
    public void testPrunedPointListKeepsBindVariableSemantics() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id INT, ts TIMESTAMP)");
            execute("""
                    INSERT INTO test VALUES
                        (1, '2024-01-01T00:00:00'),
                        (2, '2024-01-01T12:00:00'),
                        (3, '2024-01-02T00:00:00'),
                        (4, NULL)
                    """);
            final ObjList<BindVarTuple> cases = new ObjList<>();
            final String firstDay = """
                    id\tmatched\tnot_matched
                    1\ttrue\tfalse
                    2\tfalse\ttrue
                    3\tfalse\ttrue
                    4\tfalse\ttrue
                    """;
            final String nullValue = """
                    id\tmatched\tnot_matched
                    1\tfalse\ttrue
                    2\tfalse\ttrue
                    3\tfalse\ttrue
                    4\ttrue\tfalse
                    """;
            cases.add(BindVarTuple.ok("string day", firstDay, service -> service.setStr(0, "2024-01-01")));
            cases.add(BindVarTuple.ok("varchar day", firstDay, service -> service.setVarchar(0, new Utf8String("2024-01-01"))));
            cases.add(BindVarTuple.ok("null string", nullValue, service -> service.setStr(0, null)));
            cases.add(BindVarTuple.ok("null varchar", nullValue, service -> service.setVarchar(0, null)));
            cases.add(BindVarTuple.ok("second day", """
                            id\tmatched\tnot_matched
                            1\tfalse\ttrue
                            2\tfalse\ttrue
                            3\ttrue\tfalse
                            4\tfalse\ttrue
                            """,
                    service -> service.setStr(0, "2024-01-02")));
            for (int type = 0; type < 2; type++) {
                final String parameter = type == 0 ? "$1" : "$1::VARCHAR";
                for (int constantType = 0; constantType < 3; constantType++) {
                    final String suffix = switch (constantType) {
                        case 0 -> "::TIMESTAMP_NS";
                        case 1 -> "::VARCHAR";
                        default -> "";
                    };
                    final String list = parameter + ", '2024-01-01T00:00:00.000000001'" + suffix;
                    assertQuery("SELECT id, ts IN (" + list + ") AS matched, ts NOT IN (" + list + ") AS not_matched FROM test ORDER BY id")
                            .noLeakCheck()
                            .expectSize()
                            .assertBinds(cases);
                }
            }
        });
    }

    @Test
    public void testPrunedPointListKeepsConstantTextSemantics() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id INT, ts TIMESTAMP)");
            execute("""
                    INSERT INTO test VALUES
                        (1, '2024-01-01T00:00:00'),
                        (2, '2024-01-01T12:00:00'),
                        (3, '2024-01-02T00:00:00'),
                        (4, NULL)
                    """);
            execute("CREATE TABLE designated AS (SELECT * FROM test WHERE ts IS NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            final ObjList<String> values = new ObjList<>(
                    "'2024-01-01'",
                    "'2024-01-01'::VARCHAR",
                    "concat('2024-01-', '01')",
                    "concat('2024-01-', '01')::VARCHAR",
                    "NULL::STRING",
                    "NULL::VARCHAR",
                    "'2024-01-01;1d'",
                    "'2024-01-01;1d'::VARCHAR",
                    "concat('2024-01-', '02')",
                    "concat('2024-01-', '02')::VARCHAR"
            );
            for (int value = 0; value < values.size(); value++) {
                final boolean isNull = value == 4 || value == 5;
                final boolean isInterval = value == 6 || value == 7;
                final boolean isSecondDay = value >= 8;
                final String expected = "id\tmatched\tnot_matched\n"
                        + "1\t" + !(isNull || isSecondDay) + '\t' + (isNull || isSecondDay) + '\n'
                        + "2\t" + isInterval + '\t' + !isInterval + '\n'
                        + "3\t" + isSecondDay + '\t' + !isSecondDay + '\n'
                        + "4\t" + isNull + '\t' + !isNull + '\n';
                for (int order = 0; order < 2; order++) {
                    final String impossible = "'2024-01-01T00:00:00.000000001'";
                    final String list = order == 0 ? values.getQuick(value) + ", " + impossible : impossible + ", " + values.getQuick(value);
                    assertQuery("SELECT id, ts IN (" + list + ") AS matched, ts NOT IN (" + list + ") AS not_matched FROM test ORDER BY id")
                            .noLeakCheck()
                            .expectSize()
                            .returns(expected);
                    assertQuery("SELECT id FROM test WHERE ts IN (" + list + ") ORDER BY id")
                            .noLeakCheck()
                            .sizeMayVary()
                            .returns(isNull ? "id\n4\n" : isInterval ? "id\n1\n2\n" : isSecondDay ? "id\n3\n" : "id\n1\n");
                    assertQuery("SELECT id FROM designated WHERE ts IN (" + list + ") ORDER BY id")
                            .noLeakCheck()
                            .sizeMayVary()
                            .returns(isNull ? "id\n" : isInterval ? "id\n1\n2\n" : isSecondDay ? "id\n3\n" : "id\n1\n");
                }
            }
        });
    }

    @Test
    public void testPrunedPointListKeepsVariableTextSemantics() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id INT, ts TIMESTAMP, s STRING, v VARCHAR)");
            execute("""
                    INSERT INTO test VALUES
                        (1, '2024-01-01T00:00:00', '2024-01-01', '2024-01-01'),
                        (2, '2024-01-01T12:00:00', '2024-01-01', '2024-01-01'),
                        (3, '2024-01-02T00:00:00', '2024-01-01', '2024-01-01'),
                        (4, NULL, NULL, NULL)
                    """);
            final int oldJitMode = sqlExecutionContext.getJitMode();
            final boolean isParallelFilterEnabled = sqlExecutionContext.isParallelFilterEnabled();
            try {
                for (int mode = 0; mode < 3; mode++) {
                    sqlExecutionContext.setParallelFilterEnabled(mode > 0);
                    sqlExecutionContext.setJitMode(mode == 2 ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                    for (int type = 0; type < 2; type++) {
                        final String column = type == 0 ? "s" : "v";
                        for (int constantType = 0; constantType < 3; constantType++) {
                            final String suffix = switch (constantType) {
                                case 0 -> "::TIMESTAMP_NS";
                                case 1 -> "::VARCHAR";
                                default -> "";
                            };
                            final String impossible = "'2024-01-01T00:00:00.000000001'" + suffix;
                            for (int order = 0; order < 3; order++) {
                                final String list = switch (order) {
                                    case 0 -> column + ", " + impossible;
                                    case 1 -> impossible + ", " + column;
                                    default -> impossible + ", " + column + ", " + impossible + ", " + impossible;
                                };
                                assertQuery("SELECT id, ts IN (" + list + ") AS matched, ts NOT IN (" + list + ") AS not_matched FROM test ORDER BY id")
                                        .noLeakCheck()
                                        .expectSize()
                                        .returns("""
                                                id\tmatched\tnot_matched
                                                1\ttrue\tfalse
                                                2\tfalse\ttrue
                                                3\tfalse\ttrue
                                                4\ttrue\tfalse
                                                """);
                                assertQuery("SELECT id FROM test WHERE ts IN (" + list + ") ORDER BY id")
                                        .noLeakCheck()
                                        .returns("id\n1\n4\n");
                                assertQuery("SELECT id FROM test WHERE ts NOT IN (" + list + ") ORDER BY id")
                                        .noLeakCheck()
                                        .returns("id\n2\n3\n");
                            }
                        }
                        assertQuery("SELECT id FROM test WHERE ts IN (" + column + ") ORDER BY id")
                                .noLeakCheck()
                                .returns("id\n1\n2\n");
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(oldJitMode);
                sqlExecutionContext.setParallelFilterEnabled(isParallelFilterEnabled);
            }
        });
    }

    @Test
    public void testStatefulTextElementEvaluatedOncePerRow() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    x
                    1
                    2
                    3
                    4
                    """;
            assertQuery("SELECT x FROM long_sequence(4) WHERE (x - 1)::timestamp IN (timestamp_sequence(0, 1)::varchar, 100::timestamp)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("SELECT x FROM long_sequence(4) WHERE (x - 1)::timestamp IN (timestamp_sequence(0, 1)::string, 100::timestamp)")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testTypedNullTextListElements() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id INT, dt DATE, ts TIMESTAMP, tn TIMESTAMP_NS)");
            execute("INSERT INTO test VALUES (1, NULL, NULL, NULL), (2, 0, 0, 0), (3, 1, 1, 1)");
            final ObjList<String> columns = new ObjList<>("dt", "ts", "tn");
            final ObjList<String> types = new ObjList<>("STRING", "VARCHAR", "SYMBOL");
            for (int column = 0; column < columns.size(); column++) {
                for (int type = 0; type < types.size(); type++) {
                    for (int order = 0; order < 2; order++) {
                        final String nullValue = "NULL::" + types.getQuick(type);
                        final String list = order == 0 ? nullValue + ", 0" : "0, " + nullValue;
                        final String value = columns.getQuick(column);
                        assertQuery("SELECT id, " + value + " IN (" + list + ") matched, "
                                + value + " NOT IN (" + list + ") not_matched FROM test ORDER BY id")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        id\tmatched\tnot_matched
                                        1\ttrue\tfalse
                                        2\ttrue\tfalse
                                        3\tfalse\ttrue
                                        """);
                        final String invalidList = order == 0 ? nullValue + ", NULL::INTERVAL" : "NULL::INTERVAL, " + nullValue;
                        final String sql = "SELECT " + value + " IN (" + invalidList + ") FROM test";
                        assertQuery(sql).fails(sql.indexOf("::INTERVAL"), "cannot compare TIMESTAMP with type INTERVAL");
                    }
                }
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
}
