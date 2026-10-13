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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.BindVarTuple;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class SampleByLinearTimezoneTest extends AbstractCairoTest {
    private final boolean isCursor;
    private final boolean isKeyed;
    private final TestTimestampType timestampType;

    public SampleByLinearTimezoneTest(TestTimestampType timestampType, boolean isKeyed, boolean isCursor) {
        this.timestampType = timestampType;
        this.isKeyed = isKeyed;
        this.isCursor = isCursor;
    }

    @Parameterized.Parameters(name = "{0}, keyed={1}, cursor={2}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO, false, false},
                {TestTimestampType.MICRO, false, true},
                {TestTimestampType.MICRO, true, false},
                {TestTimestampType.MICRO, true, true},
                {TestTimestampType.NANO, false, false},
                {TestTimestampType.NANO, false, true},
                {TestTimestampType.NANO, true, false},
                {TestTimestampType.NANO, true, true}
        });
    }

    @Test
    public void testApiaSkippedDay() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('2011-12-29T12:00:00', 'A', 0),
                    ('2011-12-31T12:00:00', 'A', 48)
                    """);
            assertSample("1d FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'Pacific/Apia'", """
                    ts\tsum
                    2011-12-29T10:00:00.000000Z\t0.0
                    2011-12-30T10:00:00.000000Z\t24.0
                    2011-12-31T10:00:00.000000Z\t48.0
                    """);
        });
    }

    @Test
    public void testBerlinOffsetChangesAggregateMembership() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('1970-01-03T02:00:00', 'A', 10),
                    ('1970-01-03T03:00:00', 'A', 20),
                    ('1970-01-03T05:00:00', 'A', 30)
                    """);
            final String expected = """
                    ts\tsum
                    1970-01-02T23:45:00.000000Z\t10.0
                    1970-01-03T02:45:00.000000Z\t50.0
                    """;
            final String clause = "3h %s ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '00:45'";
            assertSample(clause.formatted("FILL(NONE)"), expected);
            assertSample(clause.formatted("FILL(LINEAR)"), expected);
        });
    }

    @Test
    public void testDailyFallBackGap() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('2026-10-23T22:30:00', 'A', 0),
                    ('2026-10-26T23:30:00', 'A', 73)
                    """);
            assertSample("1d FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'", """
                    ts\tsum
                    2026-10-23T22:00:00.000000Z\t0.0
                    2026-10-24T22:00:00.000000Z\tnull
                    2026-10-25T23:00:00.000000Z\tnull
                    2026-10-26T23:00:00.000000Z\t73.0
                    """);
            assertSample("1d FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'", """
                    ts\tsum
                    2026-10-23T22:00:00.000000Z\t0.0
                    2026-10-24T22:00:00.000000Z\t24.0
                    2026-10-25T23:00:00.000000Z\t49.0
                    2026-10-26T23:00:00.000000Z\t73.0
                    """);
        });
    }

    @Test
    public void testDailyFallBackOffsetInOverlap() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('2026-10-24T03:00:00', 'A', 10),
                    ('2026-10-25T00:45:00', 'A', 1),
                    ('2026-10-25T01:15:00', 'A', 2),
                    ('2026-10-25T01:45:00', 'A', 3),
                    ('2026-10-25T03:00:00', 'A', 20),
                    ('2026-10-26T03:00:00', 'A', 30)
                    """);
            assertSample("1d FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '02:30'", """
                    ts\tsum
                    2026-10-24T00:30:00.000000Z\t10.0
                    2026-10-25T00:30:00.000000Z\t26.0
                    2026-10-26T01:30:00.000000Z\t30.0
                    """);
        });
    }

    @Test
    public void testDailyFallBackTwoPointInterpolation() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('2026-10-23T22:00:00', 'A', 0),
                    ('2026-10-26T23:00:00', 'A', 1)
                    """);
            assertDistanceSample(73, """
                    ts\tsum
                    2026-10-23T22:00:00.000000Z\t24
                    2026-10-24T22:00:00.000000Z\t25
                    2026-10-25T23:00:00.000000Z\t24
                    2026-10-26T23:00:00.000000Z\t0
                    """);
        });
    }

    @Test
    public void testDailySpringForwardFromIgnoresOffset() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('2026-03-28T22:30:00', 'A', 0),
                    ('2026-03-30T22:30:00', 'A', 71)
                    """);
            assertSample("1d FROM '2026-03-28' TO '2026-04-01' FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '06:00'", """
                    ts\tsum
                    2026-03-27T23:00:00.000000Z\t0.0
                    2026-03-28T23:00:00.000000Z\t24.0
                    2026-03-29T22:00:00.000000Z\t47.0
                    2026-03-30T22:00:00.000000Z\t71.0
                    """);
        });
    }

    @Test
    public void testDailySpringForwardGap() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('2026-03-28T22:30:00', 'A', 0),
                    ('2026-03-30T22:30:00', 'A', 71)
                    """);
            assertSample("1d FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'", """
                    ts\tsum
                    2026-03-27T23:00:00.000000Z\t0.0
                    2026-03-28T23:00:00.000000Z\tnull
                    2026-03-29T22:00:00.000000Z\tnull
                    2026-03-30T22:00:00.000000Z\t71.0
                    """);
            final String expected = """
                    ts\tsum
                    2026-03-27T23:00:00.000000Z\t0.0
                    2026-03-28T23:00:00.000000Z\t24.0
                    2026-03-29T22:00:00.000000Z\t47.0
                    2026-03-30T22:00:00.000000Z\t71.0
                    """;
            assertSample("1d FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'", expected);
            assertSample("1d FROM '2026-03-28' TO '2026-04-01' FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'", expected);
        });
    }

    @Test
    public void testDailySpringForwardOffsetExtrapolation() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('2026-03-28T03:00:00', 'A', 0),
                    ('2026-03-29T03:00:00', 'A', 24),
                    ('2026-03-30T03:00:00', 'B', 47),
                    ('2026-03-31T03:00:00', 'B', 71)
                    """);
            String expected = isKeyed ? """
                    ts\tsym\tsum
                    2026-03-28T01:30:00.000000Z\tA\t0.0
                    2026-03-28T01:30:00.000000Z\tB\t0.0
                    2026-03-29T01:30:00.000000Z\tA\t24.0
                    2026-03-29T01:30:00.000000Z\tB\t24.0
                    2026-03-30T00:30:00.000000Z\tB\t47.0
                    2026-03-30T00:30:00.000000Z\tA\t47.0
                    2026-03-31T00:30:00.000000Z\tB\t71.0
                    2026-03-31T00:30:00.000000Z\tA\t71.0
                    """ : """
                    ts\tsum
                    2026-03-28T01:30:00.000000Z\t0.0
                    2026-03-29T01:30:00.000000Z\t24.0
                    2026-03-30T00:30:00.000000Z\t47.0
                    2026-03-31T00:30:00.000000Z\t71.0
                    """;
            if (timestampType == TestTimestampType.NANO) {
                expected = expected.replace(".000000Z", ".000000000Z");
            }
            assertQuery(query("1d FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '02:30'"))
                    .noLeakCheck().timestamp("ts").expectSize().returns(expected);
        });
    }

    @Test
    public void testDailySpringForwardOffsetInGap() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('2026-03-28T03:00:00', 'A', 10),
                    ('2026-03-29T01:15:00', 'A', 2),
                    ('2026-03-29T01:45:00', 'A', 3),
                    ('2026-03-29T03:00:00', 'A', 20),
                    ('2026-03-30T03:00:00', 'A', 30)
                    """);
            assertSample("1d FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '02:30'", """
                    ts\tsum
                    2026-03-28T01:30:00.000000Z\t12.0
                    2026-03-29T01:30:00.000000Z\t23.0
                    2026-03-30T00:30:00.000000Z\t30.0
                    """);
        });
    }

    @Test
    public void testDailySpringForwardTwoPointInterpolation() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('2026-03-27T23:00:00', 'A', 0),
                    ('2026-03-30T22:00:00', 'A', 1)
                    """);
            assertDistanceSample(71, """
                    ts\tsum
                    2026-03-27T23:00:00.000000Z\t24
                    2026-03-28T23:00:00.000000Z\t23
                    2026-03-29T22:00:00.000000Z\t24
                    2026-03-30T22:00:00.000000Z\t0
                    """);
        });
    }

    @Test
    public void testFromIgnoresOffset() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('2026-07-01T00:05:00', 'A', 10),
                    ('2026-07-01T00:20:00', 'A', 20),
                    ('2026-07-01T01:20:00', 'A', 30)
                    """);
            final String clause = "1h FROM '2026-07-01T02:00:00' TO '2026-07-01T05:00:00' %s ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '00:15'";
            final String expected = """
                    ts\tsum
                    2026-07-01T00:00:00.000000Z\t30.0
                    2026-07-01T01:00:00.000000Z\t30.0
                    """;
            if (isCursor) {
                assertSample(clause.formatted("FILL(NONE)"), expected);
            }
            assertSample(clause.formatted("FILL(LINEAR)"), expected);
        });
    }

    @Test
    public void testHourlySpringForwardKeepsUtcStride() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('2026-03-28T22:00:00', 'A', 10),
                    ('2026-03-29T03:00:00', 'A', 60)
                    """);
            assertSample("1h FROM '2026-03-28T23:00:00' TO '2026-03-29T06:00:00' FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'", """
                    ts\tsum
                    2026-03-28T22:00:00.000000Z\t10.0
                    2026-03-28T23:00:00.000000Z\t20.0
                    2026-03-29T00:00:00.000000Z\t30.0
                    2026-03-29T01:00:00.000000Z\t40.0
                    2026-03-29T02:00:00.000000Z\t50.0
                    2026-03-29T03:00:00.000000Z\t60.0
                    """);
        });
    }

    @Test
    public void testNamedTimezoneAtNanosecondLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts " + timestampType.getTypeName() + ", sym SYMBOL, v DOUBLE)");
            execute("""
                    INSERT INTO t VALUES
                    ('2262-04-09T09:00:00', 'A', 0),
                    ('2262-04-11T09:00:00', 'A', 48)
                    """);
            assertQuery(query("1d FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'America/Phoenix'").replace("FROM t SAMPLE BY", "FROM (SELECT * FROM t) TIMESTAMP(ts) SAMPLE BY"))
                    .noLeakCheck().timestamp("ts").expectSize().returns(expected("""
                            ts\tsum
                            2262-04-09T07:00:00.000000Z\t0.0
                            2262-04-10T07:00:00.000000Z\t24.0
                            2262-04-11T07:00:00.000000Z\t48.0
                            """));
            assertQuery(query("1d FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'America/Phoenix' WITH OFFSET '18:00'").replace("FROM t SAMPLE BY", "FROM (SELECT * FROM t) TIMESTAMP(ts) SAMPLE BY"))
                    .noLeakCheck().timestamp("ts").expectSize().returns(expected("""
                            ts\tsum
                            2262-04-09T01:00:00.000000Z\t0.0
                            2262-04-10T01:00:00.000000Z\t24.0
                            2262-04-11T01:00:00.000000Z\t48.0
                            """));
        });
    }

    @Test
    public void testNamedTimezoneAtNanosecondLimitWithDstRules() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts " + timestampType.getTypeName() + ", sym SYMBOL, v DOUBLE)");
            execute("""
                    INSERT INTO t VALUES
                    ('2262-04-09T06:00:00', 'A', 0),
                    ('2262-04-11T06:00:00', 'A', 48)
                    """);
            assertQuery(query("1d FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'America/New_York'").replace("FROM t SAMPLE BY", "FROM (SELECT * FROM t) TIMESTAMP(ts) SAMPLE BY"))
                    .noLeakCheck().timestamp("ts").expectSize().returns(expected("""
                            ts\tsum
                            2262-04-09T04:00:00.000000Z\t0.0
                            2262-04-10T04:00:00.000000Z\t24.0
                            2262-04-11T04:00:00.000000Z\t48.0
                            """));
        });
    }

    @Test
    public void testNumericTimezoneAtNanosecondLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts " + timestampType.getTypeName() + ", sym SYMBOL, v DOUBLE)");
            execute("""
                    INSERT INTO t VALUES
                    ('2262-04-09T06:00:00', 'A', 0),
                    ('2262-04-11T06:00:00', 'A', 48)
                    """);
            assertQuery(query("1d FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE '-02:00'").replace("FROM t SAMPLE BY", "FROM (SELECT * FROM t) TIMESTAMP(ts) SAMPLE BY"))
                    .noLeakCheck().timestamp("ts").expectSize().returns(expected("""
                            ts\tsum
                            2262-04-09T02:00:00.000000Z\t0.0
                            2262-04-10T02:00:00.000000Z\t24.0
                            2262-04-11T02:00:00.000000Z\t48.0
                            """));
        });
    }

    @Test
    public void testNumericTimezoneGap() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('2026-01-01T00:10:00', 'A', 10),
                    ('2026-01-01T01:10:00', 'A', 20),
                    ('2026-01-01T03:10:00', 'A', 40)
                    """);
            assertSample("1h FILL(NULL) ALIGN TO CALENDAR TIME ZONE '+05:30'", """
                    ts\tsum
                    2025-12-31T23:30:00.000000Z\t10.0
                    2026-01-01T00:30:00.000000Z\t20.0
                    2026-01-01T01:30:00.000000Z\tnull
                    2026-01-01T02:30:00.000000Z\t40.0
                    """);
            assertSample("1h FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE '+05:30'", """
                    ts\tsum
                    2025-12-31T23:30:00.000000Z\t10.0
                    2026-01-01T00:30:00.000000Z\t20.0
                    2026-01-01T01:30:00.000000Z\t30.0
                    2026-01-01T02:30:00.000000Z\t40.0
                    """);
        });
    }

    @Test
    public void testSubDayFromTimezoneBindVariableReopen() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('2026-01-01T06:20:00', 'A', 10),
                    ('2026-01-01T08:20:00', 'A', 30)
                    """);
            final String shifted = expected("""
                    ts\tsum
                    2026-01-01T05:45:00.000000Z\t10.0
                    2026-01-01T06:45:00.000000Z\t20.0
                    2026-01-01T07:45:00.000000Z\t30.0
                    """);
            final String utc = expected("""
                    ts\tsum
                    2026-01-01T06:15:00.000000Z\t10.0
                    2026-01-01T07:15:00.000000Z\t20.0
                    2026-01-01T08:15:00.000000Z\t30.0
                    """);
            final ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok("+05:30", shifted, binds -> binds.setStr(0, "+05:30")));
            cases.add(BindVarTuple.ok("UTC", utc, binds -> binds.setStr(0, "UTC")));
            cases.add(BindVarTuple.ok("+05:30 again", shifted, binds -> binds.setStr(0, "+05:30")));
            assertQuery(query("1h FROM '2026-01-01T05:15:00' TO '2026-01-02' FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE $1"))
                    .noLeakCheck().timestamp("ts").expectSize().assertBinds(cases);
        });
    }

    @Test
    public void testTimezoneBindVariableReopen() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("""
                    INSERT INTO t VALUES
                    ('2026-03-28T22:30:00', 'A', 10),
                    ('2026-03-28T23:30:00', 'A', 20),
                    ('2026-03-29T22:30:00', 'A', 30),
                    ('2026-03-30T22:30:00', 'A', 40)
                    """);
            final String berlin = expected("""
                    ts\tsum
                    2026-03-27T23:00:00.000000Z\t10.0
                    2026-03-28T23:00:00.000000Z\t20.0
                    2026-03-29T22:00:00.000000Z\t30.0
                    2026-03-30T22:00:00.000000Z\t40.0
                    """);
            final String utc = expected("""
                    ts\tsum
                    2026-03-28T00:00:00.000000Z\t30.0
                    2026-03-29T00:00:00.000000Z\t30.0
                    2026-03-30T00:00:00.000000Z\t40.0
                    """);
            final ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok("Berlin", berlin, binds -> binds.setStr(0, "Europe/Berlin")));
            cases.add(BindVarTuple.ok("+05:30", expected("""
                    ts\tsum
                    2026-03-28T18:30:00.000000Z\t30.0
                    2026-03-29T18:30:00.000000Z\t30.0
                    2026-03-30T18:30:00.000000Z\t40.0
                    """), binds -> binds.setStr(0, "+05:30")));
            cases.add(BindVarTuple.ok("UTC", utc, binds -> binds.setStr(0, "UTC")));
            cases.add(BindVarTuple.ok("null", utc, binds -> binds.setStr(0, (CharSequence) null)));
            cases.add(BindVarTuple.ok("Berlin again", berlin, binds -> binds.setStr(0, "Europe/Berlin")));
            assertQuery(query("1d FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE $1"))
                    .noLeakCheck().timestamp("ts").expectSize().assertBinds(cases);
        });
    }

    private void assertDistanceSample(int totalHours, String expected) throws Exception {
        assertQuery(query("1d FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'")
                .replace("sum(v)", "cast(round(haversine_dist_deg(0.0, v, ts) * " + totalHours + " / (6371.088 * pi() / 180.0), 0) as long) sum"))
                .noLeakCheck().timestamp("ts").expectSize().returns(expected(expected));
    }

    private void assertSample(String clause, String expected) throws Exception {
        final QueryAssertion assertion = assertQuery(query(clause)).noLeakCheck().timestamp("ts");
        if (clause.contains("FILL(LINEAR)")) {
            assertion.expectSize();
        } else {
            assertion.inferRandomAccess().sizeMayVary();
        }
        assertion.returns(expected(expected));
    }

    private void createTable() throws Exception {
        execute("CREATE TABLE t (ts " + timestampType.getTypeName() + ", sym SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
    }

    private String expected(String expected) {
        if (timestampType == TestTimestampType.NANO) {
            expected = expected.replace(".000000Z", ".000000000Z");
        }
        return isKeyed ? expected.replace("ts\tsum", "ts\tsym\tsum").replace("Z\t", "Z\tA\t") : expected;
    }

    private String query(String clause) {
        return "SELECT ts, " + (isKeyed ? "sym, " : "") + "sum(v) FROM "
                + (isCursor ? "(SELECT * FROM t LIMIT 100) TIMESTAMP(ts)" : "t") + " SAMPLE BY " + clause;
    }
}
