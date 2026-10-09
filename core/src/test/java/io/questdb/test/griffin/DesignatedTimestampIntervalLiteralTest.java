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

package io.questdb.test.griffin;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class DesignatedTimestampIntervalLiteralTest extends AbstractCairoTest {

    @Test
    public void testDateOperandsReportParserError() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE xd (d DATE, ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO xd VALUES ('2014-01-02T12:30:00.000Z', '2014-01-02T12:30:00.000Z', 1)");
            assertError("SELECT * FROM xd WHERE d IN ('2014', 'abc')", 37, "Invalid date [str=abc]");
            assertError("SELECT * FROM xd WHERE d IN ('abc', 'def')", 29, "Invalid date [str=abc]");
            assertError("SELECT * FROM xd WHERE d IN ('abc')", 29, "Invalid date: abc");
            assertError("SELECT * FROM xd WHERE d = 'abc'", 27, "Invalid date [str=abc]");
            assertError("SELECT * FROM xd WHERE d BETWEEN 'abc' AND '2014'", 33, "Invalid date [str=abc]");
            assertError("SELECT d IN ('2014', 'abc') FROM xd", 21, "Invalid date [str=abc]");
            assertQuery("SELECT * FROM xd WHERE d IN ('2014-01-02T12:30:00.000Z', '2015')").noLeakCheck().timestamp("ts").returns("""
                    d\tts\tv
                    2014-01-02T12:30:00.000Z\t2014-01-02T12:30:00.000000Z\t1
                    """);
        });
    }

    @Test
    public void testDesignatedBetweenAndInListReportBareInvalidDate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM x WHERE ts BETWEEN 'abc' AND '2014-01-02T12:30:00.000Z'", 33, "Invalid date");
            assertError("SELECT * FROM x WHERE ts BETWEEN '2014-01-02T12:30:00.000Z' AND 'abc'", 64, "Invalid date");
            assertError("SELECT * FROM x WHERE ts BETWEEN 'abc' AND 'def'", 33, "Invalid date");
            assertError("SELECT * FROM x WHERE ts NOT BETWEEN '' AND '2014-01-02T12:30:00.000Z'", 37, "Invalid date");
            assertError("SELECT * FROM xn WHERE ts BETWEEN 'abc' AND '2014-01-02T12:30:00.000Z'", 34, "Invalid date");
            assertError("SELECT * FROM x WHERE ts IN ('2014-01-02T12:30:00.000Z', 'abc')", 57, "Invalid date");
            assertError("SELECT * FROM x WHERE ts IN ('abc', 'def')", 36, "Invalid date");
            assertError("SELECT * FROM x WHERE ts IN ('2014', 'def', 'abc')", 44, "Invalid date");
            assertError("SELECT * FROM x WHERE ts NOT IN ('2014', 'def')", 41, "Invalid date");
            assertError("SELECT * FROM x WHERE NOT (ts IN ('2014-01-02T12:30:00.000Z', '1583077401000000'))", 62, "Invalid date");
            assertError("SELECT * FROM xn WHERE ts IN ('abc', '2014-01-02T12:30:00.000Z')", 30, "Invalid date");
            assertError("SELECT * FROM x WHERE ts = 'abc' OR ts = '2014-01-02T12:30:00.000Z'", 27, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts IN ('2014') OR ts = 'abc'", 45, "invalid timestamp");
            assertError("SELECT * FROM xn WHERE ts = 'abc' OR ts = '2014-01-02T12:30:00.000Z'", 28, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts = now() OR ts = 'abc'", 41, "invalid timestamp");
        });
    }

    @Test
    public void testDesignatedComputedTextReportsNotADate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM x WHERE ts = '2015-02-23T10:00:55.000Z;30m'::varchar", 57, "Not a date, use IN keyword with intervals");
            assertError("SELECT * FROM x WHERE ts != '2015;1h'::varchar", 37, "Not a date, use IN keyword with intervals");
            assertError("SELECT * FROM x WHERE ts = 'abc'::varchar", 32, "Invalid date [str=abc]");
            assertError("SELECT * FROM x WHERE ts < '2015-02-23T10:00:55.000Z;30m'::varchar", 57, "Invalid date [str=2015-02-23T10:00:55.000Z;30m]");
            assertError("SELECT * FROM x WHERE ts BETWEEN 'abc'::varchar AND '2014-01-02T12:30:00.000Z'", 38, "Invalid date [str=abc]");
            assertError("SELECT * FROM x WHERE ts IN ('2014-01-02T12:30:00.000Z', 'abc'::varchar)", 62, "Invalid date [str=abc]");
            assertError("SELECT * FROM x WHERE ts = cast('2015;1h' as string)", 27, "Not a date, use IN keyword with intervals");
            assertError("SELECT * FROM x WHERE ts = '2015' || ';1h'", 34, "Not a date, use IN keyword with intervals");
            assertError("SELECT * FROM x WHERE ts < '2015' || ';1h'", 34, "Invalid date [str=2015;1h]");
            assertError("SELECT * FROM xn WHERE ts = 'abc'::varchar", 33, "Invalid date [str=abc]");
        });
    }

    @Test
    public void testDesignatedEqualityReportsTimestampErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM x WHERE ts = 'abc'", 27, "invalid timestamp");
            assertError("SELECT * FROM x WHERE 'abc' = ts", 22, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts = ''", 27, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts = '1583077401000000'", 27, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts = '2015-02-23T10:00:55.000Z;30m'", 27, "not a timestamp, use IN keyword with intervals");
            assertError("SELECT * FROM x WHERE '2014-03-01T12:30:00.000Z;x' = ts", 22, "not a timestamp, use IN keyword with intervals");
            assertError("SELECT * FROM xn WHERE ts = 'abc'", 28, "invalid timestamp");
            assertError("SELECT * FROM xn WHERE ts = '2015-02-23T10:00:55.000Z;30m'", 28, "not a timestamp, use IN keyword with intervals");
            assertError("SELECT * FROM x WHERE NOT (ts != 'abc')", 33, "invalid timestamp");
        });
    }

    @Test
    public void testDesignatedExclusionReportsEqualityErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM x WHERE ts != 'abc'", 28, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts != ''", 28, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts <> '2015-02-23T10:00:55.000Z;30m'", 28, "not a timestamp, use IN keyword with intervals");
            assertError("SELECT * FROM x WHERE '2014-03-01T12:30:00.000Z;x' != ts", 22, "not a timestamp, use IN keyword with intervals");
            assertError("SELECT * FROM xn WHERE ts != 'abc'", 29, "invalid timestamp");
            assertError("SELECT * FROM x WHERE NOT (ts = 'abc')", 32, "invalid timestamp");
        });
    }

    @Test
    public void testDesignatedExclusionSubtractsPoint() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String row = """
                    ts\tts2\tv
                    2014-01-02T12:30:00.000000Z\t2014-01-02T12:30:00.000000Z\t1
                    """;
            assertQuery("SELECT * FROM x WHERE ts != '2014-01-02T12'").noLeakCheck().timestamp("ts").returns(row);
            assertQuery("SELECT * FROM x WHERE ts <> '2014-01-02T12'").noLeakCheck().timestamp("ts").returns(row);
            assertQuery("SELECT * FROM x WHERE '2014-01-02T12' != ts").noLeakCheck().timestamp("ts").returns(row);
            assertQuery("SELECT * FROM x WHERE ts != '2014-01-02T12:30'").noLeakCheck().timestamp("ts").returns("ts\tts2\tv\n");
            assertQuery("SELECT * FROM x WHERE NOT (ts = '2014-01-02T12:30')").noLeakCheck().timestamp("ts").returns("ts\tts2\tv\n");
            assertQuery("SELECT * FROM xn WHERE ts != '2014-01-02T12:30'").noLeakCheck().timestamp("ts").returns("ts\tts2\tv\n");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE b.ts != '2014-01-02T12:30'").noLeakCheck()
                    .noRandomAccess().returns("ts\tts2\tv\tts1\tts21\tv1\n");
            assertQuery("SELECT * FROM x WHERE ts != '2014-01-02T12:30'").noLeakCheck().assertsPlan("""
                    PageFrame
                        Row forward scan
                        Interval forward scan on: x
                          intervals: [("MIN","2014-01-02T12:29:59.999999Z"),("2014-01-02T12:30:00.000001Z","MAX")]
                    """);
            assertQuery("SELECT ts FROM (SELECT ts, ts = 'abc' b FROM x)").noLeakCheck().fails(32, "invalid timestamp");
        });
    }

    @Test
    public void testDesignatedRangeReportsQuotedLiteral() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM x WHERE ts < 'abc'", 27, "Invalid date [str='abc']");
            assertError("SELECT * FROM x WHERE ts >= ''", 28, "Invalid date [str='']");
            assertError("SELECT * FROM x WHERE '2015-02-23T10:00:55.000Z;30m' > ts", 22, "Invalid date [str='2015-02-23T10:00:55.000Z;30m']");
            assertError("SELECT * FROM x WHERE NOT (ts < 'abc')", 32, "Invalid date [str='abc']");
            assertError("SELECT * FROM xn WHERE ts <= 'abc'", 29, "Invalid date [str='abc']");
            assertError("SELECT max(ts) FROM x WHERE ts < 'abc'", 33, "Invalid date [str='abc']");
        });
    }

    @Test
    public void testDesignatedSymbolText() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM x WHERE ts = 'abc'::symbol", 32, "Invalid date [str=abc]");
            assertError("SELECT * FROM x WHERE ts != 'abc'::symbol", 33, "Invalid date [str=abc]");
            assertError("SELECT * FROM x WHERE ts = '2015;1h'::symbol", 36, "Not a date, use IN keyword with intervals");
            assertError("SELECT * FROM x WHERE ts < 'abc'::symbol", 32, "Invalid date [str=abc]");
            assertError("SELECT * FROM x WHERE ts IN ('2014', 'abc'::symbol)", 42, "Invalid date [str=abc]");
            assertError("SELECT * FROM xn WHERE ts = 'abc'::symbol", 33, "Invalid date [str=abc]");
            assertError("SELECT * FROM x WHERE ts2 < 'abc'::symbol", 33, "Invalid date [str=abc]");
        });
    }

    @Test
    public void testDesignatedUpdateAndSymbolCast() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertExceptionNoLeakCheck("UPDATE x SET v = 2 WHERE ts = 'abc'", 30, "invalid timestamp");
            assertExceptionNoLeakCheck("UPDATE x SET v = 2 WHERE ts < 'abc'", 30, "Invalid date [str='abc']");
            assertExceptionNoLeakCheck("UPDATE x SET v = 2 WHERE ts2 = 'abc'", 31, "invalid timestamp");
            assertExceptionNoLeakCheck("UPDATE x SET v = 2 WHERE ts != '2014-01-02T12:00;1h'", 31, "not a timestamp, use IN keyword with intervals");
            execute("UPDATE x SET v = 2 WHERE ts != '2014-01-02T12:30'");
            assertQuery("SELECT * FROM x WHERE ts2 = 'abc'::symbol").noLeakCheck().fails(33, "Invalid date [str=abc]");
            assertQuery("SELECT * FROM xn WHERE ts2 = 'abc'::symbol").noLeakCheck().fails(34, "Invalid date [str=abc]");
            assertQuery("SELECT * FROM x WHERE ts = '2014-01-02T12:30:00.000Z'::symbol").noLeakCheck().timestamp("ts").returns("""
                    ts\tts2\tv
                    2014-01-02T12:30:00.000000Z\t2014-01-02T12:30:00.000000Z\t1
                    """);
        });
    }

    @Test
    public void testErrorOrderAcrossConjuncts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM x WHERE ts = 'abc' AND ts = '2015-02-23T10:00:55.000Z;30m'", 27, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts = '2015-02-23T10:00:55.000Z;30m' AND ts = 'abc'", 27, "not a timestamp, use IN keyword with intervals");
            assertError("SELECT * FROM x WHERE ts < 'abc' AND ts > 'def'", 27, "Invalid date [str='abc']");
            assertError("SELECT * FROM x WHERE ts2 = 'def' AND ts = 'abc'", 28, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts = 'abc' AND nocol = 1", 27, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts = 'def' AND ts IN ('2014', 'abc')", 27, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts IN ('2014', 'abc') AND ts = 'def'", 37, "Invalid date");
            assertError("SELECT * FROM x WHERE ts = 'abc' AND v = 'z'", 27, "invalid timestamp");
            assertError("SELECT * FROM x WHERE 1 = 2 AND ts = 'abc'", 37, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts = 'abc' AND false", 27, "invalid timestamp");
            assertError("SELECT ts = 'abc', nocol FROM x", 12, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts2 = 'abc' AND nocol = 1", 28, "invalid timestamp");
        });
    }

    @Test
    public void testInEpochLiteralIsInvalidDate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM x WHERE ts IN ('1583077401000000')").noLeakCheck().fails(29, "Invalid date: 1583077401000000");
            assertQuery("SELECT * FROM x WHERE ts IN '1583077401000000'").noLeakCheck().fails(28, "Invalid date: 1583077401000000");
            assertQuery("SELECT * FROM x WHERE ts NOT IN '1583077401000000'").noLeakCheck().fails(32, "Invalid date: 1583077401000000");
            assertQuery("SELECT * FROM x WHERE ts IN ('123')").noLeakCheck().fails(29, "Invalid date: 123");
            assertQuery("SELECT * FROM x WHERE ts IN ('1583077401000000') AND v = 1").noLeakCheck().fails(29, "Invalid date: 1583077401000000");
            assertQuery("SELECT * FROM x WHERE ts IN '2014' OR ts IN ('1583077401000000')").noLeakCheck().fails(45, "Invalid date: 1583077401000000");
            assertQuery("SELECT * FROM (SELECT * FROM x) WHERE ts IN ('1583077401000000')").noLeakCheck().fails(45, "Invalid date: 1583077401000000");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE b.ts IN ('1583077401000000')").noLeakCheck().fails(55, "Invalid date: 1583077401000000");
            assertQuery("SELECT ts, count() FROM x WHERE ts IN ('1583077401000000') SAMPLE BY 1h").noLeakCheck().fails(39, "Invalid date: 1583077401000000");
            assertQuery("SELECT * FROM xn WHERE ts IN ('1583077401000000')").noLeakCheck().fails(30, "Invalid date: 1583077401000000");
            assertQuery("SELECT * FROM x WHERE ts2 IN ('1583077401000000')").noLeakCheck().fails(30, "Invalid date: 1583077401000000");
            assertQuery("SELECT * FROM (SELECT * FROM x LIMIT 10) WHERE ts IN '1583077401000000'").noLeakCheck().fails(53, "Invalid date: 1583077401000000");
            assertQuery("SELECT * FROM xn WHERE ts NOT IN ('123')").noLeakCheck().fails(34, "Invalid date: 123");
        });
    }

    @Test
    public void testInEpochTextOutsideIntervalLiteralIsEpoch() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM x WHERE ts IN '1583077401000000'::varchar")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\tts2\tv\n");
            assertQuery("SELECT * FROM x WHERE ts IN ('2014')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tts2\tv
                            2014-01-02T12:30:00.000000Z\t2014-01-02T12:30:00.000000Z\t1
                            """);
        });
    }

    @Test
    public void testNonDesignatedColumnsReportCanonicalErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM x WHERE ts2 = 'abc'", 28, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts2 != '2014-01-02T12:00;1h'", 29, "not a timestamp, use IN keyword with intervals");
            assertQuery("SELECT * FROM x WHERE ts2 != '2014-01-02T12:30'").noLeakCheck().timestamp("ts").returns("ts\tts2\tv\n");
            assertError("SELECT * FROM x WHERE ts2 IN ('abc', 'def')", 37, "Invalid date");
            assertError("SELECT * FROM x WHERE ts2 NOT IN ('2014', 'def')", 42, "Invalid date");
            assertError("SELECT * FROM x WHERE ts2 BETWEEN 'abc' AND 'def'", 34, "Invalid date");
            assertError("SELECT * FROM x WHERE ts2 = 'abc' OR ts2 = 'def'", 43, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts2 = 'abc' AND ts2 = 'def'", 28, "invalid timestamp");
            assertError("SELECT * FROM x WHERE (ts2 = 'abc' OR ts2 = 'def') AND (ts2 = 'ghi' OR v = 1)", 44, "invalid timestamp");
            assertError("SELECT ts2 = 'abc' OR ts2 = 'def' FROM x", 28, "invalid timestamp");
            assertError("SELECT * FROM xn WHERE ts2 = 'abc'", 29, "invalid timestamp");
        });
    }

    @Test
    public void testNonPredicateContextsReportCanonicalErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT ts IN ('2014', 'zz'), timestamp_floor('xx', ts) FROM x", 22, "Invalid date");
            assertError("SELECT timestamp_floor('xx', ts), ts IN ('2014', 'zz') FROM x", 23, "invalid unit 'xx'");
            assertError("SELECT ts = 'zz', timestamp_floor('xx', ts) FROM x", 12, "invalid timestamp");
            assertError("SELECT ts BETWEEN 'zz' AND '2014', timestamp_floor('xx', ts) FROM x", 18, "Invalid date");
            assertError("SELECT ts2 IN ('2014', 'zz'), timestamp_floor('xx', ts) FROM xn", 23, "Invalid date");
            assertError("SELECT * FROM x ORDER BY ts = 'zz', timestamp_floor('xx', ts)", 30, "invalid timestamp");
            assertError("SELECT * FROM x ORDER BY ts IN ('2014', 'zz'), timestamp_floor('xx', ts)", 40, "Invalid date");
            assertError("SELECT * FROM (SELECT ts IN ('2014', 'zz') k, timestamp_floor('xx', ts) FROM x) WHERE k", 37, "Invalid date");
            assertError("SELECT ts, count() FROM x SAMPLE BY 1h FROM 'zz' TO '2014-01-03' ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'", 44, "Invalid date [str=zz]");
            assertError("SELECT ts, count() FROM x SAMPLE BY 1h FROM '2014-01-01' TO 'zz' ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'", 60, "Invalid date [str=zz]");
            assertExceptionNoLeakCheck("UPDATE x SET v = (ts = 'zz')::int, ts2 = timestamp_floor('xx', ts)", 23, "invalid timestamp");
            assertExceptionNoLeakCheck("UPDATE x SET v = (ts IN ('2014', 'zz'))::int", 33, "Invalid date");
            assertExceptionNoLeakCheck("UPDATE x SET ts2 = timestamp_floor('xx', ts), v = (ts = 'zz')::int", 35, "invalid unit 'xx'");
        });
    }

    @Test
    public void testRangeBoundCastToFloatIsInvalidDate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM x WHERE ts > v::float", 28, "Invalid date");
            assertError("SELECT * FROM xn WHERE ts > v::float", 29, "Invalid date");
        });
    }

    @Test
    public void testRangeBoundComputedDoubleIsInvalidDate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM x WHERE ts > v * 1.5", 29, "Invalid date");
            assertError("SELECT * FROM xn WHERE ts < sqrt(v)", 28, "Invalid date");
        });
    }

    @Test
    public void testRangeBoundInLatestByIsInvalidDate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM x WHERE ts > v * 1.5 LATEST ON ts PARTITION BY v", 29, "Invalid date");
        });
    }

    @Test
    public void testRangeBoundInPostingDistinctIsInvalidDate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE xp (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO xp VALUES ('2014-01-02T12:30:00.000Z', 'a', 1)");
            assertError("SELECT DISTINCT sym FROM xp WHERE ts > v * 1.5", 41, "Invalid date");
        });
    }

    @Test
    public void testRangeBoundInSubqueryIsInvalidDate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM x WHERE v = (SELECT max(v) FROM x y WHERE y.ts > y.v * 1.5)", 67, "Invalid date");
        });
    }

    @Test
    public void testRangeBoundLong256IsInvalidDate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM x WHERE ts > v::long256", 28, "Invalid date");
        });
    }

    @Test
    public void testRangeBoundNegativeDoubleLiteralIsInvalidDate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM x WHERE ts <= -1.5", 28, "Invalid date");
        });
    }

    @Test
    public void testResidualContextsReportCanonicalErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM (SELECT * FROM x LIMIT 1) WHERE ts = 'abc'", 51, "invalid timestamp");
            assertError("SELECT * FROM (SELECT * FROM x LIMIT 1) WHERE ts != '2014-01-02T12:00;1h'", 52, "not a timestamp, use IN keyword with intervals");
            assertQuery("SELECT * FROM (SELECT * FROM x LIMIT 1) WHERE ts != '2014-01-02T12:30'").noLeakCheck().timestamp("ts").returns("ts\tts2\tv\n");
            assertError("SELECT * FROM (SELECT * FROM x LIMIT 1) WHERE ts IN ('2014', 'abc')", 61, "Invalid date");
            assertError("SELECT * FROM (SELECT * FROM x LIMIT 1) WHERE ts BETWEEN 'abc' AND '2014'", 57, "Invalid date");
            assertError("SELECT * FROM x a LEFT JOIN x b ON a.v = b.v WHERE b.ts = 'abc'", 58, "invalid timestamp");
            assertError("SELECT * FROM x a ASOF JOIN x b WHERE b.ts < 'abc'", 45, "Invalid date [str='abc']");
            assertError("SELECT * FROM x a SPLICE JOIN x b WHERE b.ts = 'abc'", 47, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts = 'abc' OR v = 1", 27, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE ts != '2014-01-02T12:30' OR v = 1").noLeakCheck().timestamp("ts").returns("""
                    ts\tts2\tv
                    2014-01-02T12:30:00.000000Z\t2014-01-02T12:30:00.000000Z\t1
                    """);
            assertError("SELECT * FROM x WHERE CASE WHEN v = 1 THEN ts = 'abc' ELSE false END", 48, "invalid timestamp");
            assertError("SELECT ts = 'abc' FROM x", 12, "invalid timestamp");
            assertError("SELECT ts IN ('2014', 'abc') FROM x", 22, "Invalid date");
            assertError("SELECT * FROM x WHERE dateadd('h', 1, ts) = 'abc'", 44, "invalid timestamp");
            assertError("SELECT * FROM x WHERE now() = 'abc'", 30, "invalid timestamp");
            assertError("SELECT * FROM x WHERE '2014'::timestamp = 'abc'", 42, "invalid timestamp");
            assertError("SELECT * FROM xn a LEFT JOIN xn b ON a.v = b.v WHERE b.ts = 'abc'", 60, "invalid timestamp");
            assertError("EXPLAIN SELECT * FROM x WHERE ts = 'abc' OR v = 1", 35, "invalid timestamp");
        });
    }

    @Test
    public void testScanContextsReportIntervalErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertError("SELECT * FROM (SELECT * FROM x) WHERE ts = 'abc'", 43, "invalid timestamp");
            assertError("SELECT * FROM (SELECT ts t, v FROM x) WHERE t = 'abc'", 48, "invalid timestamp");
            assertError("SELECT * FROM (SELECT ts, count() FROM x) WHERE ts = 'abc'", 53, "invalid timestamp");
            assertError("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE b.ts = 'abc'", 53, "invalid timestamp");
            assertError("SELECT * FROM x a JOIN x b ON a.v = b.v AND b.ts = 'abc'", 51, "invalid timestamp");
            assertError("SELECT * FROM x a LEFT JOIN x b ON a.v = b.v WHERE a.ts = 'abc'", 58, "invalid timestamp");
            assertError("SELECT * FROM x UNION ALL SELECT * FROM x WHERE ts = 'abc'", 53, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts = 'abc' LATEST ON ts PARTITION BY v", 27, "invalid timestamp");
            assertError("SELECT ts, count() FROM x WHERE ts = 'abc' SAMPLE BY 1h", 37, "invalid timestamp");
            assertError("SELECT * FROM x WHERE EXISTS (SELECT 1 FROM x y WHERE y.ts = 'abc')", 61, "invalid timestamp");
            assertError("SELECT * FROM x WHERE v = (SELECT max(v) FROM x y WHERE y.ts = 'abc')", 63, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts = 'abc' LIMIT -1", 27, "invalid timestamp");
            assertError("EXPLAIN SELECT * FROM x WHERE ts = 'abc'", 35, "invalid timestamp");
        });
    }

    @Test
    public void testSymbolCastReportsBinderOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM x WHERE ts2 = 'abc'::symbol OR ts2 = 'def'::symbol").noLeakCheck()
                    .fails(56, "Invalid date [str=def]");
            assertQuery("SELECT * FROM x WHERE ts2 != 'abc'::symbol OR ts2 != 'def'::symbol").noLeakCheck()
                    .fails(58, "Invalid date [str=def]");
            assertQuery("SELECT * FROM x WHERE ts2 = 'abc'::symbol AND ts2 = 'def'::symbol").noLeakCheck()
                    .fails(33, "Invalid date [str=abc]");
            assertQuery("SELECT ts2 = 'abc'::symbol OR ts2 = 'def'::symbol FROM x").noLeakCheck()
                    .fails(41, "Invalid date [str=def]");
            assertQuery("SELECT * FROM xn WHERE ts2 = 'abc'::symbol OR ts2 = 'def'::symbol").noLeakCheck()
                    .fails(57, "Invalid date [str=def]");
            assertQuery("SELECT * FROM x WHERE ts2 = 'abc'::symbol AND false").noLeakCheck()
                    .fails(33, "Invalid date [str=abc]");
            assertQuery("SELECT * FROM x WHERE ts = 'abc'::symbol OR ts = '2014-01-02T12:30:00.000Z'").noLeakCheck()
                    .fails(32, "Invalid date [str=abc]");
            assertError("SELECT * FROM x WHERE ts2 = 'def'::symbol OR ts2 = 'abc'", 51, "invalid timestamp");
            assertError("SELECT * FROM x WHERE ts = 'abc'::symbol AND false", 32, "Invalid date [str=abc]");
        });
    }

    private static void createTables() throws Exception {
        execute("CREATE TABLE x (ts TIMESTAMP, ts2 TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE xn (ts TIMESTAMP_NS, ts2 TIMESTAMP_NS, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO x VALUES ('2014-01-02T12:30:00.000Z', '2014-01-02T12:30:00.000Z', 1)");
        execute("INSERT INTO xn VALUES ('2014-01-02T12:30:00.000Z', '2014-01-02T12:30:00.000Z', 1)");
    }

    private void assertError(String sql, int position, String message) throws Exception {
        assertQuery(sql).noLeakCheck().fails(position, message);
        try {
            printSql(sql);
            Assert.fail("SQL statement should have failed");
        } catch (SqlException e) {
            Assert.assertEquals(message, e.getFlyweightMessage().toString());
        }
    }
}
