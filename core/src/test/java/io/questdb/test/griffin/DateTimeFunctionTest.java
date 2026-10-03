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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DateTimeFunctionTest extends AbstractCairoTest {
    @Test
    public void testCalendarExtractionKeepsBothTimestampPrecisionsAndNulls() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<String> columns = new ObjList<>("ts", "nt");
            {
                final int i = 0;
                final String column = columns.getQuick(i);
                assertQueryRows(
                        "SELECT id,day(" + column + "),day_of_week(" + column + "),day_of_week_sunday_first(" + column
                                + "),days_in_month(" + column + "),hour(" + column + "),is_end_of_month(" + column
                                + "),is_leap_year(" + column + "),micros(" + column + "),millis(" + column + "),minute(" + column
                                + "),month(" + column + "),nanos(" + column + "),second(" + column + "),week_of_year(" + column
                                + "),year(" + column + ") FROM lp_date ORDER BY id",
                        """
                                id	day	day_of_week	day_of_week_sunday_first	days_in_month	hour	is_end_of_month	is_leap_year	micros	millis	minute	month	nanos	second	week_of_year	year
                                1	29	6	7	29	23	true	true	456	123	59	2	0	58	9	2020
                                2	1	5	6	31	0	false	false	1	0	0	1	0	0	53	2021
                                3	31	3	4	31	23	true	false	999	999	59	12	0	59	1	1969
                                4	null	null	null	null	null	false	false	null	null	null	null	null	null	null	null
                                """
                );
            }
            {
                final int i = 1;
                final String column = columns.getQuick(i);
                assertQueryRows(
                        "SELECT id,day(" + column + "),day_of_week(" + column + "),day_of_week_sunday_first(" + column
                                + "),days_in_month(" + column + "),hour(" + column + "),is_end_of_month(" + column
                                + "),is_leap_year(" + column + "),micros(" + column + "),millis(" + column + "),minute(" + column
                                + "),month(" + column + "),nanos(" + column + "),second(" + column + "),week_of_year(" + column
                                + "),year(" + column + ") FROM lp_date ORDER BY id",
                        """
                                id	day	day_of_week	day_of_week_sunday_first	days_in_month	hour	is_end_of_month	is_leap_year	micros	millis	minute	month	nanos	second	week_of_year	year
                                1	29	6	7	29	23	true	false	456	123	59	2	789	58	9	2020
                                2	1	5	6	31	0	false	false	0	0	0	1	1	0	53	2021
                                3	31	3	4	31	23	true	false	999	999	59	12	999	59	1	1969
                                4	null	null	null	null	null	false	false	null	null	null	null	null	null	null	null
                                """
                );
            }
            assertQueryRows(
                    """
                            SELECT id,extract(epoch FROM nt),extract(century FROM ts),extract(decade FROM nt),
                                   extract(dow FROM ts),extract(doy FROM nt),extract(isodow FROM ts),extract(isoyear FROM nt),
                                   extract(microseconds FROM nt),extract(millennium FROM ts),extract(nanoseconds FROM nt),
                                   extract(milliseconds FROM ts),extract(quarter FROM nt),extract(week FROM ts)
                            FROM lp_date ORDER BY id
                            """,
                    """
                            id	extract	extract1	extract2	extract3	extract4	extract5	extract6	extract7	extract8	extract9	extract10	extract11	extract12
                            1	1583020798	21	202	6	60	6	2020	123456	3	123456789	123	1	9
                            2	1609459200	21	202	5	1	5	2020	0	3	1	0	1	53
                            3	0	20	196	3	365	3	1970	999999	2	999999999	999	4	1
                            4	null	null	null	null	null	null	null	null	null	null	null	null	null
                            """
            );
        });
    }

    @Test
    public void testFloorCeilTruncationAndOrigin() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    """
                            SELECT id,timestamp_floor('15m',ts),timestamp_floor('3n',nt),timestamp_ceil('d',ts),
                                   timestamp_ceil('U',nt),date_trunc('microsecond',ts),date_trunc('nanosecond',nt),
                                   date_trunc('quarter',nt),date_trunc('week',ts),date_trunc('century',nt),
                                   timestamp_floor('3h',ts,'2020-02-29T01:00:00.000000Z'),
                                   timestamp_floor('3U',nt,'2020-02-29T01:00:00.000000001Z'),timestamp_floor('5d',nt,null)
                            FROM lp_date ORDER BY id
                            """,
                    """
                            id	timestamp_floor	timestamp_floor1	timestamp_ceil	timestamp_ceil1	date_trunc	date_trunc1	date_trunc2	date_trunc3	date_trunc4	timestamp_floor2	timestamp_floor3	timestamp_floor4
                            1	2020-02-29T23:45:00.000000Z	2020-02-29T23:59:58.123456788Z	2020-03-01T00:00:00.000000Z	2020-02-29T23:59:58.123457000Z	2020-02-29T23:59:58.123456Z	2020-02-29T23:59:58.123456789Z	2020-01-01T00:00:00.000000000Z	2020-02-24T00:00:00.000000Z	2001-01-01T00:00:00.000000000Z	2020-02-29T22:00:00.000000Z	2020-02-29T23:59:58.123455001Z	2020-02-28T00:00:00.000000000Z
                            2	2021-01-01T00:00:00.000000Z	2021-01-01T00:00:00.000000000Z	2021-01-02T00:00:00.000000Z	2021-01-01T00:00:00.000001000Z	2021-01-01T00:00:00.000001Z	2021-01-01T00:00:00.000000001Z	2021-01-01T00:00:00.000000000Z	2020-12-28T00:00:00.000000Z	2001-01-01T00:00:00.000000000Z	2020-12-31T22:00:00.000000Z	2021-01-01T00:00:00.000000001Z	2020-12-29T00:00:00.000000000Z
                            3	1969-12-31T23:45:00.000000Z	1969-12-31T23:59:59.999999997Z	1970-01-01T00:00:00.000000Z	1970-01-01T00:00:00.000000000Z	1969-12-31T23:59:59.999999Z	1969-12-31T23:59:59.999999999Z	1969-10-01T00:00:00.000000000Z	1969-12-29T00:00:00.000000Z	1901-01-01T00:00:00.000000000Z	2020-02-29T01:00:00.000000Z	2020-02-29T01:00:00.000000001Z	1969-12-27T00:00:00.000000000Z
                            4											\t
                            """
            );
            assertQueryRows(
                    "SELECT id FROM lp_date WHERE timestamp_floor('d',ts)='2020-02-29T00:00:00.000000Z' ORDER BY id",
                    """
                            id
                            1
                            """
            );
            assertQueryRows(
                    "SELECT date_trunc('month',ts) bucket,count() FROM lp_date GROUP BY 1 ORDER BY bucket",
                    """
                            bucket	count
                            	1
                            1969-12-01T00:00:00.000000Z	1
                            2020-02-01T00:00:00.000000Z	1
                            2021-01-01T00:00:00.000000Z	1
                            """
            );
        });
    }

    @Test
    public void testDateAddUsesConstantAndRuntimeArguments() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,ts+1L,nt+1L,dateadd('d',1,ts),dateadd('n',3,nt),dateadd('M',stride,ts),dateadd(unit,stride,nt) FROM lp_date ORDER BY id",
                    """
                            id	column	column1	dateadd	dateadd1	dateadd2	dateadd3
                            1	2020-02-29T23:59:58.123457Z	2020-02-29T23:59:58.123456790Z	2020-03-01T23:59:58.123456Z	2020-02-29T23:59:58.123456792Z	2020-03-29T23:59:58.123456Z	2020-03-01T23:59:58.123456789Z
                            2	2021-01-01T00:00:00.000002Z	2021-01-01T00:00:00.000000002Z	2021-01-02T00:00:00.000001Z	2021-01-01T00:00:00.000000004Z	2020-11-01T00:00:00.000001Z	2020-12-31T22:00:00.000000001Z
                            3	1970-01-01T00:00:00.000000Z	1970-01-01T00:00:00.000000000Z	1970-01-01T23:59:59.999999Z	1970-01-01T00:00:00.000000002Z	1970-02-28T23:59:59.999999Z	1970-02-28T23:59:59.999999999Z
                            4					\t
                            """
            );
            bindVariableService.setChar(0, 'h');
            bindVariableService.setInt(1, -2);
            assertQueryRows(
                    "SELECT id,dateadd($1,$2,ts),dateadd($1,$2,nt),dateadd('d',$2,ts) FROM lp_date ORDER BY id",
                    """
                            id	dateadd	dateadd1	dateadd2
                            1	2020-02-29T21:59:58.123456Z	2020-02-29T21:59:58.123456789Z	2020-02-27T23:59:58.123456Z
                            2	2020-12-31T22:00:00.000001Z	2020-12-31T22:00:00.000000001Z	2020-12-30T00:00:00.000001Z
                            3	1969-12-31T21:59:59.999999Z	1969-12-31T21:59:59.999999999Z	1969-12-29T23:59:59.999999Z
                            4		\t
                            """
            );
            assertQueryRows(
                    "SELECT id,dateadd('y',-1,nt) FROM lp_date WHERE year(ts)=2020 ORDER BY id",
                    """
                            id	dateadd
                            1	2019-02-28T23:59:58.123456789Z
                            """
            );
        });
    }

    @Test
    public void testDateDiffConstantAndDynamicPeriodsKeepFullTypesAndNulls() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,datediff('n',ts,nt),datediff('u',nt,ts),datediff('d',ts,nt),datediff(unit,ts,nt) FROM lp_date ORDER BY id",
                    """
                            id	datediff	datediff1	datediff2	datediff3
                            1	789	0	0	0
                            2	999	0	0	0
                            3	999	0	0	0
                            4	null	null	null	null
                            """
            );
            assertQueryRows(
                    "SELECT id,datediff('d',ts,'2021-01-01T00:00:00.000000Z'),"
                            + "datediff('d','2021-01-01T00:00:00.000000001Z',ts),datediff('d',null,nt),datediff('d',ts,null)"
                            + " FROM lp_date ORDER BY id",
                    """
                            id	datediff	datediff1	datediff2	datediff3
                            1	306	306	null	null
                            2	0	0	null	null
                            3	18628	18628	null	null
                            4	null	null	null	null
                            """
            );
            assertQueryRows(
                    "SELECT datediff('n','2020-01-01T00:00:00.000000Z','2020-01-01T00:00:00.000000001Z'),"
                            + "datediff('?',null,null),datediff('?',ts,nt) FROM lp_date ORDER BY id",
                    """
                            datediff	datediff1	datediff2
                            1	\t
                            1	\t
                            1	\t
                            1	\t
                            """
            );
            bindVariableService.setChar(0, 'n');
            assertQueryRows("SELECT id,datediff($1,ts,nt) FROM lp_date ORDER BY id", """
                    id	datediff
                    1	789
                    2	999
                    3	999
                    4	null
                    """);
            bindVariableService.setChar(0, '?');
            assertQueryRows("SELECT id,datediff($1,ts,nt) FROM lp_date ORDER BY id", """
                    id	datediff
                    1	null
                    2	null
                    3	null
                    4	null
                    """);
        });
    }

    @Test
    public void testDateDiffDiscardedNativeArgumentsAndRetainedFactoryLifetime() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String arguments = "CASE WHEN id IN (1,2) THEN ts ELSE ts END,"
                    + "CASE WHEN id IN (2,3) THEN nt ELSE nt END";
            // Invalid constant units fold to typed NULL; both native sets must
            // close even though the surviving expression has no column leaves.
            assertQueryRows(
                    "SELECT id,datediff('?'," + arguments + ") value FROM lp_date ORDER BY id",
                    """
                            id	value
                            1\t
                            2\t
                            3\t
                            4\t
                            """
            );
            final String sql = "SELECT id,datediff(unit," + arguments + ") value FROM lp_date ORDER BY id";
            final String expected = """
                    id	value
                    1	0
                    2	0
                    3	0
                    4	null
                    """;
            RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, expected);
                }
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                try {
                    assertFails(compiler, "SELECT lp_missing_fn(id)+datediff('?'," + arguments + ") FROM lp_date", 7, "unknown function name: lp_missing_fn(INT)");
                    try (RecordCursorFactory factory = compiler.compile(
                            "SELECT datediff('?'," + arguments + ") FROM lp_date", sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        Assert.assertNotNull(factory);
                    }
                    compiler.clear();
                } catch (Throwable th) {
                    retained.close();
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained) {
                assertResult(factory, expected);
            }
        });
    }

    @Test
    public void testDateParsingFormattingAndConstantFolding() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    """
                            SELECT id,to_date(text,'yyyy-MM-dd HH:mm:ss'),to_timestamp(text,'yyyy-MM-dd HH:mm:ss'),
                                   to_timestamp_ns(text,'yyyy-MM-dd HH:mm:ss'),to_timestamp(epoch),to_timestamp_ns(epoch),
                                   to_str(dt,'yyyy-MM-dd HH:mm:ss'),to_str(ts,'yyyy-MM-dd HH:mm:ss.SSSUUU'),
                                   to_str(nt,'yyyy-MM-dd HH:mm:ss.SSSUUUNNN')
                            FROM lp_date ORDER BY id
                            """,
                    """
                            id	to_date	to_timestamp	to_timestamp_ns	to_timestamp1	to_timestamp_ns1	to_str	to_str1	to_str2
                            1	2020-02-29T23:59:58.000Z	2020-02-29T23:59:58.000000Z	2020-02-29T23:59:58.000000000Z	1970-01-01T00:00:00.000123Z	1970-01-01T00:00:00.000000123Z	2020-02-29 00:00:00	2020-02-29 23:59:58.123456	2020-02-29 23:59:58.123456789
                            2	2021-01-01T00:00:00.000Z	2021-01-01T00:00:00.000000Z	2021-01-01T00:00:00.000000000Z	1969-12-31T23:59:59.999999Z	1969-12-31T23:59:59.999999999Z	2021-01-01 00:00:00	2021-01-01 00:00:00.000001	2021-01-01 00:00:00.000000001
                            3						1969-12-31 00:00:00	1969-12-31 23:59:59.999999	1969-12-31 23:59:59.999999999
                            4							\t
                            """
            );
            assertQueryRows(
                    """
                            SELECT year('2020-02-29T23:59:58.123456Z'),date_trunc('nanosecond','2020-02-29T23:59:58.123456789Z'),
                                   to_date('2020-02-29','yyyy-MM-dd'),to_timestamp('123'),to_timestamp_ns('123'),
                                   to_timestamp('2020-02-29','yyyy-MM-dd'),to_timestamp_ns('2020-02-29','yyyy-MM-dd'),
                                   to_str('2020-02-29T00:00:00.000000Z'::timestamp,'yyyy-MM-dd'),
                                   to_str(null::date,'yyyy-MM-dd'),to_timestamp('invalid','yyyy-MM-dd')
                            FROM lp_date LIMIT 1
                            """,
                    """
                            year	date_trunc	to_date	to_timestamp	to_timestamp_ns	to_timestamp1	to_timestamp_ns1	to_str	to_str1	to_timestamp2
                            2020	2020-02-29T23:59:58.123456789Z	2020-02-29T00:00:00.000Z	1970-01-01T00:00:00.000123Z	1970-01-01T00:00:00.000000123Z	2020-02-29T00:00:00.000000Z	2020-02-29T00:00:00.000000000Z	2020-02-29	\t
                            """
            );
            assertQueryRows(
                    "SELECT id,text::timestamp,text::timestamp_ns FROM lp_date ORDER BY id",
                    """
                            id	cast	cast1
                            1	2020-02-29T23:59:58.000000Z	2020-02-29T23:59:58.000000000Z
                            2	2021-01-01T00:00:00.000000Z	2021-01-01T00:00:00.000000000Z
                            3	\t
                            4	\t
                            """
            );
            assertQueryRows(
                    "SELECT id FROM lp_date WHERE to_str(ts,'yyyy-MM-dd')='2020-02-29' ORDER BY id",
                    """
                            id
                            1
                            """
            );
        });
    }

    @Test
    public void testFormattingFactorySurvivesCompilerResetAndFailure() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT id,to_str(dateadd('d',1,nt),'yyyy-MM-dd HH:mm:ss.SSSUUUNNN') value FROM lp_date ORDER BY id";
            final String expected = """
                    id	value
                    1	2020-03-01 23:59:58.123456789
                    2	2021-01-02 00:00:00.000000001
                    3	1970-01-01 23:59:59.999999999
                    4\t
                    """;
            RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, expected);
                }
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                try (RecordCursorFactory ignored = retained) {
                    assertFails(compiler, "SELECT to_str(ts,null) FROM lp_date", 17, "format must not be null");
                    try (RecordCursorFactory factory = compiler.compile("SELECT year(nt) FROM lp_date", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(factory);
                    }
                    assertResult(retained, expected);
                }
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
            }
            try (RecordCursorFactory factory = retained) {
                assertResult(factory, expected);
            }
        });
    }

    @Test
    public void testTimezoneConversionKeepsConstantRuntimeAndRowArguments() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String function = "to_utc";
                assertQueryRows(
                        "SELECT id," + function + "(ts,'Europe/Berlin')," + function + "(nt,'+02:30') FROM lp_date ORDER BY id",
                        """
                                id	to_utc	to_utc1
                                1	2020-02-29T22:59:58.123456Z	2020-02-29T21:29:58.123456789Z
                                2	2020-12-31T23:00:00.000001Z	2020-12-31T21:30:00.000000001Z
                                3	1969-12-31T22:59:59.999999Z	1969-12-31T21:29:59.999999999Z
                                4	294247-01-10T03:07:26.775808Z	2262-04-11T21:17:16.854775808Z
                                """
                );
                assertQueryRows(
                        "SELECT id," + function + "(ts,CASE WHEN id=1 THEN 'Europe/Berlin' ELSE '+02:30' END),"
                                + function + "(nt,text) FROM lp_date ORDER BY id",
                        """
                                id	to_utc	to_utc1
                                1	2020-02-29T22:59:58.123456Z	2020-02-29T03:39:58.123456789Z
                                2	2020-12-31T21:30:00.000001Z	2020-12-31T03:39:00.000000001Z
                                3	1969-12-31T21:29:59.999999Z	1969-12-31T23:59:59.999999999Z
                                4	294247-01-10T01:30:54.775808Z\t
                                """
                );
                bindVariableService.setStr(0, "Pacific/Chatham");
                assertQueryRows(
                        "SELECT id," + function + "(ts,$1)," + function + "(nt,$1) FROM lp_date ORDER BY id",
                        """
                                id	to_utc	to_utc1
                                1	2020-02-29T10:14:58.123456Z	2020-02-29T10:14:58.123456789Z
                                2	2020-12-31T10:15:00.000001Z	2020-12-31T10:15:00.000000001Z
                                3	1969-12-31T11:14:59.999999Z	1969-12-31T11:14:59.999999999Z
                                4	294247-01-09T15:47:06.775808Z	2262-04-11T11:33:28.854775808Z
                                """
                );
                assertQueryRows(
                        "SELECT " + function + "('2024-01-15T12:00:00.000000123Z'::timestamp_ns,'Europe/Berlin') FROM lp_date LIMIT 1",
                        """
                                to_utc
                                2024-01-15T11:00:00.000000123Z
                                """
                );
            }
            {
                final String function = "to_timezone";
                assertQueryRows(
                        "SELECT id," + function + "(ts,'Europe/Berlin')," + function + "(nt,'+02:30') FROM lp_date ORDER BY id",
                        """
                                id	to_timezone	to_timezone1
                                1	2020-03-01T00:59:58.123456Z	2020-03-01T02:29:58.123456789Z
                                2	2021-01-01T01:00:00.000001Z	2021-01-01T02:30:00.000000001Z
                                3	1970-01-01T00:59:59.999999Z	1970-01-01T02:29:59.999999999Z
                                4	-290308-01-01T20:52:33.224192Z	1677-01-01T02:42:43.145224192Z
                                """
                );
                assertQueryRows(
                        "SELECT id," + function + "(ts,CASE WHEN id=1 THEN 'Europe/Berlin' ELSE '+02:30' END),"
                                + function + "(nt,text) FROM lp_date ORDER BY id",
                        """
                                id	to_timezone	to_timezone1
                                1	2020-03-01T00:59:58.123456Z	2020-03-01T20:19:58.123456789Z
                                2	2021-01-01T02:30:00.000001Z	2021-01-01T20:21:00.000000001Z
                                3	1970-01-01T02:29:59.999999Z	1969-12-31T23:59:59.999999999Z
                                4	-290308-01-01T22:29:05.224192Z\t
                                """
                );
                bindVariableService.setStr(0, "Pacific/Chatham");
                assertQueryRows(
                        "SELECT id," + function + "(ts,$1)," + function + "(nt,$1) FROM lp_date ORDER BY id",
                        """
                                id	to_timezone	to_timezone1
                                1	2020-03-01T13:44:58.123456Z	2020-03-01T13:44:58.123456789Z
                                2	2021-01-01T13:45:00.000001Z	2021-01-01T13:45:00.000000001Z
                                3	1970-01-01T12:44:59.999999Z	1970-01-01T12:44:59.999999999Z
                                4	-290308-01-01T08:12:53.224192Z	1677-01-01T12:26:31.145224192Z
                                """
                );
                assertQueryRows(
                        "SELECT " + function + "('2024-01-15T12:00:00.000000123Z'::timestamp_ns,'Europe/Berlin') FROM lp_date LIMIT 1",
                        """
                                to_timezone
                                2024-01-15T13:00:00.000000123Z
                                """
                );
            }
        });
    }

    @Test
    public void testInvalidConstantArgumentsPreserveDiagnosticsAndCloseNativeChildren() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertFailsAndRecovers(compiler, "SELECT timestamp_floor('invalid',ts) FROM lp_date", 23, "invalid unit 'invalid'");
                assertFailsAndRecovers(compiler, "SELECT timestamp_ceil('x',ts) FROM lp_date", 22, "invalid unit 'x'");
                assertFailsAndRecovers(compiler, "SELECT date_trunc('bad',nt) FROM lp_date", 18, "invalid unit 'bad'");
                assertFailsAndRecovers(compiler, "SELECT extract('bad',nt) FROM lp_date", 15, "unsupported part 'bad'");
                assertFailsAndRecovers(compiler, "SELECT dateadd('x',1,ts) FROM lp_date", 15, "invalid time period [unit=x]");
                assertFailsAndRecovers(compiler, "SELECT dateadd('h',null,nt) FROM lp_date", 19, "`null` is not a valid stride");
                assertFailsAndRecovers(compiler, "SELECT to_date(text,null) FROM lp_date", 20, "pattern is required");
                assertFailsAndRecovers(compiler, "SELECT to_timestamp(text,null) FROM lp_date", 25, "pattern is required");
                assertFailsAndRecovers(compiler, "SELECT to_timestamp_ns(text,null) FROM lp_date", 28, "pattern is required");
                assertFailsAndRecovers(compiler, "SELECT to_str(CASE WHEN id IN (1,2) THEN ts ELSE ts END,null) FROM lp_date", 56, "format must not be null");
                assertFailsAndRecovers(compiler, "SELECT to_utc(CASE WHEN id IN (1,2) THEN ts ELSE ts END,'Invalid/Zone') FROM lp_date", 56, "invalid timezone: Invalid/Zone");
                assertFailsAndRecovers(compiler, "SELECT to_timezone(CASE WHEN id IN (1,2) THEN nt ELSE nt END,'Invalid/Zone') FROM lp_date", 61, "invalid timezone: Invalid/Zone");
                assertFailsAndRecovers(compiler, "SELECT to_utc(ts,null) FROM lp_date", 17, "timezone must not be null");
                assertFailsAndRecovers(compiler, "SELECT to_timezone(nt,null) FROM lp_date", 22, "timezone must not be null");
            }
        });
    }

    @Test
    public void testPrecisionSensitiveMonotonicConjunctsCompareExactly() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_date_native(id INT,ts TIMESTAMP,other TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lp_date_native VALUES (1,'2020-01-01','2020-01-01'),(2,'2020-01-02','2020-01-02')");
            final ObjList<String> expressions = new ObjList<>(
                    "date_trunc('microsecond',ts)", "timestamp_floor('d',ts)", "timestamp_ceil('d',ts)",
                    "dateadd('d',0,ts)", "ts+0L"
            );
            final ObjList<String> expected = new ObjList<>("id\n1\n", "id\n1\n", "id\n", "id\n1\n", "id\n1\n");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int i = 0; i < expressions.size(); i++) {
                    final String query = "SELECT id FROM lp_date_native WHERE " + expressions.getQuick(i)
                            + "<'2020-01-01T00:00:00.000000001Z' ORDER BY id";
                    try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(factory, expected.getQuick(i));
                    }
                    try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(factory, expected.getQuick(i));
                    }
                }
            }
            {
                final int i = 0;
                assertQueryRows(
                        "SELECT id FROM lp_date_native WHERE " + expressions.getQuick(i) + "!='2020-01-01T00:00:00.000000001Z' ORDER BY id",
                        """
                                id
                                1
                                2
                                """
                );
                assertQueryRows(
                        "SELECT id FROM lp_date_native WHERE " + expressions.getQuick(i) + "<'2020-01-01T00:00:00.000000001Z' OR id=2 ORDER BY id",
                        """
                                id
                                1
                                2
                                """
                );
            }
            {
                final int i = 1;
                assertQueryRows(
                        "SELECT id FROM lp_date_native WHERE " + expressions.getQuick(i) + "!='2020-01-01T00:00:00.000000001Z' ORDER BY id",
                        """
                                id
                                1
                                2
                                """
                );
                assertQueryRows(
                        "SELECT id FROM lp_date_native WHERE " + expressions.getQuick(i) + "<'2020-01-01T00:00:00.000000001Z' OR id=2 ORDER BY id",
                        """
                                id
                                1
                                2
                                """
                );
            }
            {
                final int i = 2;
                assertQueryRows(
                        "SELECT id FROM lp_date_native WHERE " + expressions.getQuick(i) + "!='2020-01-01T00:00:00.000000001Z' ORDER BY id",
                        """
                                id
                                1
                                2
                                """
                );
                assertQueryRows(
                        "SELECT id FROM lp_date_native WHERE " + expressions.getQuick(i) + "<'2020-01-01T00:00:00.000000001Z' OR id=2 ORDER BY id",
                        """
                                id
                                2
                                """
                );
            }
            {
                final int i = 3;
                assertQueryRows(
                        "SELECT id FROM lp_date_native WHERE " + expressions.getQuick(i) + "!='2020-01-01T00:00:00.000000001Z' ORDER BY id",
                        """
                                id
                                1
                                2
                                """
                );
                assertQueryRows(
                        "SELECT id FROM lp_date_native WHERE " + expressions.getQuick(i) + "<'2020-01-01T00:00:00.000000001Z' OR id=2 ORDER BY id",
                        """
                                id
                                1
                                2
                                """
                );
            }
            {
                final int i = 4;
                assertQueryRows(
                        "SELECT id FROM lp_date_native WHERE " + expressions.getQuick(i) + "!='2020-01-01T00:00:00.000000001Z' ORDER BY id",
                        """
                                id
                                1
                                2
                                """
                );
                assertQueryRows(
                        "SELECT id FROM lp_date_native WHERE " + expressions.getQuick(i) + "<'2020-01-01T00:00:00.000000001Z' OR id=2 ORDER BY id",
                        """
                                id
                                1
                                2
                                """
                );
            }
            assertQueryRows(
                    "SELECT id FROM lp_date_native WHERE timestamp_floor('d',ts)<'2020-01-02T00:00:00.000000Z' ORDER BY id",
                    """
                            id
                            1
                            """
            );
            assertQueryRows(
                    "SELECT id FROM lp_date_native WHERE timestamp_floor('d',other)<'2020-01-01T00:00:00.000000001Z' ORDER BY id",
                    """
                            id
                            1
                            """
            );
            assertQueryRows(
                    "SELECT timestamp_floor('d',ts)<'2020-01-01T00:00:00.000000001Z' value FROM lp_date_native ORDER BY id",
                    """
                            value
                            true
                            false
                            """
            );
            assertQueryRows(
                    "SELECT id FROM (SELECT id,ts FROM lp_date_native LIMIT 1) WHERE timestamp_floor('d',ts)<'2020-01-01T00:00:00.000000001Z' ORDER BY id",
                    """
                            id
                            1
                            """
            );
        });
    }

    private static void assertFails(SqlCompilerImpl compiler, String sql, int position, String message) throws Exception {
        try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.fail(sql);
        } catch (SqlException e) {
            Assert.assertEquals(sql, position, e.getPosition());
            TestUtils.assertEquals(message, e.getFlyweightMessage());
        }
    }

    private static void assertFailsAndRecovers(SqlCompilerImpl compiler, String sql, int position, String message) throws Exception {
        assertFails(compiler, sql, position, message);
        try (RecordCursorFactory factory = compiler.compile("SELECT year(ts) FROM lp_date", sqlExecutionContext).getRecordCursorFactory()) {
            Assert.assertNotNull(factory);
        }
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_date(unused INT,id INT,ts TIMESTAMP,nt TIMESTAMP_NS,dt DATE,text STRING,epoch STRING,unit CHAR,stride INT)");
        execute("""
                INSERT INTO lp_date VALUES
                (91,1,'2020-02-29T23:59:58.123456Z','2020-02-29T23:59:58.123456789Z','2020-02-29','2020-02-29 23:59:58','123','d',1),
                (92,2,'2021-01-01T00:00:00.000001Z','2021-01-01T00:00:00.000000001Z','2021-01-01','2021-01-01 00:00:00','-1','h',-2),
                (93,3,'1969-12-31T23:59:59.999999Z','1969-12-31T23:59:59.999999999Z','1969-12-31','invalid','invalid','M',2),
                (94,4,null,null,null,null,null,'d',1)
                """);
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
