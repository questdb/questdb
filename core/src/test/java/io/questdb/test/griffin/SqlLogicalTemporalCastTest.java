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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalTemporalCastTest extends AbstractCairoTest {
    private static final ObjList<String> DATE_TARGETS = new ObjList<>(
            "BOOLEAN", "BYTE", "CHAR", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "STRING", "VARCHAR", "TIMESTAMP"
    );
    private static final ObjList<String> TIMESTAMP_TARGETS = new ObjList<>(
            "BOOLEAN", "BYTE", "CHAR", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DATE", "STRING", "VARCHAR"
    );

    @Test
    public void testColumnCastMatrixPreservesBothTimestampPrecisions() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            int count = 0;
            final ObjList<String> primitives = new ObjList<>("b", "i8", "c", "i16", "i32", "i64", "f32", "f64");
            final ObjList<String> nanoRows = new ObjList<>(
                    """
                    id	v0
                    1	1970-01-01T00:00:00.000000001Z
                    2	1970-01-01T00:00:00.000000000Z
                    3	1970-01-01T00:00:00.000000001Z
                    4	1970-01-01T00:00:00.000000000Z
                    """,
                    """
                    id	v0
                    1	1970-01-01T00:00:00.000000001Z
                    2	1969-12-31T23:59:59.999999999Z
                    3	1970-01-01T00:00:00.000000000Z
                    4	1970-01-01T00:00:00.000000127Z
                    """,
                    """
                    id	v0
                    1	1970-01-01T00:00:00.000000001Z
                    2	1970-01-01T00:00:00.000000009Z
                    3	1970-01-01T00:00:00.000000000Z
                    4	1970-01-01T00:00:00.000000001Z
                    """,
                    """
                    id	v0
                    1	1970-01-01T00:00:00.000000257Z
                    2	1969-12-31T23:59:59.999999743Z
                    3	1970-01-01T00:00:00.000000000Z
                    4	1970-01-01T00:00:00.000032767Z
                    """,
                    """
                    id	v0
                    1	1970-01-01T00:00:00.000065537Z
                    2	1969-12-31T23:59:59.999934463Z
                    3\t
                    4	1970-01-01T00:00:02.147483647Z
                    """,
                    """
                    id	v0
                    1	1970-01-01T00:00:00.000001234Z
                    2	1969-12-31T23:59:59.999998766Z
                    3\t
                    4	1970-01-01T02:33:43.372036854Z
                    """,
                    """
                    id	v0
                    1	1970-01-01T00:00:00.000000001Z
                    2	1969-12-31T23:59:59.999999999Z
                    3\t
                    4\t
                    """,
                    """
                    id	v0
                    1	1970-01-01T00:00:00.000000002Z
                    2	1969-12-31T23:59:59.999999998Z
                    3\t
                    4\t
                    """
            );
            final ObjList<String> dateTimestampRows = new ObjList<>(
                    """
                    id	v0	v1
                    1	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.000001Z
                    2	1970-01-01T00:00:00.000Z	1970-01-01T00:00:00.000000Z
                    3	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.000001Z
                    4	1970-01-01T00:00:00.000Z	1970-01-01T00:00:00.000000Z
                    """,
                    """
                    id	v0	v1
                    1	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.000001Z
                    2	1969-12-31T23:59:59.999Z	1969-12-31T23:59:59.999999Z
                    3	1970-01-01T00:00:00.000Z	1970-01-01T00:00:00.000000Z
                    4	1970-01-01T00:00:00.127Z	1970-01-01T00:00:00.000127Z
                    """,
                    """
                    id	v0	v1
                    1	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.000001Z
                    2	1970-01-01T00:00:00.009Z	1970-01-01T00:00:00.000009Z
                    3	1970-01-01T00:00:00.000Z	1970-01-01T00:00:00.000000Z
                    4	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.000001Z
                    """,
                    """
                    id	v0	v1
                    1	1970-01-01T00:00:00.257Z	1970-01-01T00:00:00.000257Z
                    2	1969-12-31T23:59:59.743Z	1969-12-31T23:59:59.999743Z
                    3	1970-01-01T00:00:00.000Z	1970-01-01T00:00:00.000000Z
                    4	1970-01-01T00:00:32.767Z	1970-01-01T00:00:00.032767Z
                    """,
                    """
                    id	v0	v1
                    1	1970-01-01T00:01:05.537Z	1970-01-01T00:00:00.065537Z
                    2	1969-12-31T23:58:54.463Z	1969-12-31T23:59:59.934463Z
                    3	\t
                    4	1970-01-25T20:31:23.647Z	1970-01-01T00:35:47.483647Z
                    """,
                    """
                    id	v0	v1
                    1	1970-01-01T00:00:01.234Z	1970-01-01T00:00:00.001234Z
                    2	1969-12-31T23:59:58.766Z	1969-12-31T23:59:59.998766Z
                    3	\t
                    4	2262-04-11T23:47:16.854Z	1970-04-17T18:02:52.036854Z
                    """,
                    """
                    id	v0	v1
                    1	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.000001Z
                    2	1969-12-31T23:59:59.999Z	1969-12-31T23:59:59.999999Z
                    3	\t
                    4	\t
                    """,
                    """
                    id	v0	v1
                    1	1970-01-01T00:00:00.002Z	1970-01-01T00:00:00.000002Z
                    2	1969-12-31T23:59:59.998Z	1969-12-31T23:59:59.999998Z
                    3	\t
                    4	\t
                    """
            );
            for (int i = 0; i < primitives.size(); i++) {
                count += assertCasts(primitives.getQuick(i), new ObjList<>("DATE", "TIMESTAMP"), "INT,DATE,TIMESTAMP", dateTimestampRows.getQuick(i));
                assertCasts(primitives.getQuick(i), new ObjList<>("TIMESTAMP_NS"), "INT,TIMESTAMP_NS", nanoRows.getQuick(i));
            }
            count += assertCasts("dt", DATE_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR,TIMESTAMP", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10
                    1	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.001000Z
                    2	true	-1	￿	-1	-1	-1	-1.0	-1.0	1969-12-31T23:59:59.999Z	1969-12-31T23:59:59.999Z	1969-12-31T23:59:59.999000Z
                    3	false	0		0	null	null	null	null		\t
                    4	true	64		-7616	123456	123456	123456.0	123456.0	1970-01-01T00:02:03.456Z	1970-01-01T00:02:03.456Z	1970-01-01T00:02:03.456000Z
                    """);
            assertCasts("dt", new ObjList<>("TIMESTAMP_NS"), "INT,TIMESTAMP_NS", """
                    id	v0
                    1	1970-01-01T00:00:00.001000000Z
                    2	1969-12-31T23:59:59.999000000Z
                    3\t
                    4	1970-01-01T00:02:03.456000000Z
                    """);
            count += assertCasts("us", TIMESTAMP_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,DATE,STRING,VARCHAR", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10
                    1	true	-46	Ӓ	1234	1234	1234	1234.0	1234.0	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.001234Z	1970-01-01T00:00:00.001234Z
                    2	true	46	אַ	-1234	-1234	-1234	-1234.0	-1234.0	1969-12-31T23:59:59.999Z	1969-12-31T23:59:59.998766Z	1969-12-31T23:59:59.998766Z
                    3	false	0		0	null	null	null	null		\t
                    4	true	-121	횇	-10617	1234567	1234567	1234567.0	1234567.0	1970-01-01T00:00:01.234Z	1970-01-01T00:00:01.234567Z	1970-01-01T00:00:01.234567Z
                    """);
            assertCasts("ns", TIMESTAMP_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,DATE,STRING,VARCHAR", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10
                    1	true	-121	횇	-10617	1234567	1234567	1234567.0	1234567.0	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.001234567Z	1970-01-01T00:00:00.001234567Z
                    2	true	121	⥹	10617	-1234567	-1234567	-1234567.0	-1234567.0	1969-12-31T23:59:59.999Z	1969-12-31T23:59:59.998765433Z	1969-12-31T23:59:59.998765433Z
                    3	false	0		0	null	null	null	null		\t
                    4	true	-46	˒	722	1234567890	1234567890	1.23456789E9	1.23456789E9	1970-01-01T00:00:01.234Z	1970-01-01T00:00:01.234567890Z	1970-01-01T00:00:01.234567890Z
                    """);
            count += assertCasts("s", new ObjList<>("DATE"), "INT,DATE", """
                    id	v0
                    1	2000-01-02T03:04:05.123Z
                    2\t
                    3\t
                    4\t
                    """);
            count += assertCasts("v", new ObjList<>("DATE", "TIMESTAMP"), "INT,DATE,TIMESTAMP", """
                    id	v0	v1
                    1	2000-01-02T03:04:05.123Z	2000-01-02T03:04:05.123456Z
                    2	\t
                    3	\t
                    4	\t
                    """);
            count += assertCasts("sym", new ObjList<>("DATE", "TIMESTAMP"), "INT,DATE,TIMESTAMP", """
                    id	v0	v1
                    1	2000-01-02T03:04:05.123Z	2000-01-02T03:04:05.123000Z
                    2	\t
                    3	\t
                    4	2020-01-01T00:00:00.000Z	2020-01-01T00:00:00.000000Z
                    """);
            assertCasts("v", new ObjList<>("TIMESTAMP_NS"), "INT,TIMESTAMP_NS", """
                    id	v0
                    1	2000-01-02T03:04:05.123456789Z
                    2\t
                    3\t
                    4\t
                    """);
            assertCasts("sym", new ObjList<>("TIMESTAMP_NS"), "INT,TIMESTAMP_NS", """
                    id	v0
                    1	2000-01-02T03:04:05.123000000Z
                    2\t
                    3\t
                    4	2020-01-01T00:00:00.000000000Z
                    """);
            count += assertCasts("ip", new ObjList<>("INT", "VARCHAR"), "INT,INT,VARCHAR", """
                    id	v0	v1
                    1	2130706433	127.0.0.1
                    2	-1	255.255.255.255
                    3	null\t
                    4	-1062731518	192.168.1.2
                    """);
            count += assertCasts("i32", new ObjList<>("IPv4"), "INT,IPv4", """
                    id	v0
                    1	0.1.0.1
                    2	255.254.255.255
                    3\t
                    4	127.255.255.255
                    """);
            count += assertCasts("s", new ObjList<>("IPv4"), "INT,IPv4", """
                    id	v0
                    1\t
                    2\t
                    3\t
                    4	192.168.1.2
                    """);
            count += assertCasts("v", new ObjList<>("IPv4"), "INT,IPv4", """
                    id	v0
                    1\t
                    2\t
                    3\t
                    4	192.168.1.2
                    """);
            Assert.assertEquals(48, count);
            assertQueryRows(
                    "SELECT id,sym::DATE,sym::TIMESTAMP,sym::TIMESTAMP_NS FROM (lp_temporal_cast UNION ALL lp_temporal_cast) ORDER BY id",
                    """
                            id	cast	cast1	cast2
                            1	2000-01-02T03:04:05.123Z	2000-01-02T03:04:05.123000Z	2000-01-02T03:04:05.123000000Z
                            1	2000-01-02T03:04:05.123Z	2000-01-02T03:04:05.123000Z	2000-01-02T03:04:05.123000000Z
                            2		\t
                            2		\t
                            3		\t
                            3		\t
                            4	2020-01-01T00:00:00.000Z	2020-01-01T00:00:00.000000Z	2020-01-01T00:00:00.000000000Z
                            4	2020-01-01T00:00:00.000Z	2020-01-01T00:00:00.000000Z	2020-01-01T00:00:00.000000000Z
                            """
            );
        });
    }

    @Test
    public void testTimestampPrecisionCastsPreserveColumnAndConstantValues() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertCasts("us", new ObjList<>("TIMESTAMP", "TIMESTAMP_NS"), "INT,TIMESTAMP,TIMESTAMP_NS", """
                    id	v0	v1
                    1	1970-01-01T00:00:00.001234Z	1970-01-01T00:00:00.001234000Z
                    2	1969-12-31T23:59:59.998766Z	1969-12-31T23:59:59.998766000Z
                    3	\t
                    4	1970-01-01T00:00:01.234567Z	1970-01-01T00:00:01.234567000Z
                    """);
            assertCasts("ns", new ObjList<>("TIMESTAMP", "TIMESTAMP_NS"), "INT,TIMESTAMP,TIMESTAMP_NS", """
                    id	v0	v1
                    1	1970-01-01T00:00:00.001234Z	1970-01-01T00:00:00.001234567Z
                    2	1969-12-31T23:59:59.998766Z	1969-12-31T23:59:59.998765433Z
                    3	\t
                    4	1970-01-01T00:00:01.234567Z	1970-01-01T00:00:01.234567890Z
                    """);
            assertQueryRows(
                    "SELECT (123456789L::TIMESTAMP_NS)::TIMESTAMP,(-123456789L::TIMESTAMP_NS)::TIMESTAMP,"
                    + "(123456L::TIMESTAMP)::TIMESTAMP_NS,(null::TIMESTAMP_NS)::TIMESTAMP FROM lp_temporal_cast LIMIT 1",
                    """
                            cast	cast1	cast2	cast3
                            1970-01-01T00:00:00.123456Z	1969-12-31T23:57:56.543211Z	1970-01-01T00:00:00.123456000Z\t
                            """
            );
            bindVariableService.setTimestamp(0, 123456);
            bindVariableService.setTimestampNano(1, -123456789L);
            assertQueryRows(
                    "SELECT $1::TIMESTAMP_NS,$2::TIMESTAMP FROM lp_temporal_cast LIMIT 1",
                    """
                            cast	cast1
                            1970-01-01T00:00:00.123456000Z	1969-12-31T23:59:59.876544Z
                            """
            );
        });
    }

    @Test
    public void testConstantAndColumnFormattingKeepExistingDifferences() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertCasts("1::DATE", DATE_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR,TIMESTAMP", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10
                    1	true	1		1	1	1	1.0	1.0	1	1	1970-01-01T00:00:00.001000Z
                    2	true	1		1	1	1	1.0	1.0	1	1	1970-01-01T00:00:00.001000Z
                    3	true	1		1	1	1	1.0	1.0	1	1	1970-01-01T00:00:00.001000Z
                    4	true	1		1	1	1	1.0	1.0	1	1	1970-01-01T00:00:00.001000Z
                    """);
            assertCasts("1::TIMESTAMP", TIMESTAMP_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,DATE,STRING,VARCHAR", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10
                    1	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.000Z	1970-01-01T00:00:00.000001Z	1
                    2	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.000Z	1970-01-01T00:00:00.000001Z	1
                    3	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.000Z	1970-01-01T00:00:00.000001Z	1
                    4	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.000Z	1970-01-01T00:00:00.000001Z	1
                    """);
            assertCasts("1::TIMESTAMP_NS", TIMESTAMP_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,DATE,STRING,VARCHAR", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10
                    1	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.000Z	1970-01-01T00:00:00.000000001Z	1
                    2	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.000Z	1970-01-01T00:00:00.000000001Z	1
                    3	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.000Z	1970-01-01T00:00:00.000000001Z	1
                    4	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.000Z	1970-01-01T00:00:00.000000001Z	1
                    """);
            assertQueryRows(
                    "SELECT (null::DATE)::STRING,(null::DATE)::VARCHAR,(null::TIMESTAMP)::STRING,(null::TIMESTAMP_NS)::VARCHAR,(null::IPv4)::VARCHAR FROM lp_temporal_cast LIMIT 1",
                    """
                            cast	cast1	cast2	cast3	cast4
                            null	null		null	0.0.0.0
                            """
            );
            assertQueryRows(
                    "SELECT dt::STRING,dt::VARCHAR,us::STRING,ns::VARCHAR,ip::VARCHAR FROM lp_temporal_cast ORDER BY id",
                    """
                            cast	cast1	cast2	cast3	cast4
                            1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.001234Z	1970-01-01T00:00:00.001234567Z	127.0.0.1
                            1969-12-31T23:59:59.999Z	1969-12-31T23:59:59.999Z	1969-12-31T23:59:59.998766Z	1969-12-31T23:59:59.998765433Z	255.255.255.255
                            			\t
                            1970-01-01T00:02:03.456Z	1970-01-01T00:02:03.456Z	1970-01-01T00:00:01.234567Z	1970-01-01T00:00:01.234567890Z	192.168.1.2
                            """
            );
            assertQueryRows(
                    "SELECT '2000-01-02T03:04:05.123Z'::DATE,'2000-01-02T03:04:05.123456789Z'::VARCHAR::TIMESTAMP_NS,''::STRING::IPv4,''::VARCHAR::IPv4,'127.0.0.1'::STRING::IPv4,'255.255.255.255'::VARCHAR::IPv4 FROM lp_temporal_cast LIMIT 1",
                    """
                            cast	cast1	cast2	cast3	cast4	cast5
                            2000-01-02T03:04:05.123Z	2000-01-02T03:04:05.123456789Z			127.0.0.1	255.255.255.255
                            """
            );
        });
    }

    @Test
    public void testInvalidConversionsPreserveErrorTimingAndRecover() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT 'not-an-ip'::STRING::IPv4 FROM lp_temporal_cast").noLeakCheck().fails(18, "invalid IPv4 constant");
            assertQuery("SELECT 'not-an-ip'::VARCHAR::IPv4 FROM lp_temporal_cast").noLeakCheck().fails(18, "invalid IPv4 constant");
            assertQueryRows(
                    "SELECT s::IPv4,v::IPv4,s::DATE,v::DATE,sym::DATE,v::TIMESTAMP_NS FROM lp_temporal_cast ORDER BY id",
                    """
                            cast	cast1	cast2	cast3	cast4	cast5
                            		2000-01-02T03:04:05.123Z	2000-01-02T03:04:05.123Z	2000-01-02T03:04:05.123Z	2000-01-02T03:04:05.123456789Z
                            				\t
                            				\t
                            192.168.1.2	192.168.1.2			2020-01-01T00:00:00.000Z\t
                            """
            );
            execute("CREATE TABLE lp_bad_temporal_char(c CHAR)");
            execute("INSERT INTO lp_bad_temporal_char VALUES('x')");
            final ObjList<String> targets = new ObjList<>("DATE", "TIMESTAMP", "TIMESTAMP_NS");
            for (int i = 0; i < targets.size(); i++) {
                final String sql = "SELECT c::" + targets.getQuick(i) + " FROM lp_bad_temporal_char";
                try (RecordCursorFactory factory = select(sql)) {
                    print(factory);
                    Assert.fail(sql);
                } catch (ImplicitCastException e) {
                    TestUtils.assertEquals("inconvertible value: x [CHAR -> " + targets.getQuick(i) + "]", e.getFlyweightMessage());
                }
            }
        });
    }

    @Test
    public void testNativeChildrenAndCompilerLifetime() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT id,(CASE WHEN id IN (1,2,3) THEN i64 ELSE 0L END)::DATE val,v::TIMESTAMP_NS ts,ip::VARCHAR addr FROM lp_temporal_cast ORDER BY id";
            final String expected = """
                    id	val	ts	addr
                    1	1970-01-01T00:00:01.234Z	2000-01-02T03:04:05.123456789Z	127.0.0.1
                    2	1969-12-31T23:59:58.766Z		255.255.255.255
                    3		\t
                    4	1970-01-01T00:00:00.000Z		192.168.1.2
                    """;
            final RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                try {
                    try (RecordCursorFactory ignored = compiler.compile(
                            "SELECT (id IN (1,2,3))::TIMESTAMP,'bad-ip'::STRING::IPv4 FROM lp_temporal_cast",
                            sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        Assert.fail();
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "invalid IPv4 constant");
                    }
                    try (RecordCursorFactory recovery = compiler.compile("SELECT i32::IPv4 FROM lp_temporal_cast", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(recovery);
                    }
                } catch (Throwable th) {
                    retained.close();
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained) {
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
            }
            assertQueryRows(
                    "SELECT id FROM lp_temporal_cast WHERE i64::DATE>0::DATE ORDER BY id",
                    """
                            id
                            1
                            4
                            """
            );
            assertQueryRows(
                    "SELECT dt::INT k,sum(i32::DATE::LONG) total FROM lp_temporal_cast GROUP BY 1 ORDER BY k",
                    """
                            k	total
                            null	null
                            -1	-65537
                            1	65537
                            123456	2147483647
                            """
            );
        });
    }

    @Test
    public void testTypedParametersAndRebinding() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setDate(0, 123);
            bindVariableService.setTimestamp(1, 123456);
            bindVariableService.setTimestampNano(2, 123456789);
            bindVariableService.setIPv4(3, "127.0.0.1");
            assertCasts("$1", DATE_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR,TIMESTAMP", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10
                    1	true	123	{	123	123	123	123.0	123.0	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123000Z
                    2	true	123	{	123	123	123	123.0	123.0	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123000Z
                    3	true	123	{	123	123	123	123.0	123.0	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123000Z
                    4	true	123	{	123	123	123	123.0	123.0	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123000Z
                    """);
            assertCasts("$2", TIMESTAMP_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,DATE,STRING,VARCHAR", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10
                    1	true	64		-7616	123456	123456	123456.0	123456.0	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123456Z	1970-01-01T00:00:00.123456Z
                    2	true	64		-7616	123456	123456	123456.0	123456.0	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123456Z	1970-01-01T00:00:00.123456Z
                    3	true	64		-7616	123456	123456	123456.0	123456.0	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123456Z	1970-01-01T00:00:00.123456Z
                    4	true	64		-7616	123456	123456	123456.0	123456.0	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123456Z	1970-01-01T00:00:00.123456Z
                    """);
            assertCasts("$3", TIMESTAMP_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,DATE,STRING,VARCHAR", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10
                    1	true	21	촕	-13035	123456789	123456789	1.23456789E8	1.23456789E8	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123456789Z	1970-01-01T00:00:00.123456789Z
                    2	true	21	촕	-13035	123456789	123456789	1.23456789E8	1.23456789E8	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123456789Z	1970-01-01T00:00:00.123456789Z
                    3	true	21	촕	-13035	123456789	123456789	1.23456789E8	1.23456789E8	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123456789Z	1970-01-01T00:00:00.123456789Z
                    4	true	21	촕	-13035	123456789	123456789	1.23456789E8	1.23456789E8	1970-01-01T00:00:00.123Z	1970-01-01T00:00:00.123456789Z	1970-01-01T00:00:00.123456789Z
                    """);
            assertCasts("$4", new ObjList<>("INT", "VARCHAR"), "INT,INT,VARCHAR", """
                    id	v0	v1
                    1	2130706433	127.0.0.1
                    2	2130706433	127.0.0.1
                    3	2130706433	127.0.0.1
                    4	2130706433	127.0.0.1
                    """);
            bindVariableService.setDate(0, 123);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile("SELECT $1::LONG val FROM lp_temporal_cast LIMIT 1", sqlExecutionContext).getRecordCursorFactory()) {
                    assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns("val\n123\n");
                    bindVariableService.setDate(0, Numbers.LONG_NULL);
                    assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns("val\nnull\n");
                }
            }
            bindVariableService.setTimestamp(1, Numbers.LONG_NULL);
            bindVariableService.setTimestampNano(2, Numbers.LONG_NULL);
            bindVariableService.setIPv4(3, Numbers.IPv4_NULL);
            assertCasts("$1", DATE_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR,TIMESTAMP", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10
                    1	false	0		0	null	null	null	null		\t
                    2	false	0		0	null	null	null	null		\t
                    3	false	0		0	null	null	null	null		\t
                    4	false	0		0	null	null	null	null		\t
                    """);
            assertCasts("$2", TIMESTAMP_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,DATE,STRING,VARCHAR", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10
                    1	false	0		0	null	null	null	null		\t
                    2	false	0		0	null	null	null	null		\t
                    3	false	0		0	null	null	null	null		\t
                    4	false	0		0	null	null	null	null		\t
                    """);
            assertCasts("$3", TIMESTAMP_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,DATE,STRING,VARCHAR", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10
                    1	false	0		0	null	null	null	null		\t
                    2	false	0		0	null	null	null	null		\t
                    3	false	0		0	null	null	null	null		\t
                    4	false	0		0	null	null	null	null		\t
                    """);
            assertCasts("$4", new ObjList<>("INT", "VARCHAR"), "INT,INT,VARCHAR", """
                    id	v0	v1
                    1	null\t
                    2	null\t
                    3	null\t
                    4	null\t
                    """);
        });
    }

    @Test
    public void testTimestampToLongRetainsEpochUnitsAndCompilerLifetime() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,us::LONG u,ns::LONG n FROM lp_temporal_cast ORDER BY id",
                    """
                            id	u	n
                            1	1234	1234567
                            2	-1234	-1234567
                            3	null	null
                            4	1234567	1234567890
                            """
            );
            assertQueryRows(
                    "SELECT (1234567L::TIMESTAMP)::LONG,(1234567L::TIMESTAMP_NS)::LONG,"
                    + "(-1L::TIMESTAMP_NS)::LONG,(NULL::TIMESTAMP)::LONG FROM lp_temporal_cast LIMIT 1",
                    """
                            cast	cast1	cast2	cast3
                            1234567	1234567	-1	null
                            """
            );
            bindVariableService.setTimestamp(0, -1);
            bindVariableService.setTimestampNano(1, -1001);
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT $1::LONG u,$2::LONG n FROM lp_temporal_cast LIMIT 1",
                            sqlExecutionContext).getRecordCursorFactory();
                    Assert.assertEquals(2, retained.getMetadata().getColumnCount());
                    Assert.assertEquals(ColumnType.LONG, retained.getMetadata().getColumnType(0));
                    Assert.assertEquals(ColumnType.LONG, retained.getMetadata().getColumnType(1));
                    Assert.assertEquals(-1, retained.getMetadata().getTimestampIndex());
                    TestUtils.assertEquals("u", retained.getMetadata().getColumnName(0));
                    TestUtils.assertEquals("n", retained.getMetadata().getColumnName(1));
                    compiler.clear();
                    try (RecordCursorFactory recovery = compiler.compile("SELECT count() FROM lp_temporal_cast",
                            sqlExecutionContext).getRecordCursorFactory()) {
                        assertFactory(recovery).withContext(sqlExecutionContext).inferRandomAccess().expectSize().returns("count\n4\n");
                    }
                }
                assertFactory(retained).withContext(sqlExecutionContext).inferRandomAccess().sizeMayVary().returns("u\tn\n-1\t-1001\n");
                bindVariableService.setTimestamp(0, Numbers.LONG_NULL);
                bindVariableService.setTimestampNano(1, Numbers.LONG_NULL);
                assertFactory(retained).withContext(sqlExecutionContext).inferRandomAccess().sizeMayVary().returns("u\tn\nnull\tnull\n");
                bindVariableService.setTimestamp(0, 1);
                bindVariableService.setTimestampNano(1, 1001);
                assertFactory(retained).withContext(sqlExecutionContext).inferRandomAccess().sizeMayVary().returns("u\tn\n1\t1001\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    private void assertCastQuery(String sql, String expectedTypes, String expectedRows) throws Exception {
        final int jitMode = sqlExecutionContext.getJitMode();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory factory = select(sql)) {
            final StringSink types = new StringSink();
            for (int i = 0, n = factory.getMetadata().getColumnCount(); i < n; i++) {
                if (i > 0) {
                    types.put(',');
                }
                types.put(ColumnType.nameOf(factory.getMetadata().getColumnType(i)));
            }
            TestUtils.assertEquals(sql, expectedTypes, types);
            assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expectedRows);
        } finally {
            sqlExecutionContext.setJitMode(jitMode);
        }
    }

    private int assertCasts(String source, ObjList<String> targets, String expectedTypes, String expectedRows) throws Exception {
        final StringSink sql = new StringSink();
        sql.put("SELECT id");
        for (int i = 0; i < targets.size(); i++) {
            sql.put(",(").put(source).put(")::").put(targets.getQuick(i)).put(" v").put(i);
        }
        sql.put(" FROM lp_temporal_cast ORDER BY id");
        assertCastQuery(sql.toString(), expectedTypes, expectedRows);
        return targets.size();
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_temporal_cast(unused INT,id INT,b BOOLEAN,i8 BYTE,c CHAR,i16 SHORT,i32 INT,i64 LONG,f32 FLOAT,f64 DOUBLE,dt DATE,us TIMESTAMP,ns TIMESTAMP_NS,s STRING,v VARCHAR,sym SYMBOL,ip IPv4)");
        execute("""
                INSERT INTO lp_temporal_cast VALUES
                (99,1,true,1,'1',257,65537,1234,1.5,2.5,1::DATE,1234::TIMESTAMP,1234567::TIMESTAMP_NS,'2000-01-02T03:04:05.123Z','2000-01-02T03:04:05.123456789Z','2000-01-02T03:04:05.123Z','127.0.0.1'),
                (99,2,false,-1,'9',-257,-65537,-1234,-1.5,-2.5,(-1)::DATE,(-1234)::TIMESTAMP,(-1234567)::TIMESTAMP_NS,'bad','中','bad','255.255.255.255'),
                (99,3,true,0,'0',0,null,null,null,null,null,null,null,null,null,null,null),
                (99,4,false,127,'1',32767,2147483647,9223372036854,1e30,1e30,123456::DATE,1234567::TIMESTAMP,1234567890::TIMESTAMP_NS,'192.168.1.2','192.168.1.2','2020-01-01T00:00:00.000Z','192.168.1.2')
                """);
    }

    private String print(RecordCursorFactory factory) throws Exception {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
            return sink.toString();
        }
    }
    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
