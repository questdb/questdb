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

public class WideCastTest extends AbstractCairoTest {
    private static final ObjList<String> LONG256_TARGETS = new ObjList<>(
            "BOOLEAN", "BYTE", "CHAR", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP", "TIMESTAMP_NS", "STRING", "VARCHAR", "SYMBOL"
    );

    @Test
    public void testUuidCastsAndTextPredicates() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertCasts("s", new ObjList<>("UUID"), "INT,UUID", """
                    id	v0
                    1	00000000-0000-0002-0000-000000000001
                    2\t
                    3\t
                    4\t
                    """);
            assertCasts("v", new ObjList<>("UUID"), "INT,UUID", """
                    id	v0
                    1\t
                    2\t
                    3\t
                    4	00000000-0000-0002-0000-000000000001
                    """);
            assertCasts("u", new ObjList<>("STRING", "VARCHAR", "UUID"), "INT,STRING,VARCHAR,UUID", """
                    id	v0	v1	v2
                    1	00000000-0000-0002-0000-000000000001	00000000-0000-0002-0000-000000000001	00000000-0000-0002-0000-000000000001
                    2	00000000-0000-0000-0000-000000000002	00000000-0000-0000-0000-000000000002	00000000-0000-0000-0000-000000000002
                    3		\t
                    4	00000000-0000-0002-0000-000000000001	00000000-0000-0002-0000-000000000001	00000000-0000-0002-0000-000000000001
                    """);
            assertQueryRows(
                    "SELECT '00000000-0000-0002-0000-000000000001'::UUID u,''::VARCHAR::UUID e,null::UUID n FROM lp_wide_cast LIMIT 1",
                    """
                            u	e	n
                            00000000-0000-0002-0000-000000000001	\t
                            """
            );
            assertQueryRows("SELECT id,u=s dynamic FROM lp_wide_cast ORDER BY id", """
                    id	dynamic
                    1	true
                    2	false
                    3	true
                    4	false
                    """);
            bindVariableService.setStr(0, "00000000-0000-0002-0000-000000000001");
            assertQueryRows("SELECT id,u=$1 runtime FROM lp_wide_cast ORDER BY id", """
                    id	runtime
                    1	true
                    2	false
                    3	false
                    4	true
                    """);
            bindVariableService.setStr(0, "invalid uuid");
            assertQueryRows("SELECT id,u!=$1 runtime FROM lp_wide_cast ORDER BY id", """
                    id	runtime
                    1	true
                    2	true
                    3	true
                    4	true
                    """);
            {
                final String op = "=";
                {
                    final String text = "'00000000-0000-0002-0000-000000000001'";
                    assertQueryRows(
                            "SELECT id,u" + op + text + " a," + text + op + "u b FROM lp_wide_cast ORDER BY id",
                            """
                                    id	a	b
                                    1	true	true
                                    2	false	false
                                    3	false	false
                                    4	true	true
                                    """
                    );
                    assertQueryRows(
                            "SELECT id FROM lp_wide_cast WHERE u" + op + text + " ORDER BY id",
                            """
                                    id
                                    1
                                    4
                                    """
                    );
                }
                {
                    final String text = "'invalid uuid'";
                    assertQueryRows(
                            "SELECT id,u" + op + text + " a," + text + op + "u b FROM lp_wide_cast ORDER BY id",
                            """
                                    id	a	b
                                    1	false	false
                                    2	false	false
                                    3	false	false
                                    4	false	false
                                    """
                    );
                    assertQueryRows(
                            "SELECT id FROM lp_wide_cast WHERE u" + op + text + " ORDER BY id",
                            """
                                    id
                                    """
                    );
                }
                {
                    final String text = "null";
                    assertQueryRows(
                            "SELECT id,u" + op + text + " a," + text + op + "u b FROM lp_wide_cast ORDER BY id",
                            """
                                    id	a	b
                                    1	false	false
                                    2	false	false
                                    3	true	true
                                    4	false	false
                                    """
                    );
                    assertQueryRows(
                            "SELECT id FROM lp_wide_cast WHERE u" + op + text + " ORDER BY id",
                            """
                                    id
                                    3
                                    """
                    );
                }
            }
            {
                final String op = "!=";
                {
                    final String text = "'00000000-0000-0002-0000-000000000001'";
                    assertQueryRows(
                            "SELECT id,u" + op + text + " a," + text + op + "u b FROM lp_wide_cast ORDER BY id",
                            """
                                    id	a	b
                                    1	false	false
                                    2	true	true
                                    3	true	true
                                    4	false	false
                                    """
                    );
                    assertQueryRows(
                            "SELECT id FROM lp_wide_cast WHERE u" + op + text + " ORDER BY id",
                            """
                                    id
                                    2
                                    3
                                    """
                    );
                }
                {
                    final String text = "'invalid uuid'";
                    assertQueryRows(
                            "SELECT id,u" + op + text + " a," + text + op + "u b FROM lp_wide_cast ORDER BY id",
                            """
                                    id	a	b
                                    1	true	true
                                    2	true	true
                                    3	true	true
                                    4	true	true
                                    """
                    );
                    assertQueryRows(
                            "SELECT id FROM lp_wide_cast WHERE u" + op + text + " ORDER BY id",
                            """
                                    id
                                    1
                                    2
                                    3
                                    4
                                    """
                    );
                }
                {
                    final String text = "null";
                    assertQueryRows(
                            "SELECT id,u" + op + text + " a," + text + op + "u b FROM lp_wide_cast ORDER BY id",
                            """
                                    id	a	b
                                    1	true	true
                                    2	true	true
                                    3	false	false
                                    4	true	true
                                    """
                    );
                    assertQueryRows(
                            "SELECT id FROM lp_wide_cast WHERE u" + op + text + " ORDER BY id",
                            """
                                    id
                                    1
                                    2
                                    4
                                    """
                    );
                }
            }
            {
                final String op = "<>";
                {
                    final String text = "'00000000-0000-0002-0000-000000000001'";
                    assertQueryRows(
                            "SELECT id,u" + op + text + " a," + text + op + "u b FROM lp_wide_cast ORDER BY id",
                            """
                                    id	a	b
                                    1	false	false
                                    2	true	true
                                    3	true	true
                                    4	false	false
                                    """
                    );
                    assertQueryRows(
                            "SELECT id FROM lp_wide_cast WHERE u" + op + text + " ORDER BY id",
                            """
                                    id
                                    2
                                    3
                                    """
                    );
                }
                {
                    final String text = "'invalid uuid'";
                    assertQueryRows(
                            "SELECT id,u" + op + text + " a," + text + op + "u b FROM lp_wide_cast ORDER BY id",
                            """
                                    id	a	b
                                    1	true	true
                                    2	true	true
                                    3	true	true
                                    4	true	true
                                    """
                    );
                    assertQueryRows(
                            "SELECT id FROM lp_wide_cast WHERE u" + op + text + " ORDER BY id",
                            """
                                    id
                                    1
                                    2
                                    3
                                    4
                                    """
                    );
                }
                {
                    final String text = "null";
                    assertQueryRows(
                            "SELECT id,u" + op + text + " a," + text + op + "u b FROM lp_wide_cast ORDER BY id",
                            """
                                    id	a	b
                                    1	true	true
                                    2	true	true
                                    3	false	false
                                    4	true	true
                                    """
                    );
                    assertQueryRows(
                            "SELECT id FROM lp_wide_cast WHERE u" + op + text + " ORDER BY id",
                            """
                                    id
                                    1
                                    2
                                    4
                                    """
                    );
                }
            }
            assertQuery("SELECT 'bad'::UUID FROM lp_wide_cast").noLeakCheck().fails(7, "invalid UUID constant");
            assertQuery("SELECT 'bad'::VARCHAR::UUID FROM lp_wide_cast").noLeakCheck().fails(12, "invalid UUID constant");
        });
    }

    @Test
    public void testEveryLong256CastKeepsExistingNullAndLowWordSemantics() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<String> values = new ObjList<>("b", "i8", "c", "i16", "i32", "i64", "f32", "f64", "dt", "us", "ns", "s", "v", "sym");
            final ObjList<String> long256Rows = new ObjList<>(
                    """
                            id	v0
                            1	0x01
                            2	0x00
                            3	0x01
                            4	0x00
                            """,
                    """
                            id	v0
                            1	0x01
                            2	0xffffffffffffffff
                            3	0x00
                            4	0x7f
                            """,
                    """
                            id	v0
                            1	0x31
                            2	0x39
                            3	0x30
                            4	0x31
                            """,
                    """
                            id	v0
                            1	0x0101
                            2	0xfffffffffffffeff
                            3	0x00
                            4	0x7fff
                            """,
                    """
                            id	v0
                            1	0x010001
                            2	0xfffffffffffeffff
                            3\t
                            4	0x7fffffff
                            """,
                    """
                            id	v0
                            1	0x04d2
                            2	0xfffffffffffffb2e
                            3\t
                            4	0x08637bd05af6
                            """,
                    """
                            id	v0
                            1	0x01
                            2	0xffffffffffffffff
                            3\t
                            4	0x7fffffffffffffff
                            """,
                    """
                            id	v0
                            1	0x02
                            2	0xfffffffffffffffe
                            3\t
                            4	0x7fffffffffffffff
                            """,
                    """
                            id	v0
                            1	0x01
                            2	0xffffffffffffffff
                            3\t
                            4	0x01e240
                            """,
                    """
                            id	v0
                            1	0x04d2
                            2	0xfffffffffffffb2e
                            3\t
                            4	0x12d687
                            """,
                    """
                            id	v0
                            1	0x12d687
                            2	0xffffffffffed2979
                            3\t
                            4	0x499602d2
                            """,
                    """
                            id	v0
                            1\t
                            2\t
                            3\t
                            4\t
                            """,
                    """
                            id	v0
                            1\t
                            2\t
                            3\t
                            4\t
                            """,
                    """
                            id	v0
                            1\t
                            2\t
                            3\t
                            4	0x01
                            """
            );
            for (int i = 0, n = values.size(); i < n; i++) {
                assertCasts(values.getQuick(i), new ObjList<>("LONG256"), "INT,LONG256", long256Rows.getQuick(i));
            }
            assertCasts("h", LONG256_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,DATE,TIMESTAMP,TIMESTAMP_NS,STRING,VARCHAR,SYMBOL", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10	v11	v12	v13
                    1	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.000001Z	1970-01-01T00:00:00.000000001Z	0x04000000000000000300000000000000020000000000000001	0x04000000000000000300000000000000020000000000000001	1
                    2	true	2		2	2	2	2.0	2.0	1970-01-01T00:00:00.002Z	1970-01-01T00:00:00.000002Z	1970-01-01T00:00:00.000000002Z	0x02	0x02	2
                    3	false	0		0	null	null	null	null					\t
                    4	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.000001Z	1970-01-01T00:00:00.000000001Z	0x01	0x01	1
                    """);
            assertCasts("0x0000000000000004000000000000000300000000000000020000000000000001", LONG256_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,DATE,TIMESTAMP,TIMESTAMP_NS,STRING,VARCHAR,SYMBOL", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10	v11	v12	v13
                    1	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.000001Z	1970-01-01T00:00:00.000000001Z	0x04000000000000000300000000000000020000000000000001	0x04000000000000000300000000000000020000000000000001	1
                    2	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.000001Z	1970-01-01T00:00:00.000000001Z	0x04000000000000000300000000000000020000000000000001	0x04000000000000000300000000000000020000000000000001	1
                    3	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.000001Z	1970-01-01T00:00:00.000000001Z	0x04000000000000000300000000000000020000000000000001	0x04000000000000000300000000000000020000000000000001	1
                    4	true	1		1	1	1	1.0	1.0	1970-01-01T00:00:00.001Z	1970-01-01T00:00:00.000001Z	1970-01-01T00:00:00.000000001Z	0x04000000000000000300000000000000020000000000000001	0x04000000000000000300000000000000020000000000000001	1
                    """);
            assertCasts("null::LONG256", LONG256_TARGETS, "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,DATE,TIMESTAMP,TIMESTAMP_NS,STRING,VARCHAR,SYMBOL", """
                    id	v0	v1	v2	v3	v4	v5	v6	v7	v8	v9	v10	v11	v12	v13
                    1	false	0		0	null	null	null	null						null
                    2	false	0		0	null	null	null	null						null
                    3	false	0		0	null	null	null	null						null
                    4	false	0		0	null	null	null	null						null
                    """);
            assertQueryRows(
                    "SELECT id,h::LONG256 same,(s::VARCHAR)::LONG256 text FROM lp_wide_cast ORDER BY id",
                    """
                            id	same	text
                            1	0x04000000000000000300000000000000020000000000000001\t
                            2	0x02\t
                            3	\t
                            4	0x01\t
                            """
            );
        });
    }

    @Test
    public void testGeoHashCastsAcrossStorageWidthsAndPrecisions() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<String> values = new ObjList<>("g5", "g10", "g20", "g60");
            final ObjList<String> geoTextRows = new ObjList<>(
                    """
                            id	v0	v1
                            1	u	u
                            2	s	s
                            3	\t
                            4	u	u
                            """,
                    """
                            id	v0	v1
                            1	u4	u4
                            2	s0	s0
                            3	\t
                            4	u4	u4
                            """,
                    """
                            id	v0	v1
                            1	u4pr	u4pr
                            2	s000	s000
                            3	\t
                            4	u4pr	u4pr
                            """,
                    """
                            id	v0	v1
                            1	u4pruydqqvj8	u4pruydqqvj8
                            2	s00000000000	s00000000000
                            3	\t
                            4	u4pruydqqvj8	u4pruydqqvj8
                            """
            );
            for (int i = 0, n = values.size(); i < n; i++) {
                assertCasts(values.getQuick(i), new ObjList<>("STRING", "VARCHAR"), "INT,STRING,VARCHAR", geoTextRows.getQuick(i));
            }
            {
                final int bits = 1;
                final String target = "GEOHASH(" + bits + "b)";
                assertCasts("g60", new ObjList<>(target), "INT,GEOHASH(1b)", """
                        id	v0
                        1	1
                        2	1
                        3\t
                        4	1
                        """);
                assertCasts("i64", new ObjList<>(target), "INT,GEOHASH(1b)", """
                        id	v0
                        1	0
                        2	0
                        3\t
                        4	0
                        """);
                assertCasts("s", new ObjList<>(target), "INT,GEOHASH(1b)", """
                        id	v0
                        1	0
                        2	0
                        3\t
                        4	1
                        """);
                assertCasts("v", new ObjList<>(target), "INT,GEOHASH(1b)", """
                        id	v0
                        1	0
                        2	0
                        3\t
                        4	0
                        """);
                assertQueryRows(
                        "SELECT ('u4pruydqqvj8'::" + target + ") text,(1L::" + target + ") num,(null::" + target + ") nil FROM lp_wide_cast LIMIT 1",
                        """
                                text	num	nil
                                1	1\t
                                """
                );
            }
            {
                final int bits = 7;
                final String target = "GEOHASH(" + bits + "b)";
                assertCasts("g60", new ObjList<>(target), "INT,GEOHASH(7b)", """
                        id	v0
                        1	1101000
                        2	1100000
                        3\t
                        4	1101000
                        """);
                assertCasts("i64", new ObjList<>(target), "INT,GEOHASH(7b)", """
                        id	v0
                        1	1010010
                        2	0101110
                        3\t
                        4	1110110
                        """);
                assertCasts("s", new ObjList<>(target), "INT,GEOHASH(7b)", """
                        id	v0
                        1	0000000
                        2\t
                        3\t
                        4	1101000
                        """);
                assertCasts("v", new ObjList<>(target), "INT,GEOHASH(7b)", """
                        id	v0
                        1	0000011
                        2\t
                        3\t
                        4	0000000
                        """);
                assertQueryRows(
                        "SELECT ('u4pruydqqvj8'::" + target + ") text,(1L::" + target + ") num,(null::" + target + ") nil FROM lp_wide_cast LIMIT 1",
                        """
                                text	num	nil
                                1101000	0000001\t
                                """
                );
            }
            {
                final int bits = 8;
                final String target = "GEOHASH(" + bits + "b)";
                assertCasts("g60", new ObjList<>(target), "INT,GEOHASH(8b)", """
                        id	v0
                        1	11010001
                        2	11000000
                        3\t
                        4	11010001
                        """);
                assertCasts("i64", new ObjList<>(target), "INT,GEOHASH(8b)", """
                        id	v0
                        1	11010010
                        2	00101110
                        3\t
                        4	11110110
                        """);
                assertCasts("s", new ObjList<>(target), "INT,GEOHASH(8b)", """
                        id	v0
                        1	00000000
                        2\t
                        3\t
                        4	11010001
                        """);
                assertCasts("v", new ObjList<>(target), "INT,GEOHASH(8b)", """
                        id	v0
                        1	00000111
                        2\t
                        3\t
                        4	00000000
                        """);
                assertQueryRows(
                        "SELECT ('u4pruydqqvj8'::" + target + ") text,(1L::" + target + ") num,(null::" + target + ") nil FROM lp_wide_cast LIMIT 1",
                        """
                                text	num	nil
                                11010001	00000001\t
                                """
                );
            }
            {
                final int bits = 15;
                final String target = "GEOHASH(" + bits + "b)";
                assertCasts("g60", new ObjList<>(target), "INT,GEOHASH(3c)", """
                        id	v0
                        1	u4p
                        2	s00
                        3\t
                        4	u4p
                        """);
                assertCasts("i64", new ObjList<>(target), "INT,GEOHASH(3c)", """
                        id	v0
                        1	16k
                        2	ytf
                        3\t
                        4	qrq
                        """);
                assertCasts("s", new ObjList<>(target), "INT,GEOHASH(3c)", """
                        id	v0
                        1	000
                        2\t
                        3\t
                        4	u4p
                        """);
                assertCasts("v", new ObjList<>(target), "INT,GEOHASH(3c)", """
                        id	v0
                        1	0x1
                        2\t
                        3\t
                        4	000
                        """);
                assertQueryRows(
                        "SELECT ('u4pruydqqvj8'::" + target + ") text,(1L::" + target + ") num,(null::" + target + ") nil FROM lp_wide_cast LIMIT 1",
                        """
                                text	num	nil
                                u4p	001\t
                                """
                );
            }
            {
                final int bits = 16;
                final String target = "GEOHASH(" + bits + "b)";
                assertCasts("g60", new ObjList<>(target), "INT,GEOHASH(16b)", """
                        id	v0
                        1	1101000100101011
                        2	1100000000000000
                        3\t
                        4	1101000100101011
                        """);
                assertCasts("i64", new ObjList<>(target), "INT,GEOHASH(16b)", """
                        id	v0
                        1	0000010011010010
                        2	1111101100101110
                        3\t
                        4	0101101011110110
                        """);
                assertCasts("s", new ObjList<>(target), "INT,GEOHASH(16b)", """
                        id	v0
                        1	0000000000000000
                        2\t
                        3\t
                        4	1101000100101011
                        """);
                assertCasts("v", new ObjList<>(target), "INT,GEOHASH(16b)", """
                        id	v0
                        1	0000011101000010
                        2\t
                        3\t
                        4	0000000000000000
                        """);
                assertQueryRows(
                        "SELECT ('u4pruydqqvj8'::" + target + ") text,(1L::" + target + ") num,(null::" + target + ") nil FROM lp_wide_cast LIMIT 1",
                        """
                                text	num	nil
                                1101000100101011	0000000000000001\t
                                """
                );
            }
            {
                final int bits = 31;
                final String target = "GEOHASH(" + bits + "b)";
                assertCasts("g60", new ObjList<>(target), "INT,GEOHASH(31b)", """
                        id	v0
                        1	1101000100101011011111010111100
                        2	1100000000000000000000000000000
                        3\t
                        4	1101000100101011011111010111100
                        """);
                assertCasts("i64", new ObjList<>(target), "INT,GEOHASH(31b)", """
                        id	v0
                        1	0000000000000000000010011010010
                        2	1111111111111111111101100101110
                        3\t
                        4	1111011110100000101101011110110
                        """);
                assertCasts("s", new ObjList<>(target), "INT,GEOHASH(31b)", """
                        id	v0
                        1	0000000000000000000000000000000
                        2\t
                        3\t
                        4	1101000100101011011111010111100
                        """);
                assertCasts("v", new ObjList<>(target), "INT,GEOHASH(31b)", """
                        id	v0
                        1	0000011101000010001000011001000
                        2\t
                        3\t
                        4	0000000000000000000000000000000
                        """);
                assertQueryRows(
                        "SELECT ('u4pruydqqvj8'::" + target + ") text,(1L::" + target + ") num,(null::" + target + ") nil FROM lp_wide_cast LIMIT 1",
                        """
                                text	num	nil
                                1101000100101011011111010111100	0000000000000000000000000000001\t
                                """
                );
            }
            {
                final int bits = 32;
                final String target = "GEOHASH(" + bits + "b)";
                assertCasts("g60", new ObjList<>(target), "INT,GEOHASH(32b)", """
                        id	v0
                        1	11010001001010110111110101111001
                        2	11000000000000000000000000000000
                        3\t
                        4	11010001001010110111110101111001
                        """);
                assertCasts("i64", new ObjList<>(target), "INT,GEOHASH(32b)", """
                        id	v0
                        1	00000000000000000000010011010010
                        2	11111111111111111111101100101110
                        3\t
                        4	01111011110100000101101011110110
                        """);
                assertCasts("s", new ObjList<>(target), "INT,GEOHASH(32b)", """
                        id	v0
                        1	00000000000000000000000000000000
                        2\t
                        3\t
                        4	11010001001010110111110101111001
                        """);
                assertCasts("v", new ObjList<>(target), "INT,GEOHASH(32b)", """
                        id	v0
                        1	00000111010000100010000110010000
                        2\t
                        3\t
                        4	00000000000000000000000000000000
                        """);
                assertQueryRows(
                        "SELECT ('u4pruydqqvj8'::" + target + ") text,(1L::" + target + ") num,(null::" + target + ") nil FROM lp_wide_cast LIMIT 1",
                        """
                                text	num	nil
                                11010001001010110111110101111001	00000000000000000000000000000001\t
                                """
                );
            }
            {
                final int bits = 60;
                final String target = "GEOHASH(" + bits + "b)";
                assertCasts("g60", new ObjList<>(target), "INT,GEOHASH(12c)", """
                        id	v0
                        1	u4pruydqqvj8
                        2	s00000000000
                        3\t
                        4	u4pruydqqvj8
                        """);
                assertCasts("i64", new ObjList<>(target), "INT,GEOHASH(12c)", """
                        id	v0
                        1	00000000016k
                        2	zzzzzzzzzytf
                        3\t
                        4	0008dexx0qrq
                        """);
                assertCasts("s", new ObjList<>(target), "INT,GEOHASH(12c)", """
                        id	v0
                        1\t
                        2\t
                        3\t
                        4	u4pruydqqvj8
                        """);
                assertCasts("v", new ObjList<>(target), "INT,GEOHASH(12c)", """
                        id	v0
                        1\t
                        2\t
                        3\t
                        4\t
                        """);
                assertQueryRows(
                        "SELECT ('u4pruydqqvj8'::" + target + ") text,(1L::" + target + ") num,(null::" + target + ") nil FROM lp_wide_cast LIMIT 1",
                        """
                                text	num	nil
                                u4pruydqqvj8	000000000001\t
                                """
                );
            }
            assertQueryRows("SELECT ##10101 bits,#u4pr chars FROM lp_wide_cast LIMIT 1", """
                    bits	chars
                    p	u4pr
                    """);
            assertQuery("SELECT ('x'::GEOHASH(60b)) FROM lp_wide_cast").noLeakCheck().fails(8, "string is too short to cast to chosen GEOHASH precision [len=1, precision=60]");
            assertQuery("SELECT ('invalid'::GEOHASH(5b)) FROM lp_wide_cast").noLeakCheck().fails(8, "invalid GEOHASH");
            assertQuery("SELECT (g5::GEOHASH(60b)) FROM lp_wide_cast").noLeakCheck().fails(10, "CAST cannot narrow values from GEOHASH(5b) to GEOHASH(60b)");
        });
    }

    @Test
    public void testTypedParametersRefreshTheirFullValues() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final int geoType = ColumnType.getGeoHashTypeWithBits(60);
            bindVariableService.setUuid(0, 1, 2);
            bindVariableService.setLong256(1, 1, 2, 3, 4);
            bindVariableService.setGeoHash(2, 123456789, geoType);
            assertQueryRows(
                    "SELECT $1::STRING u,$2::VARCHAR h,($3::GEOHASH(5b)) g FROM lp_wide_cast LIMIT 1",
                    """
                            u	h	g
                            00000000-0000-0002-0000-000000000001	0x04000000000000000300000000000000020000000000000001	0
                            """
            );
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT $1 u,$2 h,$3 g FROM lp_wide_cast LIMIT 1", sqlExecutionContext).getRecordCursorFactory();
                    Assert.assertEquals(ColumnType.UUID, retained.getMetadata().getColumnType(0));
                    Assert.assertEquals(ColumnType.LONG256, retained.getMetadata().getColumnType(1));
                    Assert.assertEquals(geoType, retained.getMetadata().getColumnType(2));
                    compiler.clear();
                }
                for (int value : new int[]{1, 7}) {
                    bindVariableService.setUuid(0, value, value + 1);
                    bindVariableService.setLong256(1, value, 2, 3, 4);
                    bindVariableService.setGeoHash(2, value, geoType);
                    try (RecordCursor cursor = retained.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(value, cursor.getRecord().getLong128Lo(0));
                        Assert.assertEquals(value + 1, cursor.getRecord().getLong128Hi(0));
                        Assert.assertEquals(value, cursor.getRecord().getLong256A(1).getLong0());
                        Assert.assertEquals(4, cursor.getRecord().getLong256A(1).getLong3());
                        Assert.assertEquals(value, cursor.getRecord().getGeoLong(2));
                        Assert.assertFalse(cursor.hasNext());
                    }
                }
                bindVariableService.setUuid(0, Numbers.LONG_NULL, Numbers.LONG_NULL);
                bindVariableService.setLong256(1, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
                bindVariableService.setGeoHash(2, -1, geoType);
                try (RecordCursor cursor = retained.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(Numbers.LONG_NULL, cursor.getRecord().getLong128Lo(0));
                    Assert.assertEquals(Numbers.LONG_NULL, cursor.getRecord().getLong256A(1).getLong3());
                    Assert.assertEquals(-1, cursor.getRecord().getGeoLong(2));
                }
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testPrunedComputedProjectionSurvivesCompilerCloseAndFailure() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT u::STRING identifier,h::VARCHAR wide,(g60::GEOHASH(5b)) geo FROM (SELECT g60,h,u,id FROM lp_wide_cast WHERE id=1)";
            final String expected = """
                    identifier	wide	geo
                    00000000-0000-0002-0000-000000000001	0x04000000000000000300000000000000020000000000000001	u
                    """;
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    compiler.clear();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT (CASE WHEN id IN(1,2,3) THEN u ELSE null END)='invalid uuid' FROM lp_wide_cast", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                    try (RecordCursorFactory ignored = compiler.compile("SELECT (id IN(1,2,3))::LONG256,'bad'::UUID FROM lp_wide_cast", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail();
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "invalid UUID constant");
                    }
                }
                assertFactory(retained).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
                try (RecordCursor cursor = retained.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    TestUtils.assertEquals("00000000-0000-0002-0000-000000000001", cursor.getRecord().getStrA(0));
                    Assert.assertEquals("0x04000000000000000300000000000000020000000000000001", cursor.getRecord().getVarcharA(1).toString());
                }
            } finally {
                Misc.free(retained);
            }
        });
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_wide_cast(unused INT,id INT,b BOOLEAN,i8 BYTE,c CHAR,i16 SHORT,i32 INT,i64 LONG,f32 FLOAT,f64 DOUBLE,dt DATE,us TIMESTAMP,ns TIMESTAMP_NS,s STRING,v VARCHAR,sym SYMBOL,u UUID,h LONG256,g5 GEOHASH(5b),g10 GEOHASH(10b),g20 GEOHASH(20b),g60 GEOHASH(60b))");
        execute("INSERT INTO lp_wide_cast VALUES "
                + "(99,1,true,1,'1',257,65537,1234,1.5,2.5,1::DATE,1234::TIMESTAMP,1234567::TIMESTAMP_NS,'00000000-0000-0002-0000-000000000001','0x123456789abcdef','0x123456789abcdef','00000000-0000-0002-0000-000000000001',0x0000000000000004000000000000000300000000000000020000000000000001,#u,#u4,#u4pr,#u4pruydqqvj8),"
                + "(99,2,false,-1,'9',-257,-65537,-1234,-1.5,-2.5,(-1)::DATE,(-1234)::TIMESTAMP,(-1234567)::TIMESTAMP_NS,'bad','bad','bad','00000000-0000-0000-0000-000000000002',0x02,#s,#s0,#s000,#s00000000000),"
                + "(99,3,true,0,'0',0,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null),"
                + "(99,4,false,127,'1',32767,2147483647,9223372036854,1e30,1e30,123456::DATE,1234567::TIMESTAMP,1234567890::TIMESTAMP_NS,'u4pruydqqvj8','00000000-0000-0002-0000-000000000001','0x01','00000000-0000-0002-0000-000000000001',0x01,#u,#u4,#u4pr,#u4pruydqqvj8)");
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
            sql.put(",((").put(source).put(")::").put(targets.getQuick(i)).put(") v").put(i);
        }
        sql.put(" FROM lp_wide_cast ORDER BY id");
        assertCastQuery(sql.toString(), expectedTypes, expectedRows);
        return targets.size();
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
