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
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalPrimitiveCastTest extends AbstractCairoTest {
    private static final ObjList<String> TYPES = new ObjList<>(
            "BOOLEAN", "BYTE", "CHAR", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "STRING", "VARCHAR"
    );

    @Test
    public void testColumnCastMatrix() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            Assert.assertEquals(83, assertCastMatrix(new ObjList<>(
                    "bool_value", "byte_value", "char_value", "short_value", "int_value", "long_value",
                    "float_value", "double_value", "str_value", "varchar_value", "symbol_value"
            ), new ObjList<>(
                    "INT,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE",
                    "INT,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE"
            ), new ObjList<>(
                    """
                    id	v1	v2	v3	v4	v5	v6	v7	v8	v9
                    1	1	T	1	1	1	1.0	1.0	true	true
                    2	0	F	0	0	0	0.0	0.0	false	false
                    3	1	T	1	1	1	1.0	1.0	true	true
                    4	0	F	0	0	0	0.0	0.0	false	false
                    5	1	T	1	1	1	1.0	1.0	true	true
                    6	0	F	0	0	0	0.0	0.0	false	false
                    """,
                    """
                    id	v0	v2	v3	v4	v5	v6	v7	v8	v9
                    1	true		1	1	1	1.0	1.0	1	1
                    2	false		0	0	0	0.0	0.0	0	0
                    3	true		127	127	127	127.0	127.0	127	127
                    4	true	ﾀ	-128	-128	-128	-128.0	-128.0	-128	-128
                    5	false		0	0	0	0.0	0.0	0	0
                    6	true	￿	-1	-1	-1	-1.0	-1.0	-1	-1
                    """,
                    """
                    id	v0	v1	v3	v4	v5	v6	v7	v8	v9
                    1	true	1	1	1	1	1.0	1.0	1	1
                    2	false	0	0	0	0	0.0	0.0	0	0
                    3	true	1	1	1	1	1.0	1.0	1	1
                    4	false	0	0	0	0	0.0	0.0	0	0
                    5	true	1	1	1	1	1.0	1.0	1	1
                    6	false	0	0	0	0	0.0	0.0	0	0
                    """,
                    """
                    id	v0	v1	v2	v4	v5	v6	v7	v8	v9
                    1	true	1		1	1	1.0	1.0	1	1
                    2	false	0		0	0	0.0	0.0	0	0
                    3	true	1	ā	257	257	257.0	257.0	257	257
                    4	true	0	耀	-32768	-32768	-32768.0	-32768.0	-32768	-32768
                    5	false	0		0	0	0.0	0.0	0	0
                    6	true	-1	翿	32767	32767	32767.0	32767.0	32767	32767
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	true	1		1	1	1
                    2	false	0		0	0	0
                    3	true	1		1	65537	65537
                    4	true	-1	﻿	-257	-257	-257
                    5	false	0		0	\t
                    6	true	1		1	-2147483647	-2147483647
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	true	1		1	1	1
                    2	false	0		0	0	0
                    3	true	1		1	4294967297	4294967297
                    4	true	-1	￿	-1	-4294967297	-4294967297
                    5	false	0		0	\t
                    6	true	-1	￿	-1	9223372036854775807	9223372036854775807
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	true	1		1	1.25	1.25
                    2	false	0		0	-0.0	-0.0
                    3	true	-1	￿	-1	-1.25	-1.25
                    4	true	0	￿	0	1.0E20	1.0E20
                    5	false	0		0	\t
                    6	true	0		0	0.5	0.5
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	true	1		1	1.25	1.25
                    2	false	0		0	-0.0	-0.0
                    3	true	0		0	65537.5	65537.5
                    4	true	0		0	-1.0E20	-1.0E20
                    5	false	0		0	\t
                    6	true	0		0	-0.5	-0.5
                    """,
                    """
                    id	v0	v1	v2	v3	v4	v5	v6	v7
                    1	false	1	1	1	1	1	1.0	1.0
                    2	false	0		0	null	null	null	null
                    3	false	0	1	1024	1024	1024	null	null
                    4	false	0	i	0	null	null	null	null
                    5	false	0		0	null	null	null	null
                    6	false	0	9	0	null	null	1.0E24	1.0E24
                    """,
                    """
                    id	v0	v1	v2	v3	v4	v5	v6	v7
                    1	false	1	1	1	1	1	1.0	1.0
                    2	false	0		0	null	null	null	null
                    3	false	0	1	1024	1024	1024	null	null
                    4	false	0	中	0	null	null	null	null
                    5	false	0		0	null	null	null	null
                    6	true	0	t	0	null	null	null	null
                    """,
                    """
                    id	v1	v2	v3	v4	v5	v6	v7
                    1	1	1	1	1	1	1.0	1.0
                    2	0		0	null	null	null	null
                    3	0	1	1024	1024	1024	null	null
                    4	0	i	0	null	null	null	null
                    5	0		0	null	null	null	null
                    6	0	-	0	null	null	-1.5	-1.5
                    """
            )));
            assertQueryRows(
                    "SELECT id,symbol_value::BYTE,symbol_value::CHAR,symbol_value::SHORT,symbol_value::INT,symbol_value::LONG,symbol_value::FLOAT,symbol_value::DOUBLE FROM (lp_cast UNION ALL lp_cast) ORDER BY id",
                    """
                            id	cast	cast1	cast2	cast3	cast4	cast5	cast6
                            1	1	1	1	1	1	1.0	1.0
                            1	1	1	1	1	1	1.0	1.0
                            2	0		0	null	null	null	null
                            2	0		0	null	null	null	null
                            3	0	1	1024	1024	1024	null	null
                            3	0	1	1024	1024	1024	null	null
                            4	0	i	0	null	null	null	null
                            4	0	i	0	null	null	null	null
                            5	0		0	null	null	null	null
                            5	0		0	null	null	null	null
                            6	0	-	0	null	null	-1.5	-1.5
                            6	0	-	0	null	null	-1.5	-1.5
                            """
            );
        });
    }

    @Test
    public void testConstantCharCaseBranchesPreserveTextTypesAndNativeChildren() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT CASE WHEN id IN (1,2,3) THEN str_value ELSE '0' END val FROM lp_cast ORDER BY id",
                    """
                            val
                            1

                            1_024
                            0
                            0
                            0
                            """
            );
            assertQueryRows(
                    "SELECT CASE WHEN id IN (1,2,3) THEN '中' ELSE varchar_value END val FROM lp_cast ORDER BY id",
                    """
                            val
                            中
                            中
                            中
                            中

                            true
                            """
            );
            assertQueryRows(
                    "SELECT CASE WHEN id IN (1,2,3) THEN symbol_value ELSE '''' END val FROM lp_cast ORDER BY id",
                    """
                            val
                            1

                            1_024
                            '
                            '
                            '
                            """
            );
            assertQueryRows(
                    "SELECT CASE id WHEN 1 THEN '0' WHEN 2 THEN str_value ELSE '中' END val FROM lp_cast ORDER BY id",
                    """
                            val
                            0

                            中
                            中
                            中
                            中
                            """
            );
            assertQueryRows(
                    "SELECT CASE id WHEN 1 THEN varchar_value ELSE '''' END val FROM lp_cast ORDER BY id",
                    """
                            val
                            1
                            '
                            '
                            '
                            '
                            '
                            """
            );
            assertQueryRows(
                    "SELECT CASE id WHEN 1 THEN '0' ELSE symbol_value END val FROM lp_cast ORDER BY id",
                    """
                            val
                            0

                            1_024
                            invalid

                            -1.5
                            """
            );
            assertQueryRows(
                    "SELECT CASE WHEN id>0 THEN char_value WHEN id<0 THEN '0' ELSE str_value END val FROM lp_cast ORDER BY id",
                    """
                            val
                            1
                            0
                            1
                            0
                            1
                            0
                            """
            );
        });
    }

    @Test
    public void testConstantFoldingAndNullText() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            Assert.assertEquals(76, assertCastMatrix(new ObjList<>(
                    "true", "1::BYTE", "'1'::CHAR", "1::SHORT", "1", "1::LONG",
                    "1.25::FLOAT", "1.25::DOUBLE", "'1'::STRING", "'1'::VARCHAR"
            ), new ObjList<>(
                    "INT,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE"
            ), new ObjList<>(
                    """
                    id	v1	v2	v3	v4	v5	v6	v7	v8	v9
                    1	1	T	1	1	1	1.0	1.0	true	true
                    2	1	T	1	1	1	1.0	1.0	true	true
                    3	1	T	1	1	1	1.0	1.0	true	true
                    4	1	T	1	1	1	1.0	1.0	true	true
                    5	1	T	1	1	1	1.0	1.0	true	true
                    6	1	T	1	1	1	1.0	1.0	true	true
                    """,
                    """
                    id	v0	v2	v3	v4	v5	v6	v7	v8	v9
                    1	true		1	1	1	1.0	1.0	1	1
                    2	true		1	1	1	1.0	1.0	1	1
                    3	true		1	1	1	1.0	1.0	1	1
                    4	true		1	1	1	1.0	1.0	1	1
                    5	true		1	1	1	1.0	1.0	1	1
                    6	true		1	1	1	1.0	1.0	1	1
                    """,
                    """
                    id	v0	v1	v3	v4	v5	v6	v7	v8	v9
                    1	true	1	1	1	1	1.0	1.0	1	1
                    2	true	1	1	1	1	1.0	1.0	1	1
                    3	true	1	1	1	1	1.0	1.0	1	1
                    4	true	1	1	1	1	1.0	1.0	1	1
                    5	true	1	1	1	1	1.0	1.0	1	1
                    6	true	1	1	1	1	1.0	1.0	1	1
                    """,
                    """
                    id	v0	v1	v2	v4	v5	v6	v7	v8	v9
                    1	true	1		1	1	1.0	1.0	1	1
                    2	true	1		1	1	1.0	1.0	1	1
                    3	true	1		1	1	1.0	1.0	1	1
                    4	true	1		1	1	1.0	1.0	1	1
                    5	true	1		1	1	1.0	1.0	1	1
                    6	true	1		1	1	1.0	1.0	1	1
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	true	1		1	1	1
                    2	true	1		1	1	1
                    3	true	1		1	1	1
                    4	true	1		1	1	1
                    5	true	1		1	1	1
                    6	true	1		1	1	1
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	true	1		1	1	1
                    2	true	1		1	1	1
                    3	true	1		1	1	1
                    4	true	1		1	1	1
                    5	true	1		1	1	1
                    6	true	1		1	1	1
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	true	1		1	1.25	1.25
                    2	true	1		1	1.25	1.25
                    3	true	1		1	1.25	1.25
                    4	true	1		1	1.25	1.25
                    5	true	1		1	1.25	1.25
                    6	true	1		1	1.25	1.25
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	true	1		1	1.25	1.25
                    2	true	1		1	1.25	1.25
                    3	true	1		1	1.25	1.25
                    4	true	1		1	1.25	1.25
                    5	true	1		1	1.25	1.25
                    6	true	1		1	1.25	1.25
                    """,
                    """
                    id	v0	v1	v2	v3	v4	v5	v6	v7
                    1	false	1	1	1	1	1	1.0	1.0
                    2	false	1	1	1	1	1	1.0	1.0
                    3	false	1	1	1	1	1	1.0	1.0
                    4	false	1	1	1	1	1	1.0	1.0
                    5	false	1	1	1	1	1	1.0	1.0
                    6	false	1	1	1	1	1	1.0	1.0
                    """,
                    """
                    id	v0	v1	v2	v3	v4	v5	v6	v7
                    1	false	1	1	1	1	1	1.0	1.0
                    2	false	1	1	1	1	1	1.0	1.0
                    3	false	1	1	1	1	1	1.0	1.0
                    4	false	1	1	1	1	1	1.0	1.0
                    5	false	1	1	1	1	1	1.0	1.0
                    6	false	1	1	1	1	1	1.0	1.0
                    """
            )));
            assertQueryRows(
                    """
                    SELECT (null::CHAR)::STRING,(null::CHAR)::VARCHAR,
                           (null::INT)::STRING,(null::INT)::VARCHAR,
                           (null::LONG)::STRING,(null::LONG)::VARCHAR,
                           (null::FLOAT)::STRING,(null::FLOAT)::VARCHAR,
                           (null::DOUBLE)::STRING,(null::DOUBLE)::VARCHAR,
                           (null::STRING)::INT,(null::VARCHAR)::CHAR
                    FROM lp_cast LIMIT 1
                    """,
                    """
                            cast	cast1	cast2	cast3	cast4	cast5	cast6	cast7	cast8	cast9	cast10	cast11
                            		null	null	null	null	NaN	NaN	NaN	NaN	null\t
                            """
            );
            assertQueryRows(
                    "SELECT (CASE true WHEN true THEN true WHEN false THEN false ELSE id IN (1,2,3) END)::VARCHAR val FROM lp_cast ORDER BY id",
                    """
                            val
                            true
                            true
                            true
                            true
                            true
                            true
                            """
            );
        });
    }

    @Test
    public void testFactorySurvivesCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT id,(CASE WHEN id IN (1,2,3) THEN int_value ELSE 0 END)::VARCHAR val FROM lp_cast ORDER BY id";
            final String expected = """
                    id	val
                    1	1
                    2	0
                    3	65537
                    4	0
                    5	0
                    6	0
                    """;
            RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                try (RecordCursorFactory factoryBeforeReset = retained) {
                    assertUnknownFunction(compiler, "SELECT (id IN (1,2,3))::VARCHAR,lp_missing_fn(int_value) FROM lp_cast");
                    try (RecordCursorFactory recovered = compiler.compile("SELECT int_value::STRING FROM lp_cast", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(recovered);
                    }
                    assertResult(factoryBeforeReset, expected);
                }
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
            }
            try (RecordCursorFactory factory = retained) {
                assertResult(factory, expected);
            }
        });
    }

    @Test
    public void testInvalidAndNullCharConversions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_cast_bad(id INT,c CHAR)");
            execute("INSERT INTO lp_cast_bad VALUES(1,'x'),(2,null)");
            for (int row = 1; row <= 2; row++) {
                for (int target = 0; target < 8; target++) {
                    if (target != 2) {
                        assertCastError(
                                "SELECT c::" + TYPES.getQuick(target) + " FROM lp_cast_bad WHERE id=" + row,
                                "inconvertible value: " + (row == 1 ? 'x' : '\0') + " [CHAR -> " + (target == 6 ? "DOUBLE" : TYPES.getQuick(target)) + "]"
                        );
                    }
                }
            }
            assertQueryRows(
                    "SELECT id,c::STRING,c::VARCHAR FROM lp_cast_bad ORDER BY id",
                    """
                            id	cast	cast1
                            1	x	x
                            2	\t
                            """
            );
        });
    }

    @Test
    public void testNestedCastsInFiltersAndAggregates() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id FROM lp_cast WHERE str_value::INT>0 OR int_value::BOOLEAN ORDER BY id",
                    """
                            id
                            1
                            3
                            4
                            6
                            """
            );
            assertQueryRows(
                    "SELECT short_value::BYTE k,sum((int_value::STRING)::LONG) total FROM lp_cast GROUP BY 1 ORDER BY k",
                    """
                            k	total
                            -1	-2147483647
                            0	-257
                            1	65538
                            """
            );
            assertQueryRows(
                    "SELECT val FROM (SELECT id,(int_value::VARCHAR)::SHORT val FROM lp_cast) WHERE val>0 ORDER BY val,id",
                    """
                            val
                            1
                            1
                            1
                            """
            );
            assertQueryRows(
                    "SELECT id,(CASE WHEN id IN (1,2,3) THEN str_value ELSE '0' END)::DOUBLE val FROM lp_cast ORDER BY id",
                    """
                            id	val
                            1	1.0
                            2	null
                            3	null
                            4	0.0
                            5	0.0
                            6	0.0
                            """
            );
        });
    }

    @Test
    public void testTypedParametersAndRebinding() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setBoolean(0, true);
            bindVariableService.setByte(1, (byte) 127);
            bindVariableService.setChar(2, '1');
            bindVariableService.setShort(3, (short) 257);
            bindVariableService.setInt(4, 65_537);
            bindVariableService.setLong(5, 4_294_967_297L);
            bindVariableService.setFloat(6, -1.25f);
            bindVariableService.setDouble(7, 65_537.5);
            bindVariableService.setStr(8, "1_024");
            bindVariableService.setVarchar(9, new Utf8String("中"));
            final ObjList<String> parameters = new ObjList<>("$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8", "$9", "$10");
            Assert.assertEquals(76, assertCastMatrix(parameters, new ObjList<>(
                    "INT,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE"
            ), new ObjList<>(
                    """
                    id	v1	v2	v3	v4	v5	v6	v7	v8	v9
                    1	1	T	1	1	1	1.0	1.0	true	true
                    2	1	T	1	1	1	1.0	1.0	true	true
                    3	1	T	1	1	1	1.0	1.0	true	true
                    4	1	T	1	1	1	1.0	1.0	true	true
                    5	1	T	1	1	1	1.0	1.0	true	true
                    6	1	T	1	1	1	1.0	1.0	true	true
                    """,
                    """
                    id	v0	v2	v3	v4	v5	v6	v7	v8	v9
                    1	true		127	127	127	127.0	127.0	127	127
                    2	true		127	127	127	127.0	127.0	127	127
                    3	true		127	127	127	127.0	127.0	127	127
                    4	true		127	127	127	127.0	127.0	127	127
                    5	true		127	127	127	127.0	127.0	127	127
                    6	true		127	127	127	127.0	127.0	127	127
                    """,
                    """
                    id	v0	v1	v3	v4	v5	v6	v7	v8	v9
                    1	true	1	1	1	1	1.0	1.0	1	1
                    2	true	1	1	1	1	1.0	1.0	1	1
                    3	true	1	1	1	1	1.0	1.0	1	1
                    4	true	1	1	1	1	1.0	1.0	1	1
                    5	true	1	1	1	1	1.0	1.0	1	1
                    6	true	1	1	1	1	1.0	1.0	1	1
                    """,
                    """
                    id	v0	v1	v2	v4	v5	v6	v7	v8	v9
                    1	true	1	ā	257	257	257.0	257.0	257	257
                    2	true	1	ā	257	257	257.0	257.0	257	257
                    3	true	1	ā	257	257	257.0	257.0	257	257
                    4	true	1	ā	257	257	257.0	257.0	257	257
                    5	true	1	ā	257	257	257.0	257.0	257	257
                    6	true	1	ā	257	257	257.0	257.0	257	257
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	true	1		1	65537	65537
                    2	true	1		1	65537	65537
                    3	true	1		1	65537	65537
                    4	true	1		1	65537	65537
                    5	true	1		1	65537	65537
                    6	true	1		1	65537	65537
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	true	1		1	4294967297	4294967297
                    2	true	1		1	4294967297	4294967297
                    3	true	1		1	4294967297	4294967297
                    4	true	1		1	4294967297	4294967297
                    5	true	1		1	4294967297	4294967297
                    6	true	1		1	4294967297	4294967297
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	true	-1	￿	-1	-1.25	-1.25
                    2	true	-1	￿	-1	-1.25	-1.25
                    3	true	-1	￿	-1	-1.25	-1.25
                    4	true	-1	￿	-1	-1.25	-1.25
                    5	true	-1	￿	-1	-1.25	-1.25
                    6	true	-1	￿	-1	-1.25	-1.25
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	true	0		0	65537.5	65537.5
                    2	true	0		0	65537.5	65537.5
                    3	true	0		0	65537.5	65537.5
                    4	true	0		0	65537.5	65537.5
                    5	true	0		0	65537.5	65537.5
                    6	true	0		0	65537.5	65537.5
                    """,
                    """
                    id	v0	v1	v2	v3	v4	v5	v6	v7
                    1	false	0	1	1024	1024	1024	null	null
                    2	false	0	1	1024	1024	1024	null	null
                    3	false	0	1	1024	1024	1024	null	null
                    4	false	0	1	1024	1024	1024	null	null
                    5	false	0	1	1024	1024	1024	null	null
                    6	false	0	1	1024	1024	1024	null	null
                    """,
                    """
                    id	v0	v1	v2	v3	v4	v5	v6	v7
                    1	false	0	中	0	null	null	null	null
                    2	false	0	中	0	null	null	null	null
                    3	false	0	中	0	null	null	null	null
                    4	false	0	中	0	null	null	null	null
                    5	false	0	中	0	null	null	null	null
                    6	false	0	中	0	null	null	null	null
                    """
            )));

            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                bindVariableService.setInt(4, 17);
                try (RecordCursorFactory factory = compiler.compile("SELECT $5::VARCHAR val FROM lp_cast LIMIT 1", sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, "val\n17\n");
                    bindVariableService.setInt(4, Numbers.INT_NULL);
                    assertResult(factory, "val\n\n");
                    bindVariableService.setInt(4, -12);
                    assertResult(factory, "val\n-12\n");
                }
            }
            bindVariableService.setBoolean(0, false);
            bindVariableService.setByte(1, (byte) 0);
            bindVariableService.setChar(2, '0');
            bindVariableService.setShort(3, (short) 0);
            bindVariableService.setInt(4, Numbers.INT_NULL);
            bindVariableService.setLong(5, Numbers.LONG_NULL);
            bindVariableService.setFloat(6, Float.NaN);
            bindVariableService.setDouble(7, Double.NaN);
            bindVariableService.setStr(8, null);
            bindVariableService.setVarchar(9, null);
            Assert.assertEquals(76, assertCastMatrix(parameters, new ObjList<>(
                    "INT,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,SHORT,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,INT,LONG,DOUBLE,DOUBLE,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,STRING,VARCHAR",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE",
                    "INT,BOOLEAN,BYTE,CHAR,SHORT,INT,LONG,DOUBLE,DOUBLE"
            ), new ObjList<>(
                    """
                    id	v1	v2	v3	v4	v5	v6	v7	v8	v9
                    1	0	F	0	0	0	0.0	0.0	false	false
                    2	0	F	0	0	0	0.0	0.0	false	false
                    3	0	F	0	0	0	0.0	0.0	false	false
                    4	0	F	0	0	0	0.0	0.0	false	false
                    5	0	F	0	0	0	0.0	0.0	false	false
                    6	0	F	0	0	0	0.0	0.0	false	false
                    """,
                    """
                    id	v0	v2	v3	v4	v5	v6	v7	v8	v9
                    1	false		0	0	0	0.0	0.0	0	0
                    2	false		0	0	0	0.0	0.0	0	0
                    3	false		0	0	0	0.0	0.0	0	0
                    4	false		0	0	0	0.0	0.0	0	0
                    5	false		0	0	0	0.0	0.0	0	0
                    6	false		0	0	0	0.0	0.0	0	0
                    """,
                    """
                    id	v0	v1	v3	v4	v5	v6	v7	v8	v9
                    1	false	0	0	0	0	0.0	0.0	0	0
                    2	false	0	0	0	0	0.0	0.0	0	0
                    3	false	0	0	0	0	0.0	0.0	0	0
                    4	false	0	0	0	0	0.0	0.0	0	0
                    5	false	0	0	0	0	0.0	0.0	0	0
                    6	false	0	0	0	0	0.0	0.0	0	0
                    """,
                    """
                    id	v0	v1	v2	v4	v5	v6	v7	v8	v9
                    1	false	0		0	0	0.0	0.0	0	0
                    2	false	0		0	0	0.0	0.0	0	0
                    3	false	0		0	0	0.0	0.0	0	0
                    4	false	0		0	0	0.0	0.0	0	0
                    5	false	0		0	0	0.0	0.0	0	0
                    6	false	0		0	0	0.0	0.0	0	0
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	false	0		0	\t
                    2	false	0		0	\t
                    3	false	0		0	\t
                    4	false	0		0	\t
                    5	false	0		0	\t
                    6	false	0		0	\t
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	false	0		0	\t
                    2	false	0		0	\t
                    3	false	0		0	\t
                    4	false	0		0	\t
                    5	false	0		0	\t
                    6	false	0		0	\t
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	false	0		0	\t
                    2	false	0		0	\t
                    3	false	0		0	\t
                    4	false	0		0	\t
                    5	false	0		0	\t
                    6	false	0		0	\t
                    """,
                    """
                    id	v0	v1	v2	v3	v8	v9
                    1	false	0		0	\t
                    2	false	0		0	\t
                    3	false	0		0	\t
                    4	false	0		0	\t
                    5	false	0		0	\t
                    6	false	0		0	\t
                    """,
                    """
                    id	v0	v1	v2	v3	v4	v5	v6	v7
                    1	false	0		0	null	null	null	null
                    2	false	0		0	null	null	null	null
                    3	false	0		0	null	null	null	null
                    4	false	0		0	null	null	null	null
                    5	false	0		0	null	null	null	null
                    6	false	0		0	null	null	null	null
                    """,
                    """
                    id	v0	v1	v2	v3	v4	v5	v6	v7
                    1	false	0		0	null	null	null	null
                    2	false	0		0	null	null	null	null
                    3	false	0		0	null	null	null	null
                    4	false	0		0	null	null	null	null
                    5	false	0		0	null	null	null	null
                    6	false	0		0	null	null	null	null
                    """
            )));
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

    private void assertCastError(String sql, String message) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            for (int reopen = 0; reopen < 2; reopen++) {
                try {
                    print(factory);
                    Assert.fail(sql);
                } catch (ImplicitCastException e) {
                    TestUtils.assertEquals(message, e.getFlyweightMessage());
                }
            }
        }
    }

    private int assertCastMatrix(ObjList<String> sources, ObjList<String> expectedTypes, ObjList<String> expectedRows) throws Exception {
        int count = 0;
        for (int source = 0; source < sources.size(); source++) {
            final StringSink sql = new StringSink();
            sql.put("SELECT id");
            for (int target = 0; target < TYPES.size(); target++) {
                // INT/LONG/FLOAT/DOUBLE cross-casts belong to the existing numeric family.
                if (source == target
                        || source >= 4 && source < 8 && target >= 4 && target < 8
                        || source >= 8 && target >= 8
                        || source == 10 && target == 0) {
                    continue;
                }
                sql.put(",(").put(sources.getQuick(source)).put(")::").put(TYPES.getQuick(target)).put(" v").put(target);
                count++;
            }
            sql.put(" FROM lp_cast ORDER BY id");
            assertCastQuery(sql.toString(), expectedTypes.getQuick(source), expectedRows.getQuick(source));
        }
        return count;
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void assertUnknownFunction(SqlCompilerImpl compiler, String sql) throws Exception {
        try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.fail(sql);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "unknown function name");
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_cast(unused INT,id INT,bool_value BOOLEAN,byte_value BYTE,char_value CHAR,short_value SHORT,int_value INT,long_value LONG,float_value FLOAT,double_value DOUBLE,str_value STRING,varchar_value VARCHAR,symbol_value SYMBOL)");
        execute("""
                INSERT INTO lp_cast VALUES
                (91,1,true,1,'1',1,1,1,1.25,1.25,'1','1','1'),
                (92,2,false,0,'0',0,0,0,-0.0,-0.0,'','',''),
                (93,3,true,127,'1',257,65_537,4_294_967_297,-1.25,65537.5,'1_024','1_024','1_024'),
                (94,4,false,-128,'0',-32_768,-257,-4_294_967_297,1e20,-1e20,'invalid','中','invalid'),
                (95,5,true,0,'1',0,null,null,null,null,null,null,null),
                (96,6,false,-1,'0',32_767,-2_147_483_647,9_223_372_036_854_775_807,0.5,-0.5,'999999999999999999999999','true','-1.5')
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
