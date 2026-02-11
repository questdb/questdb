/*******************************************************************************
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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class DescribeStatementTest extends AbstractCairoTest {

    @Test
    public void testDescribeAdditionalColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE extra_types (
                        col_char CHAR,
                        col_varchar VARCHAR,
                        col_long256 LONG256,
                        col_ipv4 IPv4,
                        col_binary BINARY
                    )""");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tcol_char\tCHAR
                    1\tcol_varchar\tVARCHAR
                    2\tcol_long256\tLONG256
                    3\tcol_ipv4\tIPv4
                    4\tcol_binary\tBINARY
                    """, "DESCRIBE (SELECT * FROM extra_types)");
        });
    }

    @Test
    public void testDescribeAllColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE all_types (
                        col_boolean BOOLEAN,
                        col_byte BYTE,
                        col_short SHORT,
                        col_int INT,
                        col_long LONG,
                        col_float FLOAT,
                        col_double DOUBLE,
                        col_string STRING,
                        col_symbol SYMBOL,
                        col_date DATE,
                        col_timestamp TIMESTAMP,
                        col_uuid UUID
                    )""");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tcol_boolean\tBOOLEAN
                    1\tcol_byte\tBYTE
                    2\tcol_short\tSHORT
                    3\tcol_int\tINT
                    4\tcol_long\tLONG
                    5\tcol_float\tFLOAT
                    6\tcol_double\tDOUBLE
                    7\tcol_string\tSTRING
                    8\tcol_symbol\tSYMBOL
                    9\tcol_date\tDATE
                    10\tcol_timestamp\tTIMESTAMP
                    11\tcol_uuid\tUUID
                    """, "DESCRIBE (SELECT * FROM all_types)");
        });
    }

    @Test
    public void testDescribeAfterExplain() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (id INT, name STRING)");

            assertSql("""
                    QUERY PLAN
                    describe()
                    """, "EXPLAIN DESCRIBE (SELECT * FROM test_table)");
        });
    }

    @Test
    public void testDescribeCaseInsensitive() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (id INT, name STRING)");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tid\tINT
                    1\tname\tSTRING
                    """, "describe (SELECT * FROM test_table)");
        });
    }

    @Test
    public void testDescribeFunctionDirectly() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (id INT, name STRING)");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tid\tINT
                    1\tname\tSTRING
                    """, "SELECT * FROM describe((SELECT * FROM test_table))");
        });
    }

    @Test
    public void testDescribeImmediateParenthesis() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (id INT, name STRING)");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tid\tINT
                    1\tname\tSTRING
                    """, "DESCRIBE(SELECT * FROM test_table)");
        });
    }

    @Test
    public void testDescribeJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE table1 (id INT, name STRING)");
            execute("CREATE TABLE table2 (id INT, value DOUBLE)");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tid\tINT
                    1\tname\tSTRING
                    2\tid1\tINT
                    3\tvalue\tDOUBLE
                    """, "DESCRIBE (SELECT * FROM table1 JOIN table2 ON table1.id = table2.id)");
        });
    }

    @Test
    public void testDescribeNestedSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (id INT, value DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tid\tINT
                    1\tvalue\tDOUBLE
                    """, "DESCRIBE (SELECT * FROM (SELECT id, value FROM test_table))");
        });
    }

    @Test
    public void testDescribeNonExistentTable() throws Exception {
        assertMemoryLeak(() -> {
            assertException("DESCRIBE (SELECT * FROM non_existent_table)", 38, "table does not exist [table=non_existent_table]");
        });
    }

    @Test
    public void testDescribeSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (id INT, name STRING, value DOUBLE)");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tid\tINT
                    1\tname\tSTRING
                    2\tvalue\tDOUBLE
                    """, "DESCRIBE (SELECT * FROM test_table)");
        });
    }

    @Test
    public void testDescribeSelectWithAggregation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (id INT, value DOUBLE)");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tid\tINT
                    1\tsum\tDOUBLE
                    """, "DESCRIBE (SELECT id, sum(value) FROM test_table GROUP BY id)");
        });
    }

    @Test
    public void testDescribeSelectWithAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (id INT, value DOUBLE)");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tid\tINT
                    1\tdouble_value\tDOUBLE
                    """, "DESCRIBE (SELECT id, value AS double_value FROM test_table)");
        });
    }

    @Test
    public void testDescribeSelectWithSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (id INT, sym SYMBOL, price DOUBLE)");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tid\tINT
                    1\tsym\tSYMBOL
                    2\tprice\tDOUBLE
                    """, "DESCRIBE (SELECT * FROM test_table)");
        });
    }

    @Test
    public void testDescribeSelectWithCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (id INT, value DOUBLE)");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tcast\tLONG
                    1\tcast1\tDOUBLE
                    """, "DESCRIBE (SELECT id::LONG, value::FLOAT FROM test_table)");
        });
    }

    @Test
    public void testDescribeSelectWithExpressions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (a INT, b INT)");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tcolumn\tINT
                    1\tresult\tINT
                    """, "DESCRIBE (SELECT a + b, a * b AS result FROM test_table)");
        });
    }

    @Test
    public void testDescribeSelectWithSingleColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (id INT, name STRING, value DOUBLE)");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tname\tSTRING
                    """, "DESCRIBE (SELECT name FROM test_table)");
        });
    }

    @Test
    public void testDescribeSimpleSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (id INT, name STRING, ts TIMESTAMP) TIMESTAMP(ts)");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tid\tINT
                    1\tname\tSTRING
                    2\tts\tTIMESTAMP
                    """, "DESCRIBE (SELECT * FROM test_table)");
        });
    }

    @Test
    public void testDescribeUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE table1 (id INT, name STRING)");
            execute("CREATE TABLE table2 (id INT, label STRING)");

            assertSql("""
                    ordinal_position\tcolumn_name\tdata_type
                    0\tid\tINT
                    1\tname\tSTRING
                    """, "DESCRIBE (SELECT * FROM table1 UNION ALL SELECT * FROM table2)");
        });
    }
}
