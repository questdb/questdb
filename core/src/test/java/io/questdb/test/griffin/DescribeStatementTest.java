/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
    public void testDescribeAllColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table all_types (" +
                    "col_boolean boolean, " +
                    "col_byte byte, " +
                    "col_short short, " +
                    "col_int int, " +
                    "col_long long, " +
                    "col_float float, " +
                    "col_double double, " +
                    "col_string string, " +
                    "col_symbol symbol, " +
                    "col_date date, " +
                    "col_timestamp timestamp, " +
                    "col_uuid uuid" +
                    ")");

            assertSql(
                    "ordinal_position\tcolumn_name\tdata_type\n" +
                            "0\tcol_boolean\tBOOLEAN\n" +
                            "1\tcol_byte\tBYTE\n" +
                            "2\tcol_short\tSHORT\n" +
                            "3\tcol_int\tINT\n" +
                            "4\tcol_long\tLONG\n" +
                            "5\tcol_float\tFLOAT\n" +
                            "6\tcol_double\tDOUBLE\n" +
                            "7\tcol_string\tSTRING\n" +
                            "8\tcol_symbol\tSYMBOL\n" +
                            "9\tcol_date\tDATE\n" +
                            "10\tcol_timestamp\tTIMESTAMP\n" +
                            "11\tcol_uuid\tUUID\n",
                    "DESCRIBE (SELECT * FROM all_types)"
            );
        });
    }

    @Test
    public void testDescribeFunctionDirectly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (id int, name string)");

            // Test the describe() function directly first
            assertSql(
                    "ordinal_position\tcolumn_name\tdata_type\n" +
                            "0\tid\tINT\n" +
                            "1\tname\tSTRING\n",
                    "SELECT * FROM describe((SELECT * FROM test_table))"
            );
        });
    }

    @Test
    public void testDescribeJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table table1 (id int, name string)");
            execute("create table table2 (id int, value double)");

            assertSql(
                    "ordinal_position\tcolumn_name\tdata_type\n" +
                            "0\tid\tINT\n" +
                            "1\tname\tSTRING\n" +
                            "2\tid1\tINT\n" +
                            "3\tvalue\tDOUBLE\n",
                    "DESCRIBE (SELECT * FROM table1 JOIN table2 ON table1.id = table2.id)"
            );
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
            execute("create table test_table (id int, name string, value double)");

            assertSql(
                    "ordinal_position\tcolumn_name\tdata_type\n" +
                            "0\tid\tINT\n" +
                            "1\tname\tSTRING\n" +
                            "2\tvalue\tDOUBLE\n",
                    "DESCRIBE (SELECT * FROM test_table)"
            );
        });
    }

    @Test
    public void testDescribeSelectWithAggregation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (id int, value double)");

            assertSql(
                    "ordinal_position\tcolumn_name\tdata_type\n" +
                            "0\tid\tINT\n" +
                            "1\tsum\tDOUBLE\n",
                    "DESCRIBE (SELECT id, sum(value) FROM test_table GROUP BY id)"
            );
        });
    }

    @Test
    public void testDescribeSelectWithAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (id int, value double)");

            assertSql(
                    "ordinal_position\tcolumn_name\tdata_type\n" +
                            "0\tid\tINT\n" +
                            "1\tdouble_value\tDOUBLE\n",
                    "DESCRIBE (SELECT id, value as double_value FROM test_table)"
            );
        });
    }

    @Test
    public void testDescribeSelectWithSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (id int, sym symbol, price double)");

            assertSql(
                    "ordinal_position\tcolumn_name\tdata_type\n" +
                            "0\tid\tINT\n" +
                            "1\tsym\tSYMBOL\n" +
                            "2\tprice\tDOUBLE\n",
                    "DESCRIBE (SELECT * FROM test_table)"
            );
        });
    }

    @Test
    public void testDescribeSimpleSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (id int, name string, ts timestamp) timestamp(ts)");

            assertSql(
                    "ordinal_position\tcolumn_name\tdata_type\n" +
                            "0\tid\tINT\n" +
                            "1\tname\tSTRING\n" +
                            "2\tts\tTIMESTAMP\n",
                    "DESCRIBE (SELECT * FROM test_table)"
            );
        });
    }
}
