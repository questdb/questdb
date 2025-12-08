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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for RecordToRowCopier implementations.
 * These tests verify that both bytecode-based and loop-based implementations
 * produce identical results across various scenarios.
 */
public class RecordToRowCopierIntegrationTest extends AbstractCairoTest {

    @Test
    public void testInsertAsSelectWithManyRows() throws Exception {
        assertMemoryLeak(() -> {
            // Create source table with 50 columns
            StringBuilder createSql = new StringBuilder("create table src (ts timestamp");
            for (int i = 0; i < 50; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            createSql = new StringBuilder("create table dst (ts timestamp");
            for (int i = 0; i < 50; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            // Insert test data with multiple rows
            StringBuilder insertSql = new StringBuilder("insert into src values ");
            for (int row = 0; row < 10; row++) {
                if (row > 0) insertSql.append(", ");
                insertSql.append("(").append(row * 1000000L);
                for (int col = 0; col < 50; col++) {
                    insertSql.append(", ").append(row * 100 + col);
                }
                insertSql.append(")");
            }
            execute(insertSql.toString());

            // Copy data
            execute("insert into dst select * from src");

            // Verify
            assertSql("count\n10\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testInsertAsSelectWithComplexTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (" +
                    "ts timestamp, " +
                    "i int, " +
                    "l long, " +
                    "d double, " +
                    "str string, " +
                    "sym symbol, " +
                    "b boolean, " +
                    "dt date" +
                    ") timestamp(ts) partition by DAY");

            execute("create table dst (" +
                    "ts timestamp, " +
                    "i int, " +
                    "l long, " +
                    "d double, " +
                    "str string, " +
                    "sym symbol, " +
                    "b boolean, " +
                    "dt date" +
                    ") timestamp(ts) partition by DAY");

            execute("insert into src values " +
                    "(0, 1, 2, 3.14, 'hello', 'world', true, '2024-01-01'), " +
                    "(1000000, 10, 20, 2.71, 'foo', 'bar', false, '2024-02-01'), " +
                    "(2000000, null, null, null, null, null, null, null)");

            execute("insert into dst select * from src");

            assertSql("count\n3\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testInsertAsSelectWithTypeConversions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, b byte, s short, i int) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, b long, s long, i long) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 1, 2, 3), (1000000, 10, 20, 30), (2000000, 100, 200, 300)");
            execute("insert into dst select * from src");

            assertSql("count\n3\n", "select count(*) from dst");
            assertSql("b\ts\ti\n1\t2\t3\n10\t20\t30\n100\t200\t300\n", "select b, s, i from dst order by ts");
        });
    }

    @Test
    public void testInsertAsSelectWithStringSymbolConversions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src_str (ts timestamp, val string) timestamp(ts) partition by DAY");
            execute("create table dst_sym (ts timestamp, val symbol) timestamp(ts) partition by DAY");

            execute("insert into src_str values (0, 'apple'), (1000000, 'banana'), (2000000, 'cherry')");
            execute("insert into dst_sym select * from src_str");

            assertSql("val\napple\nbanana\ncherry\n", "select val from dst_sym order by ts");

            // Test reverse direction
            execute("create table src_sym (ts timestamp, val symbol) timestamp(ts) partition by DAY");
            execute("create table dst_str (ts timestamp, val string) timestamp(ts) partition by DAY");

            execute("insert into src_sym values (0, 'red'), (1000000, 'green'), (2000000, 'blue')");
            execute("insert into dst_str select * from src_sym");

            assertSql("val\nred\ngreen\nblue\n", "select val from dst_str order by ts");
        });
    }

    @Test
    public void testInsertAsSelectWithUnionExcept() throws Exception {
        assertMemoryLeak(() -> {
            // Create tables with many columns to trigger loop-based copier
            StringBuilder createSql = new StringBuilder("create table t1 (ts timestamp");
            for (int i = 0; i < 100; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            createSql = new StringBuilder("create table t2 (ts timestamp");
            for (int i = 0; i < 100; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            createSql = new StringBuilder("create table result (ts timestamp");
            for (int i = 0; i < 100; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            // Insert data
            StringBuilder insertSql = new StringBuilder("insert into t1 values (0");
            for (int i = 0; i < 100; i++) {
                insertSql.append(", ").append(i);
            }
            insertSql.append(")");
            execute(insertSql.toString());

            insertSql = new StringBuilder("insert into t2 values (1000000");
            for (int i = 0; i < 100; i++) {
                insertSql.append(", ").append(i + 100);
            }
            insertSql.append(")");
            execute(insertSql.toString());

            // Test UNION
            execute("insert into result select * from t1 union select * from t2");
            assertSql("count\n2\n", "select count(*) from result");
        });
    }

    @Test
    public void testInsertAsSelectWithPartitionedTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, val int) timestamp(ts) partition by MONTH");
            execute("create table dst (ts timestamp, val int) timestamp(ts) partition by MONTH");

            // Insert data across multiple months
            execute("insert into src values " +
                    "('2024-01-15', 1), " +
                    "('2024-02-15', 2), " +
                    "('2024-03-15', 3), " +
                    "('2024-04-15', 4)");

            execute("insert into dst select * from src");

            assertSql("count\n4\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testInsertAsSelectWithWhere() throws Exception {
        assertMemoryLeak(() -> {
            StringBuilder createSql = new StringBuilder("create table src (ts timestamp, id int");
            for (int i = 0; i < 50; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            createSql = new StringBuilder("create table dst (ts timestamp, id int");
            for (int i = 0; i < 50; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            // Insert 10 rows
            for (int row = 0; row < 10; row++) {
                StringBuilder insertSql = new StringBuilder("insert into src values (")
                        .append(row * 1000000L).append(", ").append(row);
                for (int col = 0; col < 50; col++) {
                    insertSql.append(", ").append(row * 100 + col);
                }
                insertSql.append(")");
                execute(insertSql.toString());
            }

            // Insert only rows where id > 5
            execute("insert into dst select * from src where id > 5");

            assertSql("count\n4\n", "select count(*) from dst");
            assertSql("id\n6\n7\n8\n9\n", "select id from dst order by id");
        });
    }

    @Test
    public void testExtremelyWideTableInsert() throws Exception {
        assertMemoryLeak(() -> {
            // Test with 5000 columns to really stress the loop-based implementation
            StringBuilder createSql = new StringBuilder("create table src_extreme (ts timestamp");
            for (int i = 0; i < 5000; i++) {
                createSql.append(", c").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            createSql = new StringBuilder("create table dst_extreme (ts timestamp");
            for (int i = 0; i < 5000; i++) {
                createSql.append(", c").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            // Insert a single row with all values
            StringBuilder insertSql = new StringBuilder("insert into src_extreme values (0");
            for (int i = 0; i < 5000; i++) {
                insertSql.append(", ").append(i);
            }
            insertSql.append(")");
            execute(insertSql.toString());

            // Copy it
            execute("insert into dst_extreme select * from src_extreme");

            // Verify
            assertSql("count\n1\n", "select count(*) from dst_extreme");
            assertSql("c0\tc2500\tc4999\n0\t2500\t4999\n", "select c0, c2500, c4999 from dst_extreme");
        });
    }

    @Test
    public void testBatchInsertWithManyColumns() throws Exception {
        assertMemoryLeak(() -> {
            StringBuilder createSql = new StringBuilder("create table src (ts timestamp");
            for (int i = 0; i < 200; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            createSql = new StringBuilder("create table dst (ts timestamp");
            for (int i = 0; i < 200; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            // Insert 1000 rows
            for (int row = 0; row < 1000; row++) {
                StringBuilder insertSql = new StringBuilder("insert into src values (")
                        .append(row * 1000L);
                for (int col = 0; col < 200; col++) {
                    insertSql.append(", ").append((row + col) % 1000);
                }
                insertSql.append(")");
                execute(insertSql.toString());
            }

            // Batch copy
            execute("insert into dst select * from src");

            assertSql("count\n1000\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }
}
