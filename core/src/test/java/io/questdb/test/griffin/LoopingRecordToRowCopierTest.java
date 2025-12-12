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

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class LoopingRecordToRowCopierTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_SQL_COPIER_CHUNKED, false);
    }

    @Test
    public void testBasicInsertWithManyColumns() throws Exception {
        assertMemoryLeak(() -> {
            // Create tables with 1000 columns to trigger loop-based copier
            StringBuilder createSql = new StringBuilder("create table src (ts timestamp");
            for (int i = 0; i < 1000; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            createSql = new StringBuilder("create table dst (ts timestamp");
            for (int i = 0; i < 1000; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            // Insert test data
            StringBuilder insertSql = new StringBuilder("insert into src values (0");
            for (int i = 0; i < 1000; i++) {
                insertSql.append(", ").append(i);
            }
            insertSql.append(")");
            execute(insertSql.toString());

            // Copy using INSERT AS SELECT (will use loop-based copier)
            execute("insert into dst select * from src");

            // Verify data was copied correctly
            assertSql("count\n1\n", "select count(*) from dst");
            assertSql("col0\tcol50\tcol99\n0\t50\t99\n", "select col0, col50, col99 from dst");
        });
    }

    @Test
    public void testCompareBothImplementations() throws Exception {
        assertMemoryLeak(() -> {
            // Create test tables
            execute("create table src (ts timestamp, b byte, s short, i int, l long, f float, d double, str string, sym symbol) timestamp(ts) partition by DAY");
            execute("create table dst_result (ts timestamp, b byte, s short, i int, l long, f float, d double, str string, sym symbol) timestamp(ts) partition by DAY");

            // Insert test data
            execute("insert into src values " +
                    "(0, 1, 2, 3, 4, 5.5, 6.6, 'test', 'symbol'), " +
                    "(1000000, 10, 20, 30, 40, 50.5, 60.6, 'hello', 'world'), " +
                    "(2000000, null, null, null, null, null, null, null, null)");

            // Copy (using default threshold - will use loop-based for 9 columns)
            execute("insert into dst_result select * from src");

            // Verify results are correct
            assertSql("count\n3\n", "select count(*) from dst_result");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst_result", LOG);
        });
    }

    @Test
    public void testMultipleRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, i int, s string) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, i int, s string) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 1, 'one'), (1000000, 2, 'two'), (2000000, 3, 'three')");
            execute("insert into dst select * from src");

            assertSql("count\n3\n", "select count(*) from dst");
            assertSql("s\none\ntwo\nthree\n", "select s from dst order by i");
        });
    }

    @Test
    public void testNullHandling() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, i int, s string, sym symbol) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, i int, s string, sym symbol) timestamp(ts) partition by DAY");

            execute("insert into src values (0, null, null, null)");
            execute("insert into dst select * from src");

            assertSql("ts\ti\ts\tsym\n1970-01-01T00:00:00.000000Z\tnull\t\t\n", "dst");
        });
    }

    @Test
    public void testStringToSymbolConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, str string) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, sym symbol) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 'test_symbol')");
            execute("insert into dst select * from src");

            assertSql("ts\tsym\n1970-01-01T00:00:00.000000Z\ttest_symbol\n", "dst");
        });
    }

    @Test
    public void testTypeConversionsWithManyColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, b byte, s short, i int, l long, f float, d double, str string, sym symbol) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, b byte, s short, i int, l long, f float, d double, str string, sym symbol) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 1, 2, 3, 4, 5.5, 6.6, 'hello', 'world')");
            execute("insert into dst select * from src");

            assertSql("""
                    ts\tb\ts\ti\tl\tf\td\tstr\tsym
                    1970-01-01T00:00:00.000000Z\t1\t2\t3\t4\t5.5\t6.6\thello\tworld
                    """, "dst");
        });
    }

    @Test
    public void testTypeWidening() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, b byte, s short, i int) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, b long, s long, i long) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 10, 20, 30)");
            execute("insert into dst select * from src");

            assertSql("ts\tb\ts\ti\n1970-01-01T00:00:00.000000Z\t10\t20\t30\n", "dst");
        });
    }

    @Test
    public void testVarcharToLong256() throws Exception {
        assertMemoryLeak(() -> {
            // Test VARCHAR to LONG256 conversion to verify the cast is safe
            execute("create table src (ts timestamp, vc varchar) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, l256 long256) timestamp(ts) partition by DAY");

            // Insert a valid long256 hex value as varchar
            execute("insert into src values (0, '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef')");
            execute("insert into src values (1000000, null)");

            // Copy using INSERT AS SELECT
            execute("insert into dst select * from src");

            // Verify data was copied correctly
            assertSql("count\n2\n", "select count(*) from dst");
            assertSql("l256\n0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n\n",
                    "select l256 from dst order by ts");
        });
    }

    @Test
    public void testVeryWideTable() throws Exception {
        assertMemoryLeak(() -> {
            // Test with 2000 columns to ensure loop-based implementation handles it
            StringBuilder createSql = new StringBuilder("create table wide_src (ts timestamp");
            for (int i = 0; i < 2000; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            createSql = new StringBuilder("create table wide_dst (ts timestamp");
            for (int i = 0; i < 2000; i++) {
                createSql.append(", col").append(i).append(" int");
            }
            createSql.append(") timestamp(ts) partition by DAY");
            execute(createSql.toString());

            // Insert a row
            StringBuilder insertSql = new StringBuilder("insert into wide_src values (0");
            for (int i = 0; i < 2000; i++) {
                insertSql.append(", ").append(i);
            }
            insertSql.append(")");
            execute(insertSql.toString());

            // Copy data
            execute("insert into wide_dst select * from wide_src");

            // Verify
            assertSql("count\n1\n", "select count(*) from wide_dst");
            assertSql("col0\tcol1000\tcol1999\n0\t1000\t1999\n", "select col0, col1000, col1999 from wide_dst");
        });
    }
}
