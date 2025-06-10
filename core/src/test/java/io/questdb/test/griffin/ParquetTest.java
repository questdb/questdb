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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.griffin.SqlException;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Miscellaneous tests for tables with partitions in Parquet format.
 */
public class ParquetTest extends AbstractCairoTest {

    @Test
    public void test3dArray() throws Exception {
        final String arr1 = "ARRAY[\n" +
                "  [\n" +
                "    [1.0, 2.0, 3.0],\n" +
                "    [4.0, 5.0, 6.0],\n" +
                "    [7.0, 8.0, 9.0]\n" +
                "  ],\n" +
                "  [\n" +
                "    [10.0, 11.0, 12.0],\n" +
                "    [13.0, 14.0, 15.0],\n" +
                "    [16.0, 17.0, 18.0]\n" +
                "  ],\n" +
                "  [\n" +
                "    [19.0, 20.0, 21.0],\n" +
                "    [22.0, 23.0, 24.0],\n" +
                "    [25.0, 26.0, 27.0]\n" +
                "  ]\n" +
                "]\n";
        final String arr1exp = arr1
                .replaceAll(" ", "")
                .replaceAll("\n", "")
                .replace("ARRAY", "");
        assertMemoryLeak(() -> {
            execute("create table x (a1 double[][][], ts timestamp) timestamp(ts) partition by month;");
            execute("insert into x values(" + arr1 + ", '2024-04-10T00:00:00.000000Z');");
            execute("insert into x values(" + arr1 + ", '2024-05-10T00:00:00.000000Z');");
            execute("insert into x values(" + arr1 + ", '2024-06-10T00:00:00.000000Z');");
            assertSql(
                    "a1\tts\n"
                            + arr1exp + "\t2024-04-10T00:00:00.000000Z\n"
                            + arr1exp + "\t2024-05-10T00:00:00.000000Z\n"
                            + arr1exp + "\t2024-06-10T00:00:00.000000Z\n"
                    ,
                    "x");

            final SqlException ex = Assert.assertThrows(SqlException.class, () -> execute("alter table x convert partition to parquet where ts >= 0"));
            TestUtils.assertContains(ex.getMessage(), "tables with array columns cannot be converted to Parquet partitions yet");
        });
    }

    @Test
    public void testColTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id long, ts timestamp) timestamp(ts) partition by month;");
            execute("insert into x values(1, '2024-06-10T00:00:00.000000Z');");
            execute("insert into x values(2, '2024-06-11T00:00:00.000000Z');");
            execute("insert into x values(3, '2024-06-12T00:00:00.000000Z');");
            execute("insert into x values(4, '2024-06-12T00:00:01.000000Z');");
            execute("insert into x values(5, '2024-06-15T00:00:00.000000Z');");
            execute("insert into x values(6, '2024-06-12T00:00:02.000000Z');");

            execute("alter table x add column a int;");
            execute("insert into x values(7, '2024-06-10T00:00:00.000000Z', 1);");

            execute("alter table x convert partition to parquet where ts >= 0");
            assertSql(
                    "id\tts\ta\n" +
                            "1\t2024-06-10T00:00:00.000000Z\tnull\n" +
                            "7\t2024-06-10T00:00:00.000000Z\t1\n" +
                            "2\t2024-06-11T00:00:00.000000Z\tnull\n" +
                            "3\t2024-06-12T00:00:00.000000Z\tnull\n" +
                            "4\t2024-06-12T00:00:01.000000Z\tnull\n" +
                            "6\t2024-06-12T00:00:02.000000Z\tnull\n" +
                            "5\t2024-06-15T00:00:00.000000Z\tnull\n",
                    "x"
            );
        });
    }

    @Test
    public void testConvertToNativeFailure() throws Exception {
        // Verify that we aren't closing garbage fds when parquetDecoder.of() fail.
        assertMemoryLeak(() -> {
            execute("create table x (id symbol, ts timestamp) timestamp(ts) partition by day;");
            execute("insert into x values('k1', '2024-06-10T00:00:00.000000Z');");
            execute("insert into x values('k2', '2024-06-11T00:00:00.000000Z');");
            execute("insert into x values('k3', '2024-06-12T00:00:00.000000Z');");

            execute("alter table x convert partition to parquet where ts >= 0");

            long rss = Unsafe.getRssMemUsed();
            // Leave enough room for columnFdAndDataSize list only
            Unsafe.setRssMemLimit(rss + 48);
            try {
                execute("alter table x convert partition to native where ts >= 0");
            } catch (CairoException e) {
                Assert.assertTrue(e.isOutOfMemory());
            } finally {
                Unsafe.setRssMemLimit(0);
            }
        });
    }

    @Test
    public void testDedupFixedKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (x int, ts timestamp) timestamp(ts) partition by day wal DEDUP UPSERT KEYS(ts, x) ;");

            execute("insert into x(x,ts) values (1, '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values (2, '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values (3, '2020-01-03T00:00:00.000Z');");

            drainWalQueue();

            assertSql("x\tts\n" +
                    "1\t2020-01-01T00:00:00.000000Z\n" +
                    "2\t2020-01-02T00:00:00.000000Z\n" +
                    "3\t2020-01-03T00:00:00.000000Z\n", "x");

            drainWalQueue();

            execute("alter table x convert partition to parquet list '2020-01-01', '2020-01-02';");
            assertSql("x\tts\n" +
                    "1\t2020-01-01T00:00:00.000000Z\n" +
                    "2\t2020-01-02T00:00:00.000000Z\n" +
                    "3\t2020-01-03T00:00:00.000000Z\n", "x");

            drainWalQueue();

            execute("insert into x(x,ts) values (11, '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values (1, '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values (2, '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values (22, '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values (33, '2020-01-03T00:00:00.000Z');");

            drainWalQueue();

            assertSql("x\tts\n" +
                    "1\t2020-01-01T00:00:00.000000Z\n" +
                    "11\t2020-01-01T00:00:00.000000Z\n" +
                    "2\t2020-01-02T00:00:00.000000Z\n" +
                    "22\t2020-01-02T00:00:00.000000Z\n" +
                    "3\t2020-01-03T00:00:00.000000Z\n" +
                    "33\t2020-01-03T00:00:00.000000Z\n", "x");
        });
    }

    @Test
    public void testDedupTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (x int, ts timestamp) timestamp(ts) partition by day wal DEDUP UPSERT KEYS(ts) ;");

            execute("insert into x(x,ts) values (1, '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values (2, '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values (3, '2020-01-03T00:00:00.000Z');");

            drainWalQueue();

            assertSql("x\tts\n" +
                    "1\t2020-01-01T00:00:00.000000Z\n" +
                    "2\t2020-01-02T00:00:00.000000Z\n" +
                    "3\t2020-01-03T00:00:00.000000Z\n", "x");

            drainWalQueue();

            execute("alter table x convert partition to parquet list '2020-01-01', '2020-01-02';");
            assertSql("x\tts\n" +
                    "1\t2020-01-01T00:00:00.000000Z\n" +
                    "2\t2020-01-02T00:00:00.000000Z\n" +
                    "3\t2020-01-03T00:00:00.000000Z\n", "x");

            drainWalQueue();

            execute("insert into x(x,ts) values (11, '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values (22, '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values (33, '2020-01-03T00:00:00.000Z');");

            drainWalQueue();

            assertSql("x\tts\n" +
                    "11\t2020-01-01T00:00:00.000000Z\n" +
                    "22\t2020-01-02T00:00:00.000000Z\n" +
                    "33\t2020-01-03T00:00:00.000000Z\n", "x");
        });
    }

    @Test
    public void testDedupVarlenKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (x varchar, ts timestamp) timestamp(ts) partition by day wal DEDUP UPSERT KEYS(ts, x) ;");

            execute("insert into x(x,ts) values ('1', '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('2', '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('3', '2020-01-03T00:00:00.000Z');");

            drainWalQueue();

            assertSql("x\tts\n" +
                    "1\t2020-01-01T00:00:00.000000Z\n" +
                    "2\t2020-01-02T00:00:00.000000Z\n" +
                    "3\t2020-01-03T00:00:00.000000Z\n", "x");

            drainWalQueue();

            execute("alter table x convert partition to parquet list '2020-01-01', '2020-01-02';");
            assertSql("x\tts\n" +
                    "1\t2020-01-01T00:00:00.000000Z\n" +
                    "2\t2020-01-02T00:00:00.000000Z\n" +
                    "3\t2020-01-03T00:00:00.000000Z\n", "x");

            drainWalQueue();

            execute("insert into x(x,ts) values ('100000000001', '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('1', '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('2', '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('200000000002', '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('33', '2020-01-03T00:00:00.000Z');");

            drainWalQueue();

            assertSql("x\tts\n" +
                    "1\t2020-01-01T00:00:00.000000Z\n" +
                    "100000000001\t2020-01-01T00:00:00.000000Z\n" +
                    "2\t2020-01-02T00:00:00.000000Z\n" +
                    "200000000002\t2020-01-02T00:00:00.000000Z\n" +
                    "3\t2020-01-03T00:00:00.000000Z\n" +
                    "33\t2020-01-03T00:00:00.000000Z\n", "x");
        });
    }

    @Test
    public void testFilterAndOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    "id\tts\n" +
                            "3\t1970-01-01T00:33:20.000000Z\n" +
                            "2\t1970-01-01T00:16:40.000000Z\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n",
                    "x where id < 4 order by id desc"
            );
        });
    }

    @Test
    public void testIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id symbol, ts timestamp) timestamp(ts) partition by day;");
            execute("insert into x values('k1', '2024-06-10T00:00:00.000000Z');");
            execute("insert into x values('k2', '2024-06-11T00:00:00.000000Z');");
            execute("insert into x values('k3', '2024-06-12T00:00:00.000000Z');");
            execute("insert into x values('k1', '2024-06-12T00:00:01.000000Z');");
            execute("insert into x values('k2', '2024-06-15T00:00:00.000000Z');");
            execute("insert into x values('k3', '2024-06-12T00:00:02.000000Z');");

            execute("alter table x alter column id add index;");
            execute("insert into x values('k1', '2024-06-10T00:00:00.000000Z');");

            final String expected = "id\tts\n" +
                    "k1\t2024-06-10T00:00:00.000000Z\n" +
                    "k1\t2024-06-10T00:00:00.000000Z\n" +
                    "k1\t2024-06-12T00:00:01.000000Z\n";
            final String query = "x where id = 'k1'";

            execute("alter table x convert partition to parquet where ts >= 0");
            assertSql(expected, query);

            execute("alter table x convert partition to native where ts >= 0");
            assertSql(expected, query);

            execute("alter table x convert partition to parquet where ts >= 0");
            execute("alter table x alter column id drop index;");
            assertSql(expected, query);

            execute("alter table x alter column id add index;");
            assertSql(expected, query);
        });
    }

    // TODO(puzpuzpuz): enable when we support DDLs for parquet partitions
    @Ignore
    @Test
    public void testIndexBumpedColumnVersion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id symbol, ts timestamp) timestamp(ts) partition by day;");

            // bump column version
            execute("alter table x drop column id;");
            execute("alter table x add column id symbol;");

            execute("insert into x (id, ts) values('k1', '2024-06-10T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k2', '2024-06-11T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k3', '2024-06-12T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k1', '2024-06-12T01:00:01.000000Z');");
            execute("insert into x (id, ts) values('k2', '2024-06-15T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k3', '2024-06-12T01:00:02.000000Z');");

            // bump column version one more time
            execute("alter table x alter column id add index;");
            execute("alter table x alter column id drop index;");

            execute("alter table x convert partition to parquet where ts >= 0");

            final String expected = "ts\tid\n" +
                    "2024-06-10T01:00:00.000000Z\tk1\n" +
                    "2024-06-11T01:00:00.000000Z\tk2\n" +
                    "2024-06-12T01:00:00.000000Z\tk3\n" +
                    "2024-06-12T01:00:01.000000Z\tk1\n" +
                    "2024-06-12T01:00:02.000000Z\tk3\n" +
                    "2024-06-15T01:00:00.000000Z\tk2\n";
            final String query = "x";

            assertSql(expected, query);

            execute("alter table x convert partition to native where ts >= 0");
            assertSql(expected, query);
        });
    }

    @Test
    public void testIndexColTopColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp) timestamp(ts) partition by day;");
            execute("insert into x values('2024-06-10T00:00:00.000000Z');");

            execute("alter table x add column id symbol");
            execute("insert into x values('2024-06-11T00:00:00.000000Z', 'k1');");
            execute("insert into x values('2024-06-12T00:00:00.000000Z', 'k2');");
            execute("insert into x values('2024-06-12T00:00:01.000000Z', 'k3');");
            execute("insert into x values('2024-06-15T00:00:00.000000Z', 'k3');");

            execute("alter table x alter column id add index;");
            execute("insert into x values('2024-06-10T00:00:00.000000Z', 'k1');");

            execute("alter table x convert partition to parquet where ts >= 0");
            assertSql(
                    "ts\tid\n" +
                            "2024-06-10T00:00:00.000000Z\tk1\n" +
                            "2024-06-11T00:00:00.000000Z\tk1\n",
                    "x where id = 'k1'"
            );
        });
    }

    @Test
    public void testIndexO3Writes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id symbol, ts timestamp) timestamp(ts) partition by day;");
            execute("insert into x values('k1', '2024-06-10T01:00:00.000000Z');");
            execute("insert into x values('k2', '2024-06-11T01:00:00.000000Z');");
            execute("insert into x values('k3', '2024-06-12T01:00:00.000000Z');");
            execute("insert into x values('k1', '2024-06-12T01:00:01.000000Z');");
            execute("insert into x values('k2', '2024-06-15T01:00:00.000000Z');");
            execute("insert into x values('k3', '2024-06-12T01:00:02.000000Z');");

            execute("alter table x convert partition to parquet where ts >= 0");
            execute("alter table x alter column id add index;");

            execute("insert into x values('k1', '2024-06-10T00:00:00.000000Z');");
            execute("insert into x values('k1', '2024-06-11T00:00:00.000000Z');");
            execute("insert into x values('k1', '2024-06-12T00:00:00.000000Z');");

            final String expected = "id\tts\n" +
                    "k1\t2024-06-10T00:00:00.000000Z\n" +
                    "k1\t2024-06-10T01:00:00.000000Z\n" +
                    "k1\t2024-06-11T00:00:00.000000Z\n" +
                    "k1\t2024-06-12T00:00:00.000000Z\n" +
                    "k1\t2024-06-12T01:00:01.000000Z\n";
            final String query = "x where id = 'k1'";

            assertSql(expected, query);

            execute("alter table x convert partition to native where ts >= 0");
            assertSql(expected, query);
        });
    }

    @Test
    public void testIndexO3WritesBumpedColumnVersion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id symbol, ts timestamp) timestamp(ts) partition by day;");

            execute("insert into x (id, ts) values('k1', '2024-06-10T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k2', '2024-06-11T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k3', '2024-06-12T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k1', '2024-06-12T01:00:01.000000Z');");
            execute("insert into x (id, ts) values('k2', '2024-06-15T01:00:00.000000Z');");
            execute("insert into x (id, ts) values('k3', '2024-06-12T01:00:02.000000Z');");

            // bump column version
            execute("alter table x alter column id add index;");
            execute("alter table x alter column id drop index;");

            execute("alter table x convert partition to parquet where ts >= 0");
            execute("alter table x alter column id add index;");

            execute("insert into x (id, ts) values('k1', '2024-06-10T00:00:00.000000Z');");
            execute("insert into x (id, ts) values('k1', '2024-06-11T00:00:00.000000Z');");
            execute("insert into x (id, ts) values('k1', '2024-06-12T00:00:00.000000Z');");

            final String expected = "id\tts\n" +
                    "k1\t2024-06-10T00:00:00.000000Z\n" +
                    "k1\t2024-06-10T01:00:00.000000Z\n" +
                    "k1\t2024-06-11T00:00:00.000000Z\n" +
                    "k1\t2024-06-12T00:00:00.000000Z\n" +
                    "k1\t2024-06-12T01:00:01.000000Z\n";
            final String query = "x where id = 'k1'";

            assertSql(expected, query);

            execute("alter table x convert partition to native where ts >= 0");
            assertSql(expected, query);
        });
    }

    @Test
    public void testJitFilter() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);

            execute(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n" +
                            "2\t1970-01-01T00:16:40.000000Z\n" +
                            "3\t1970-01-01T00:33:20.000000Z\n",
                    "x where id < 4"
            );
        });
    }

    @Test
    public void testMixedPartitionsNativeLast() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            execute("alter table x convert partition to parquet where ts = '1970-01-01T02'");

            assertSql(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n" +
                            "2\t1970-01-01T00:16:40.000000Z\n" +
                            "3\t1970-01-01T00:33:20.000000Z\n" +
                            "4\t1970-01-01T00:50:00.000000Z\n" +
                            "5\t1970-01-01T01:06:40.000000Z\n" +
                            "6\t1970-01-01T01:23:20.000000Z\n" +
                            "7\t1970-01-01T01:40:00.000000Z\n" +
                            "8\t1970-01-01T01:56:40.000000Z\n" +
                            "9\t1970-01-01T02:13:20.000000Z\n" +
                            "10\t1970-01-01T02:30:00.000000Z\n",
                    "x"
            );
        });
    }

    @Test
    public void testMixedPartitionsParquetLast() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            execute("alter table x convert partition to parquet where ts = '1970-01-01T01'");

            assertSql(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n" +
                            "2\t1970-01-01T00:16:40.000000Z\n" +
                            "3\t1970-01-01T00:33:20.000000Z\n" +
                            "4\t1970-01-01T00:50:00.000000Z\n" +
                            "5\t1970-01-01T01:06:40.000000Z\n" +
                            "6\t1970-01-01T01:23:20.000000Z\n" +
                            "7\t1970-01-01T01:40:00.000000Z\n" +
                            "8\t1970-01-01T01:56:40.000000Z\n" +
                            "9\t1970-01-01T02:13:20.000000Z\n" +
                            "10\t1970-01-01T02:30:00.000000Z\n",
                    "x"
            );
        });
    }

    @Test
    public void testMultiplePartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n" +
                            "2\t1970-01-01T00:16:40.000000Z\n" +
                            "3\t1970-01-01T00:33:20.000000Z\n" +
                            "4\t1970-01-01T00:50:00.000000Z\n" +
                            "5\t1970-01-01T01:06:40.000000Z\n" +
                            "6\t1970-01-01T01:23:20.000000Z\n" +
                            "7\t1970-01-01T01:40:00.000000Z\n" +
                            "8\t1970-01-01T01:56:40.000000Z\n" +
                            "9\t1970-01-01T02:13:20.000000Z\n" +
                            "10\t1970-01-01T02:30:00.000000Z\n",
                    "x"
            );
        });
    }

    @Test
    public void testNonJitFilter() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);

            execute(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n" +
                            "2\t1970-01-01T00:16:40.000000Z\n" +
                            "3\t1970-01-01T00:33:20.000000Z\n",
                    "x where id < 4"
            );
        });
    }

    @Test
    public void testNonWildcardSelect1() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    "1\tts\tid\tid2\tts2\n" +
                            "1\t1970-01-01T00:00:00.000000Z\t1\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "1\t1970-01-01T00:16:40.000000Z\t2\t2\t1970-01-01T00:16:40.000000Z\n" +
                            "1\t1970-01-01T00:33:20.000000Z\t3\t3\t1970-01-01T00:33:20.000000Z\n" +
                            "1\t1970-01-01T00:50:00.000000Z\t4\t4\t1970-01-01T00:50:00.000000Z\n" +
                            "1\t1970-01-01T01:06:40.000000Z\t5\t5\t1970-01-01T01:06:40.000000Z\n" +
                            "1\t1970-01-01T01:23:20.000000Z\t6\t6\t1970-01-01T01:23:20.000000Z\n" +
                            "1\t1970-01-01T01:40:00.000000Z\t7\t7\t1970-01-01T01:40:00.000000Z\n" +
                            "1\t1970-01-01T01:56:40.000000Z\t8\t8\t1970-01-01T01:56:40.000000Z\n" +
                            "1\t1970-01-01T02:13:20.000000Z\t9\t9\t1970-01-01T02:13:20.000000Z\n" +
                            "1\t1970-01-01T02:30:00.000000Z\t10\t10\t1970-01-01T02:30:00.000000Z\n",
                    "select 1, ts, id, id as id2, ts as ts2 from x"
            );
        });
    }

    @Test
    public void testNonWildcardSelect2() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    "ts\n" +
                            "1970-01-01T00:00:00.000000Z\n" +
                            "1970-01-01T00:16:40.000000Z\n" +
                            "1970-01-01T00:33:20.000000Z\n" +
                            "1970-01-01T00:50:00.000000Z\n" +
                            "1970-01-01T01:06:40.000000Z\n" +
                            "1970-01-01T01:23:20.000000Z\n" +
                            "1970-01-01T01:40:00.000000Z\n" +
                            "1970-01-01T01:56:40.000000Z\n" +
                            "1970-01-01T02:13:20.000000Z\n" +
                            "1970-01-01T02:30:00.000000Z\n",
                    "select ts from x"
            );
        });
    }

    @Test
    public void testO3Inserts() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (x int, ts timestamp) timestamp(ts) partition by day;");

            execute("insert into x(x,ts) values ('1', '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('2', '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('3', '2020-01-03T00:00:00.000Z');");

            execute("alter table x convert partition to parquet list '2020-01-02';");
            assertSql(
                    "x\tts\n" +
                            "1\t2020-01-01T00:00:00.000000Z\n" +
                            "2\t2020-01-02T00:00:00.000000Z\n" +
                            "3\t2020-01-03T00:00:00.000000Z\n",
                    "x"
            );

            execute("insert into x(x,ts) values ('1', '2020-01-01T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('2', '2020-01-02T00:00:00.000Z');");
            execute("insert into x(x,ts) values ('3', '2020-01-03T00:00:00.000Z');");

            assertSql(
                    "x\tts\n" +
                            "1\t2020-01-01T00:00:00.000000Z\n" +
                            "1\t2020-01-01T00:00:00.000000Z\n" +
                            "2\t2020-01-02T00:00:00.000000Z\n" +
                            "2\t2020-01-02T00:00:00.000000Z\n" +
                            "3\t2020-01-03T00:00:00.000000Z\n" +
                            "3\t2020-01-03T00:00:00.000000Z\n",
                    "x"
            );
        });
    }

    @Test
    public void testOrderBy1() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            // Order by single long column uses only a single record
            assertSql(
                    "id\tts\n" +
                            "10\t1970-01-01T02:30:00.000000Z\n" +
                            "9\t1970-01-01T02:13:20.000000Z\n" +
                            "8\t1970-01-01T01:56:40.000000Z\n" +
                            "7\t1970-01-01T01:40:00.000000Z\n" +
                            "6\t1970-01-01T01:23:20.000000Z\n" +
                            "5\t1970-01-01T01:06:40.000000Z\n" +
                            "4\t1970-01-01T00:50:00.000000Z\n" +
                            "3\t1970-01-01T00:33:20.000000Z\n" +
                            "2\t1970-01-01T00:16:40.000000Z\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n",
                    "x order by id desc"
            );
        });
    }

    @Test
    public void testOrderBy2() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (\n" +
                            "  select x%5 id1, x id2, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            // Order by single long column uses both records
            assertSql(
                    "id1\tid2\tts\n" +
                            "4\t4\t1970-01-01T00:50:00.000000Z\n" +
                            "4\t9\t1970-01-01T02:13:20.000000Z\n" +
                            "3\t3\t1970-01-01T00:33:20.000000Z\n" +
                            "3\t8\t1970-01-01T01:56:40.000000Z\n" +
                            "2\t2\t1970-01-01T00:16:40.000000Z\n" +
                            "2\t7\t1970-01-01T01:40:00.000000Z\n" +
                            "1\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "1\t6\t1970-01-01T01:23:20.000000Z\n" +
                            "0\t5\t1970-01-01T01:06:40.000000Z\n" +
                            "0\t10\t1970-01-01T02:30:00.000000Z\n",
                    "x order by id1 desc, id2 asc"
            );
        });
    }

    @Test
    public void testSinglePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,10000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by day;"
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            assertSql(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n" +
                            "2\t1970-01-01T00:00:10.000000Z\n" +
                            "3\t1970-01-01T00:00:20.000000Z\n" +
                            "4\t1970-01-01T00:00:30.000000Z\n" +
                            "5\t1970-01-01T00:00:40.000000Z\n" +
                            "6\t1970-01-01T00:00:50.000000Z\n" +
                            "7\t1970-01-01T00:01:00.000000Z\n" +
                            "8\t1970-01-01T00:01:10.000000Z\n" +
                            "9\t1970-01-01T00:01:20.000000Z\n" +
                            "10\t1970-01-01T00:01:30.000000Z\n",
                    "x"
            );
        });
    }

    @Test
    public void testSymbols() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x (\n" +
                            "  id int,\n" +
                            "  ts timestamp,\n" +
                            "  name symbol\n" +
                            ") timestamp(ts) partition by day;"
            );

            // Day 0 -- using every symbol, two nulls
            execute("insert into x values (0, 0, 'SYM_A')");
            execute("insert into x values (1, 10000000, 'SYM_A')");
            execute("insert into x values (2, 20000000, 'SYM_B_junk123')");
            execute("insert into x values (3, 30000000, 'SYM_C_junk123123123123')");
            execute("insert into x values (4, 40000000, 'SYM_D_junk12319993')");
            execute("insert into x values (5, 50000000, 'SYM_E_junk9923')");
            execute("insert into x values (6, 60000000, 'SYM_A')");
            execute("insert into x values (7, 70000000, NULL)");
            execute("insert into x values (8, 80000000, 'SYM_C_junk123123123123')");
            execute("insert into x values (9, 90000000, NULL)");

            // Day 1, NULLS, using two symbols
            execute("insert into x values (10, 86400000000, 'SYM_B_junk123')");
            execute("insert into x values (11, 86410000000, NULL)");
            execute("insert into x values (12, 86420000000, 'SYM_B_junk123')");
            execute("insert into x values (13, 86430000000, 'SYM_B_junk123')");
            execute("insert into x values (14, 86440000000, 'SYM_B_junk123')");
            execute("insert into x values (15, 86450000000, NULL)");
            execute("insert into x values (16, 86460000000, NULL)");
            execute("insert into x values (17, 86470000000, 'SYM_D_junk12319993')");
            execute("insert into x values (18, 86480000000, NULL)");
            execute("insert into x values (19, 86490000000, NULL)");

            // Day 2, no nulls, using first and last symbols only
            execute("insert into x values (20, 172800000000, 'SYM_A')");
            execute("insert into x values (21, 172810000000, 'SYM_A')");
            execute("insert into x values (22, 172820000000, 'SYM_A')");
            execute("insert into x values (23, 172830000000, 'SYM_A')");
            execute("insert into x values (24, 172840000000, 'SYM_E_junk9923')");
            execute("insert into x values (25, 172850000000, 'SYM_E_junk9923')");
            execute("insert into x values (26, 172860000000, 'SYM_E_junk9923')");

            TestUtils.LeakProneCode checkData = () -> assertQueryNoLeakCheck(
                    "id\tts\tname\n" +
                            "0\t1970-01-01T00:00:00.000000Z\tSYM_A\n" +
                            "1\t1970-01-01T00:00:10.000000Z\tSYM_A\n" +
                            "2\t1970-01-01T00:00:20.000000Z\tSYM_B_junk123\n" +
                            "3\t1970-01-01T00:00:30.000000Z\tSYM_C_junk123123123123\n" +
                            "4\t1970-01-01T00:00:40.000000Z\tSYM_D_junk12319993\n" +
                            "5\t1970-01-01T00:00:50.000000Z\tSYM_E_junk9923\n" +
                            "6\t1970-01-01T00:01:00.000000Z\tSYM_A\n" +
                            "7\t1970-01-01T00:01:10.000000Z\t\n" +
                            "8\t1970-01-01T00:01:20.000000Z\tSYM_C_junk123123123123\n" +
                            "9\t1970-01-01T00:01:30.000000Z\t\n" +

                            "10\t1970-01-02T00:00:00.000000Z\tSYM_B_junk123\n" +
                            "11\t1970-01-02T00:00:10.000000Z\t\n" +
                            "12\t1970-01-02T00:00:20.000000Z\tSYM_B_junk123\n" +
                            "13\t1970-01-02T00:00:30.000000Z\tSYM_B_junk123\n" +
                            "14\t1970-01-02T00:00:40.000000Z\tSYM_B_junk123\n" +
                            "15\t1970-01-02T00:00:50.000000Z\t\n" +
                            "16\t1970-01-02T00:01:00.000000Z\t\n" +
                            "17\t1970-01-02T00:01:10.000000Z\tSYM_D_junk12319993\n" +
                            "18\t1970-01-02T00:01:20.000000Z\t\n" +
                            "19\t1970-01-02T00:01:30.000000Z\t\n" +

                            "20\t1970-01-03T00:00:00.000000Z\tSYM_A\n" +
                            "21\t1970-01-03T00:00:10.000000Z\tSYM_A\n" +
                            "22\t1970-01-03T00:00:20.000000Z\tSYM_A\n" +
                            "23\t1970-01-03T00:00:30.000000Z\tSYM_A\n" +
                            "24\t1970-01-03T00:00:40.000000Z\tSYM_E_junk9923\n" +
                            "25\t1970-01-03T00:00:50.000000Z\tSYM_E_junk9923\n" +
                            "26\t1970-01-03T00:01:00.000000Z\tSYM_E_junk9923\n",
                    "x",
                    "ts",
                    true,
                    true
            );

            checkData.run();

            execute("alter table x convert partition to parquet where ts >= 0");

            checkData.run();
        });
    }

    @Test
    public void testTimeFilterMultipleRowGroupPerPartition() throws Exception {
        testTimeFilter(10);
    }

    @Test
    public void testTimeFilterSingleRowGroupPerPartition() throws Exception {
        testTimeFilter(100);
    }

    private void testTimeFilter(int rowGroupSize) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, rowGroupSize);
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(100)\n" +
                            ") timestamp(ts) partition by day;"
            );
            execute("alter table x convert partition to parquet where ts >= 0");

            // fwd
            assertSql(
                    "id\tts\n" +
                            "5\t1970-01-01T01:06:40.000000Z\n" +
                            "6\t1970-01-01T01:23:20.000000Z\n" +
                            "7\t1970-01-01T01:40:00.000000Z\n" +
                            "8\t1970-01-01T01:56:40.000000Z\n",
                    "x where ts in '1970-01-01T01'"
            );
            assertSql(
                    "id\tts\n" +
                            "25\t1970-01-01T06:40:00.000000Z\n" +
                            "26\t1970-01-01T06:56:40.000000Z\n",
                    "x where ts in '1970-01-01T06:30:00.000Z;30m;1d;2'"
            );
            assertSql(
                    "id\tts\n" +
                            "25\t1970-01-01T06:40:00.000000Z\n" +
                            "26\t1970-01-01T06:56:40.000000Z\n" +
                            "28\t1970-01-01T07:30:00.000000Z\n" +
                            "29\t1970-01-01T07:46:40.000000Z\n" +
                            "32\t1970-01-01T08:36:40.000000Z\n" +
                            "33\t1970-01-01T08:53:20.000000Z\n" +
                            "36\t1970-01-01T09:43:20.000000Z\n" +
                            "37\t1970-01-01T10:00:00.000000Z\n",
                    "x where ts in '1970-01-01T06:30:00.000Z;30m;1h;4'"
            );

            // bwd
            assertSql(
                    "id\tts\n" +
                            "8\t1970-01-01T01:56:40.000000Z\n" +
                            "7\t1970-01-01T01:40:00.000000Z\n" +
                            "6\t1970-01-01T01:23:20.000000Z\n" +
                            "5\t1970-01-01T01:06:40.000000Z\n",
                    "x where ts in '1970-01-01T01' order by ts desc"
            );
            assertSql(
                    "id\tts\n" +
                            "26\t1970-01-01T06:56:40.000000Z\n" +
                            "25\t1970-01-01T06:40:00.000000Z\n",
                    "x where ts in '1970-01-01T06:30:00.000Z;30m;1d;2' order by ts desc"
            );
            assertSql(
                    "id\tts\n" +
                            "37\t1970-01-01T10:00:00.000000Z\n" +
                            "36\t1970-01-01T09:43:20.000000Z\n" +
                            "33\t1970-01-01T08:53:20.000000Z\n" +
                            "32\t1970-01-01T08:36:40.000000Z\n" +
                            "29\t1970-01-01T07:46:40.000000Z\n" +
                            "28\t1970-01-01T07:30:00.000000Z\n" +
                            "26\t1970-01-01T06:56:40.000000Z\n" +
                            "25\t1970-01-01T06:40:00.000000Z\n",
                    "x where ts in '1970-01-01T06:30:00.000Z;30m;1h;4' order by ts desc"
            );
        });
    }
}
