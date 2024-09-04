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

import io.questdb.cairo.*;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Miscellaneous tests for tables with partitions in Parquet format.
 */
public class ParquetTest extends AbstractCairoTest {

    @Test
    public void testFilterAndOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            ddl(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            ddl("alter table x convert partition to parquet where ts >= 0");

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
    public void testJitFilter() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);

            ddl(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            ddl("alter table x convert partition to parquet where ts >= 0");

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
    public void testMultiplePartitions() throws Exception {
        assertMemoryLeak(() -> {
            ddl(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            ddl("alter table x convert partition to parquet where ts >= 0");

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

            ddl(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            ddl("alter table x convert partition to parquet where ts >= 0");

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
    public void testOrderBy1() throws Exception {
        assertMemoryLeak(() -> {
            ddl(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            ddl("alter table x convert partition to parquet where ts >= 0");

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
            ddl(
                    "create table x as (\n" +
                            "  select x%5 id1, x id2, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            ddl("alter table x convert partition to parquet where ts >= 0");

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
            ddl(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,10000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by day;"
            );
            ddl("alter table x convert partition to parquet where ts >= 0");

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
}
