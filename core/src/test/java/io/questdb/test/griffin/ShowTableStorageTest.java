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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class ShowTableStorageTest extends AbstractCairoTest {
    @Test
    public void testAllPartitionsStorageForMultipleTablesPartitionByHour() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades_1(timestamp TIMESTAMP, " +
                    "id SYMBOL , price INT)TIMESTAMP(timestamp) PARTITION BY HOUR;");
            ddl("create table trades_2(timestamp TIMESTAMP, " +
                    "id SYMBOL , price INT)TIMESTAMP(timestamp) PARTITION BY HOUR;");
            insert(
                    "INSERT INTO trades_1\n" +
                            "VALUES\n" +
                            "    ('2021-10-05T11:31:35.878Z', 's1', 245),\n" +
                            "    ('2021-10-05T12:31:35.878Z', 's2', 245),\n" +
                            "    ('2021-10-05T13:31:35.878Z', 's3', 250),\n" +
                            "    ('2021-10-05T14:31:35.878Z', 's4', 250);"
            );
            insert(
                    "INSERT INTO trades_2\n" +
                            "VALUES\n" +
                            "    ('2021-10-05T11:31:35.878Z', 's1', 245),\n" +
                            "    ('2021-10-05T12:31:35.878Z', 's2', 245),\n" +
                            "    ('2021-10-05T13:31:35.878Z', 's3', 250),\n" +
                            "    ('2021-10-05T14:31:35.878Z', 's4', 250);"
            );
            drainWalQueue();
            engine.releaseAllWriters();
            final CharSequence size1 = Long.toString(getDirSize("trades_1"));
            final CharSequence size2 = Long.toString(getDirSize("trades_2"));
            assertSql(
                    "tableName\twalEnabled\tpartitionBy\tpartitionCount\trowCount\tdiskSize\n" +
                            "trades_2\tfalse\tHOUR\t4\t4\t" + size1 + "\n" +
                            "trades_1\tfalse\tHOUR\t4\t4\t" + size2 + "\n",
                    "select * from table_storage()"
            );
        });
    }

    @Test
    public void testAllPartitionsStorageForMultipleTablesWithNoPartitions() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades_1(timestamp TIMESTAMP, " +
                    "id SYMBOL , price INT)TIMESTAMP(timestamp);");
            ddl("create table trades_2(timestamp TIMESTAMP, " +
                    "id SYMBOL , price INT)TIMESTAMP(timestamp);");
            insert(
                    "INSERT INTO trades_1\n" +
                            "VALUES\n" +
                            "    ('2021-10-05T11:31:35.878Z', 's1', 245),\n" +
                            "    ('2021-10-05T12:31:35.878Z', 's2', 245),\n" +
                            "    ('2021-10-05T13:31:35.878Z', 's3', 250),\n" +
                            "    ('2021-10-05T14:31:35.878Z', 's4', 250);"
            );
            insert(
                    "INSERT INTO trades_2\n" +
                            "VALUES\n" +
                            "    ('2021-10-05T11:31:35.878Z', 's1', 245),\n" +
                            "    ('2021-10-05T12:31:35.878Z', 's2', 245),\n" +
                            "    ('2021-10-05T13:31:35.878Z', 's3', 250),\n" +
                            "    ('2021-10-05T14:31:35.878Z', 's4', 250);"
            );
            drainWalQueue();
            engine.releaseAllWriters();
            final CharSequence size1 = Long.toString(getDirSize("trades_1"));
            final CharSequence size2 = Long.toString(getDirSize("trades_2"));
            assertSql(
                    "tableName\twalEnabled\tpartitionBy\tpartitionCount\trowCount\tdiskSize\n" +
                            "trades_2\tfalse\tNONE\t1\t4\t" + size1 + "\n" +
                            "trades_1\tfalse\tNONE\t1\t4\t" + size2 + "\n",
                    "select * from table_storage()"
            );
        });
    }

    @Test
    public void testAllPartitionsStorageForSingleTablePartitionByHour() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades_1(timestamp TIMESTAMP, " +
                    "id SYMBOL , price INT)TIMESTAMP(timestamp) PARTITION BY HOUR;");
            insert(
                    "INSERT INTO trades_1\n" +
                            "VALUES\n" +
                            "    ('2021-10-05T11:31:35.878Z', 's1', 245),\n" +
                            "    ('2021-10-05T12:31:35.878Z', 's2', 245),\n" +
                            "    ('2021-10-05T13:31:35.878Z', 's3', 250),\n" +
                            "    ('2021-10-05T14:31:35.878Z', 's4', 250);\n"
            );
            drainWalQueue();
            engine.releaseAllWriters();
            engine.releaseAllWriters();
            final CharSequence size = Long.toString(getDirSize("trades_1"));
            assertSql("tableName\twalEnabled\tpartitionBy\tpartitionCount\trowCount\tdiskSize\n" +
                            "trades_1\tfalse\tHOUR\t4\t4\t" + size + "\n",
                    "select * from table_storage()"
            );
        });
    }

    @Test
    public void testAllPartitionsStorageForSingleTableWithNoPartitions() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades_1(timestamp TIMESTAMP, " +
                    "id SYMBOL , price INT)TIMESTAMP(timestamp);");
            insert(
                    "INSERT INTO trades_1\n" +
                            "VALUES\n" +
                            "    ('2021-10-05T11:31:35.878Z', 's1', 245),\n" +
                            "    ('2021-10-05T12:31:35.878Z', 's2', 245),\n" +
                            "    ('2021-10-05T13:31:35.878Z', 's3', 250),\n" +
                            "    ('2021-10-05T14:31:35.878Z', 's4', 250);"
            );
            drainWalQueue();
            engine.releaseAllWriters();
            final CharSequence size = Long.toString(getDirSize("trades_1"));
            assertSql("tableName\twalEnabled\tpartitionBy\tpartitionCount\trowCount\tdiskSize\n" +
                            "trades_1\tfalse\tNONE\t1\t4\t" + size + "\n",
                    "select * from table_storage()"
            );
        });
    }

    private long getDirSize(@NotNull CharSequence tableName) {
        Path path = new Path();
        TableToken token = sqlExecutionContext.getTableToken(tableName);
        TableUtils.setPathTable(path, configuration, token);
        final long size = Files.getDirSize(path);
        path.close();
        return size;
    }
}
