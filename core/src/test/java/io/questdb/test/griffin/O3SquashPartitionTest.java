/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.TableReader;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class O3SquashPartitionTest extends AbstractGriffinTest {

    @Before
    public void setUp() {
        node1.getConfigurationOverrides().setPartitionO3SplitThreshold(4 << 10);
    }

    @Test
    public void testSplitLastPartition() throws Exception {
        // 4kb prefix split threshold
        node1.getConfigurationOverrides().setPartitionO3SplitThreshold(4 * (2 << 10));
        node1.getConfigurationOverrides().setO3PartitionSplitMaxCount(2);

        compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " -x j," +
                        " rnd_str(5,16,2) as str," +
                        " timestamp_sequence('2020-02-04T00', 60*1000000L) ts" +
                        " from long_sequence(60*(23*2-24))" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        String sqlPrefix = "insert into x " +
                "select" +
                " cast(x as int) * 1000000 i," +
                " -x - 1000000L as j," +
                " rnd_str(5,16,2) as str,";
        compile(
                sqlPrefix +
                        " timestamp_sequence('2020-02-04T20:01', 1000000L) ts" +
                        " from long_sequence(700)",
                sqlExecutionContext
        );

        String partitionsSql = "select minTimestamp, numRows, name from table_partitions('x')";
        assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                "2020-02-04T00:00:00.000000Z\t1202\t2020-02-04\n" +
                "2020-02-04T20:01:00.000000Z\t818\t2020-02-04T200100\n");

        // Partition "2020-02-04" squashed the new update

        compile(sqlPrefix +
                        " timestamp_sequence('2020-02-04T18:01', 60*1000000L) ts" +
                        " from long_sequence(50)",
                sqlExecutionContext
        );

        assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                "2020-02-04T00:00:00.000000Z\t1252\t2020-02-04\n" +
                "2020-02-04T20:01:00.000000Z\t818\t2020-02-04T200100\n");

        try (TableReader ignore = getReader("x")) {
            // Partition "2020-02-04" cannot be squashed with the new update because it's locked by the reader
            compile(sqlPrefix +
                            " timestamp_sequence('2020-02-04T18:01', 1000000L) ts" +
                            " from long_sequence(50)",
                    sqlExecutionContext
            );

            assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                    "2020-02-04T00:00:00.000000Z\t1083\t2020-02-04\n" +
                    "2020-02-04T18:01:00.000000Z\t219\t2020-02-04T180100\n" +
                    "2020-02-04T20:01:00.000000Z\t818\t2020-02-04T200100\n");
        }

        // commit in order, should squash partitions
        compile(sqlPrefix +
                        " timestamp_sequence('2020-02-04T22:01:13', 60*1000000L) ts" +
                        " from long_sequence(50)",
                sqlExecutionContext
        );

        assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                "2020-02-04T00:00:00.000000Z\t1302\t2020-02-04\n" +
                "2020-02-04T20:01:00.000000Z\t868\t2020-02-04T200100\n");

        // commit in order, rolls to next partition, should squash "2020-02-04" to single part
        compile(sqlPrefix +
                        " timestamp_sequence('2020-02-04T22:01:13', 60*1000000L) ts" +
                        " from long_sequence(50)",
                sqlExecutionContext
        );

        // commit in order rolls to the next partition, should squash partition "2020-02-04" to single part
        compile(sqlPrefix +
                        " timestamp_sequence('2020-02-05T01:01:15', 10*60*1000000L) ts" +
                        " from long_sequence(50)",
                sqlExecutionContext
        );

        assertSql(partitionsSql, "minTimestamp\tnumRows\tname\n" +
                "2020-02-04T00:00:00.000000Z\t2220\t2020-02-04\n" +
                "2020-02-05T01:01:15.000000Z\t50\t2020-02-05\n");
    }

    @Test
    public void testSplitMidPartitionCheckIndex() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " timestamp_sequence('2020-02-03T13', 60*1000000L) ts" +
                            " from long_sequence(60*24*2)" +
                            ") timestamp (ts) partition by DAY",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table z as (" +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " rnd_str(5,16,2) as str," +
                            " timestamp_sequence('2020-02-04T23:01', 60*1000000L) ts" +
                            " from long_sequence(50))",
                    sqlExecutionContext
            );

            compiler.compile(
                    "create table y as (select * from x union all select * from z)",
                    sqlExecutionContext
            );

            compiler.compile("insert into x select * from z", sqlExecutionContext);

            TestUtils.assertEquals(
                    compiler,
                    sqlExecutionContext,
                    "y order by ts",
                    "x"
            );
        });
    }
}
