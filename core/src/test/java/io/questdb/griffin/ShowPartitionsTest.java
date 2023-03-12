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

package io.questdb.griffin;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class ShowPartitionsTest extends AbstractGriffinTest {

    @Test
    public void testShowPartitionsWhenThereAreNoAttachableNorDetached() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
                    "CREATE TABLE " + tableName + " AS (" +
                            "    SELECT" +
                            "        rnd_symbol('EURO', 'USD', 'OTHER') symbol," +
                            "        rnd_double() * 50.0 price," +
                            "        rnd_double() * 20.0 amount," +
                            "        to_timestamp('2023-01-01', 'yyyy-MM-dd') + x * 6 * 3600 * 1000000L timestamp" +
                            "    FROM long_sequence(700)" +
                            "), INDEX(symbol capacity 32) TIMESTAMP(timestamp) PARTITION BY MONTH;",
                    sqlExecutionContext);
            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize (bytes)\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                            "0\tMONTH\t2023-01\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\t98304\t96.0 KB\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                            "1\tMONTH\t2023-02\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\t98304\t96.0 KB\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                            "2\tMONTH\t2023-03\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\t98304\t96.0 KB\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                            "3\tMONTH\t2023-04\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\t98304\t96.0 KB\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                            "4\tMONTH\t2023-05\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\t98304\t96.0 KB\tfalse\tfalse\ttrue\tfalse\tfalse\n" +
                            "5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\t9453568\t9.0 MB\tfalse\ttrue\ttrue\tfalse\tfalse\n",
                    tableName
            );
        });
    }

    @Test
    public void testShowPartitionsDetachedPartition() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
                    "CREATE TABLE " + tableName + " AS (" +
                            "    SELECT" +
                            "        rnd_symbol('EURO', 'USD', 'OTHER') symbol," +
                            "        rnd_double() * 50.0 price," +
                            "        rnd_double() * 20.0 amount," +
                            "        to_timestamp('2023-01-01', 'yyyy-MM-dd') + x * 6 * 3600 * 1000000L timestamp" +
                            "    FROM long_sequence(700)" +
                            "), INDEX(symbol capacity 32) TIMESTAMP(timestamp) PARTITION BY MONTH;",
                    sqlExecutionContext);

            compile("ALTER TABLE " + tableName + " DETACH PARTITION WHERE timestamp < '2023-06-01T00:00:00.000000Z'", sqlExecutionContext);

            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize (bytes)\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                            "0\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\t9453568\t9.0 MB\tfalse\ttrue\ttrue\tfalse\tfalse\n" +
                            "0\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "-1\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "-2\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "-3\tMONTH\t2023-04.detached\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "-4\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n",
                    tableName);
        });
    }

    @Test
    public void testShowPartitionsDetachedPartitionPlusAttachable() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // no links in windows
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
                    "CREATE TABLE " + tableName + " AS (" +
                            "    SELECT" +
                            "        rnd_symbol('EURO', 'USD', 'OTHER') symbol," +
                            "        rnd_double() * 50.0 price," +
                            "        rnd_double() * 20.0 amount," +
                            "        to_timestamp('2023-01-01', 'yyyy-MM-dd') + x * 6 * 3600 * 1000000L timestamp" +
                            "    FROM long_sequence(700)" +
                            "), INDEX(symbol capacity 32) TIMESTAMP(timestamp) PARTITION BY MONTH;",
                    sqlExecutionContext);

            compile("ALTER TABLE " + tableName + " DETACH PARTITION WHERE timestamp < '2023-06-01T00:00:00.000000Z'", sqlExecutionContext);

            // prepare 3 partitions for attachment
            TableToken tableToken = engine.getTableToken(tableName);
            try (
                    Path path = new Path().of(configuration.getRoot()).concat(tableToken).concat("2023-0");
                    Path link = new Path().of(configuration.getRoot()).concat(tableToken).concat("2023-0")
            ) {
                int len = path.length();
                for (int i = 2; i < 5; i++) {
                    path.trimTo(len).put(i).put(TableUtils.DETACHED_DIR_MARKER).$();
                    link.trimTo(len).put(i).put(TableUtils.ATTACHABLE_DIR_MARKER).$();
                    Assert.assertEquals(0, Files.softLink(path, link));
                }
            }

            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize (bytes)\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                            "0\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\t9453568\t9.0 MB\tfalse\ttrue\ttrue\tfalse\tfalse\n" +
                            "0\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "-1\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "-2\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "-3\tMONTH\t2023-04.detached\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "-4\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "-1\tMONTH\t2023-02.attachable\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\ttrue\n" +
                            "-2\tMONTH\t2023-03.attachable\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\ttrue\n" +
                            "-3\tMONTH\t2023-04.attachable\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\ttrue\n",
                    tableName);

            compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '2023-02', '2023-03'", sqlExecutionContext);

            assertShowPartitions(
                    "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize (bytes)\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                            "0\tMONTH\t2023-02\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\t147456\t144.0 KB\ttrue\tfalse\ttrue\tfalse\tfalse\n" +
                            "1\tMONTH\t2023-03\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\t147456\t144.0 KB\ttrue\tfalse\ttrue\tfalse\tfalse\n" +
                            "2\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-25T00:00:00.000000Z\t97\t9453568\t9.0 MB\tfalse\ttrue\ttrue\tfalse\tfalse\n" +
                            "0\tMONTH\t2023-01.detached\t2023-01-01T06:00:00.000000Z\t2023-01-31T18:00:00.000000Z\t123\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "-1\tMONTH\t2023-02.detached\t2023-02-01T00:00:00.000000Z\t2023-02-28T18:00:00.000000Z\t112\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "-2\tMONTH\t2023-03.detached\t2023-03-01T00:00:00.000000Z\t2023-03-31T18:00:00.000000Z\t124\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "-3\tMONTH\t2023-04.detached\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "-4\tMONTH\t2023-05.detached\t2023-05-01T00:00:00.000000Z\t2023-05-31T18:00:00.000000Z\t124\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\tfalse\n" +
                            "-3\tMONTH\t2023-04.attachable\t2023-04-01T00:00:00.000000Z\t2023-04-30T18:00:00.000000Z\t120\t147456\t144.0 KB\tfalse\tfalse\tfalse\ttrue\ttrue\n",
                    tableName);
        });
    }

    private void assertShowPartitions(String expected, String tableName) throws SqlException {
        assertQuery(expected, "SHOW PARTITIONS FROM " + tableName, null, false, sqlExecutionContext, false);
        assertQuery(expected, "SELECT * FROM table_partitions('" + tableName + "')", null, false, sqlExecutionContext, false);
    }
}
