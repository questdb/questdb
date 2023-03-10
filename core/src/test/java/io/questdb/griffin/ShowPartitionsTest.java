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

import org.junit.Test;

public class ShowPartitionsTest extends AbstractGriffinTest {

    @Test
    public void testShowPartitions() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
                    "CREATE TABLE " + tableName + " AS (" +
                            "    SELECT" +
                            "        rnd_symbol('EURO', 'USD', 'OTHER') symbol," +
                            "        rnd_double() * 50.0 price," +
                            "        rnd_double() * 20.0 amount," +
                            "        to_timestamp('2023-01-01', 'yyyy-MM-dd') + x * 60 * 1000000 timestamp" +
                            "    FROM long_sequence(500000)" +
                            "), INDEX(symbol capacity 128) TIMESTAMP(timestamp) PARTITION BY MONTH;",
                    sqlExecutionContext);
            assertQuery(
                    "index\tpartitionBy\ttimestamp\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\twal\tlocation\ttxn\tcv\n" +
                            "0\tMONTH\t2023-01\t2023-01-01T00:01:00.000000Z\t2023-01-31T23:59:00.000000Z\t44639\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitions~\t-1\t-1\n" +
                            "1\tMONTH\t2023-02\t2023-02-01T00:00:00.000000Z\t2023-02-28T23:59:00.000000Z\t40320\t1507328\t1.4 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitions~\t-1\t-1\n" +
                            "2\tMONTH\t2023-03\t2023-03-01T00:00:00.000000Z\t2023-03-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitions~\t-1\t-1\n" +
                            "3\tMONTH\t2023-04\t2023-04-01T00:00:00.000000Z\t2023-04-30T23:59:00.000000Z\t43200\t1638400\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitions~\t-1\t-1\n" +
                            "4\tMONTH\t2023-05\t2023-05-01T00:00:00.000000Z\t2023-05-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitions~\t-1\t-1\n" +
                            "5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-30T23:59:00.000000Z\t43200\t1638400\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitions~\t-1\t-1\n" +
                            "6\tMONTH\t2023-07\t2023-07-01T00:00:00.000000Z\t2023-07-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitions~\t-1\t-1\n" +
                            "7\tMONTH\t2023-08\t2023-08-01T00:00:00.000000Z\t2023-08-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitions~\t-1\t-1\n" +
                            "8\tMONTH\t2023-09\t2023-09-01T00:00:00.000000Z\t2023-09-30T23:59:00.000000Z\t43200\t1638400\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitions~\t-1\t-1\n" +
                            "9\tMONTH\t2023-10\t2023-10-01T00:00:00.000000Z\t2023-10-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitions~\t-1\t-1\n" +
                            "10\tMONTH\t2023-11\t2023-11-01T00:00:00.000000Z\t2023-11-30T23:59:00.000000Z\t43200\t1638400\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitions~\t-1\t-1\n" +
                            "11\tMONTH\t2023-12\t2023-12-01T00:00:00.000000Z\t2023-12-14T05:20:00.000000Z\t19041\t9453568\t1.1 MB\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\ttestShowPartitions~\t-1\t-1\n",
                    "SHOW PARTITIONS FROM " + tableName, null, false, sqlExecutionContext, false);
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
                            "        to_timestamp('2023-01-01', 'yyyy-MM-dd') + x * 60 * 1000000 timestamp" +
                            "    FROM long_sequence(500000)" +
                            "), INDEX(symbol capacity 128) TIMESTAMP(timestamp) PARTITION BY MONTH;",
                    sqlExecutionContext);
            compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '2023-01';");
            assertQuery(
                    "index\tpartitionBy\ttimestamp\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\twal\tlocation\ttxn\tcv\n" +
                            "0\tMONTH\t2023-02\t2023-02-01T00:00:00.000000Z\t2023-02-28T23:59:00.000000Z\t40320\t1507328\t1.4 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsDetachedPartition~\t-1\t-1\n" +
                            "1\tMONTH\t2023-03\t2023-03-01T00:00:00.000000Z\t2023-03-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsDetachedPartition~\t-1\t-1\n" +
                            "2\tMONTH\t2023-04\t2023-04-01T00:00:00.000000Z\t2023-04-30T23:59:00.000000Z\t43200\t1638400\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsDetachedPartition~\t-1\t-1\n" +
                            "3\tMONTH\t2023-05\t2023-05-01T00:00:00.000000Z\t2023-05-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsDetachedPartition~\t-1\t-1\n" +
                            "4\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-30T23:59:00.000000Z\t43200\t1638400\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsDetachedPartition~\t-1\t-1\n" +
                            "5\tMONTH\t2023-07\t2023-07-01T00:00:00.000000Z\t2023-07-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsDetachedPartition~\t-1\t-1\n" +
                            "6\tMONTH\t2023-08\t2023-08-01T00:00:00.000000Z\t2023-08-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsDetachedPartition~\t-1\t-1\n" +
                            "7\tMONTH\t2023-09\t2023-09-01T00:00:00.000000Z\t2023-09-30T23:59:00.000000Z\t43200\t1638400\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsDetachedPartition~\t-1\t-1\n" +
                            "8\tMONTH\t2023-10\t2023-10-01T00:00:00.000000Z\t2023-10-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsDetachedPartition~\t-1\t-1\n" +
                            "9\tMONTH\t2023-11\t2023-11-01T00:00:00.000000Z\t2023-11-30T23:59:00.000000Z\t43200\t1638400\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsDetachedPartition~\t-1\t-1\n" +
                            "10\tMONTH\t2023-12\t2023-12-01T00:00:00.000000Z\t2023-12-14T05:20:00.000000Z\t19041\t9453568\t1.1 MB\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsDetachedPartition~\t-1\t-1\n",
                    "SHOW PARTITIONS FROM " + tableName, null, false, sqlExecutionContext, false);
        });
    }

    @Test
    public void testShowPartitionsFunction() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
                    "CREATE TABLE " + tableName + " AS (" +
                            "    SELECT" +
                            "        rnd_symbol('EURO', 'USD', 'OTHER') symbol," +
                            "        rnd_double() * 50.0 price," +
                            "        rnd_double() * 20.0 amount," +
                            "        to_timestamp('2023-01-01', 'yyyy-MM-dd') + x * 60 * 1000000 timestamp" +
                            "    FROM long_sequence(500000)" +
                            "), INDEX(symbol capacity 128) TIMESTAMP(timestamp) PARTITION BY MONTH;",
                    sqlExecutionContext);
            assertQuery(
                    "index\tpartitionBy\ttimestamp\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\twal\tlocation\ttxn\tcv\n" +
                            "0\tMONTH\t2023-01\t2023-01-01T00:01:00.000000Z\t2023-01-31T23:59:00.000000Z\t44639\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsFunction~\t-1\t-1\n" +
                            "1\tMONTH\t2023-02\t2023-02-01T00:00:00.000000Z\t2023-02-28T23:59:00.000000Z\t40320\t1507328\t1.4 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsFunction~\t-1\t-1\n" +
                            "2\tMONTH\t2023-03\t2023-03-01T00:00:00.000000Z\t2023-03-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsFunction~\t-1\t-1\n" +
                            "3\tMONTH\t2023-04\t2023-04-01T00:00:00.000000Z\t2023-04-30T23:59:00.000000Z\t43200\t1638400\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsFunction~\t-1\t-1\n" +
                            "4\tMONTH\t2023-05\t2023-05-01T00:00:00.000000Z\t2023-05-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsFunction~\t-1\t-1\n" +
                            "5\tMONTH\t2023-06\t2023-06-01T00:00:00.000000Z\t2023-06-30T23:59:00.000000Z\t43200\t1638400\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsFunction~\t-1\t-1\n" +
                            "6\tMONTH\t2023-07\t2023-07-01T00:00:00.000000Z\t2023-07-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsFunction~\t-1\t-1\n" +
                            "7\tMONTH\t2023-08\t2023-08-01T00:00:00.000000Z\t2023-08-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsFunction~\t-1\t-1\n" +
                            "8\tMONTH\t2023-09\t2023-09-01T00:00:00.000000Z\t2023-09-30T23:59:00.000000Z\t43200\t1638400\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsFunction~\t-1\t-1\n" +
                            "9\tMONTH\t2023-10\t2023-10-01T00:00:00.000000Z\t2023-10-31T23:59:00.000000Z\t44640\t1654784\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsFunction~\t-1\t-1\n" +
                            "10\tMONTH\t2023-11\t2023-11-01T00:00:00.000000Z\t2023-11-30T23:59:00.000000Z\t43200\t1638400\t1.6 MB\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsFunction~\t-1\t-1\n" +
                            "11\tMONTH\t2023-12\t2023-12-01T00:00:00.000000Z\t2023-12-14T05:20:00.000000Z\t19041\t9453568\t1.1 MB\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\ttestShowPartitionsFunction~\t-1\t-1\n",
                    "SELECT * FROM table_partitions('" + tableName + "')", null, false, sqlExecutionContext, false);
        });
    }
}
