/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class AllTablesPartitionsFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testEmptyDatabase() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select tableName, partitionBy, name from all_tables_partitions()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("tableName\tpartitionBy\tname\n");
        });
    }

    @Test
    public void testMultipleTablesWithMixedPartitioning() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab_day AS (" +
                            "SELECT x::INT id, timestamp_sequence('2023-01-01', 24 * 3600 * 1_000_000L) ts " +
                            "FROM long_sequence(3)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY");
            execute(
                    "CREATE TABLE tab_none AS (" +
                            "SELECT x::INT id, timestamp_sequence('2023-01-01', 24 * 3600 * 1_000_000L) ts " +
                            "FROM long_sequence(2)" +
                            ") TIMESTAMP(ts)");
            drainWalQueue();
            assertQuery("select tableName, partitionBy, name, numRows, attached from all_tables_partitions() order by tableName, name")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns(
                            "tableName\tpartitionBy\tname\tnumRows\tattached\n" +
                                    "tab_day\tDAY\t2023-01-01\t1\ttrue\n" +
                                    "tab_day\tDAY\t2023-01-02\t1\ttrue\n" +
                                    "tab_day\tDAY\t2023-01-03\t1\ttrue\n" +
                                    "tab_none\tNONE\tdefault\t2\ttrue\n");
        });
    }

    @Test
    public void testNonPartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab_np AS (" +
                            "SELECT x::INT id, timestamp_sequence('2023-01-01', 24 * 3600 * 1_000_000L) ts " +
                            "FROM long_sequence(5)" +
                            ") TIMESTAMP(ts)");
            drainWalQueue();
            assertQuery("select tableName, partitionBy, name, numRows, readOnly, active, attached, detached, attachable from all_tables_partitions()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns(
                            "tableName\tpartitionBy\tname\tnumRows\treadOnly\tactive\tattached\tdetached\tattachable\n" +
                                    "tab_np\tNONE\tdefault\t5\tfalse\ttrue\ttrue\tfalse\tfalse\n");
        });
    }

    @Test
    public void testPartitionByDay() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab_d AS (" +
                            "SELECT x::INT id, timestamp_sequence('2023-01-01', 24 * 3600 * 1_000_000L) ts " +
                            "FROM long_sequence(3)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            assertQuery("select tableName, partitionBy, name, numRows, readOnly, active, attached from all_tables_partitions() order by name")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns(
                            "tableName\tpartitionBy\tname\tnumRows\treadOnly\tactive\tattached\n" +
                                    "tab_d\tDAY\t2023-01-01\t1\tfalse\tfalse\ttrue\n" +
                                    "tab_d\tDAY\t2023-01-02\t1\tfalse\tfalse\ttrue\n" +
                                    "tab_d\tDAY\t2023-01-03\t1\tfalse\ttrue\ttrue\n");
        });
    }

    @Test
    public void testSelectSubsetOfColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab_s AS (" +
                            "SELECT x::INT id, timestamp_sequence('2023-01-01', 24 * 3600 * 1_000_000L) ts " +
                            "FROM long_sequence(2)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            assertQuery("select tableName, name from all_tables_partitions() order by name")
                    .noLeakCheck()
                    .returns(
                            "tableName\tname\n" +
                                    "tab_s\t2023-01-01\n" +
                                    "tab_s\t2023-01-02\n");
        });
    }

    @Test
    public void testTimestampColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab_ts AS (" +
                            "SELECT x::INT id, timestamp_sequence('2023-01-01', 24 * 3600 * 1_000_000L) ts " +
                            "FROM long_sequence(2)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            assertQuery("select tableName, name, minTimestamp, maxTimestamp from all_tables_partitions() order by name")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns(
                            "tableName\tname\tminTimestamp\tmaxTimestamp\n" +
                                    "tab_ts\t2023-01-01\t2023-01-01T00:00:00.000000Z\t2023-01-01T00:00:00.000000Z\n" +
                                    "tab_ts\t2023-01-02\t2023-01-02T00:00:00.000000Z\t2023-01-02T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testWithWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab_w1 AS (" +
                            "SELECT x::INT id, timestamp_sequence('2023-01-01', 24 * 3600 * 1_000_000L) ts " +
                            "FROM long_sequence(2)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY");
            execute(
                    "CREATE TABLE tab_w2 AS (" +
                            "SELECT x::INT id, timestamp_sequence('2023-02-01', 24 * 3600 * 1_000_000L) ts " +
                            "FROM long_sequence(2)" +
                            ") TIMESTAMP(ts) PARTITION BY MONTH");
            drainWalQueue();
            assertQuery("select tableName, partitionBy, name from all_tables_partitions() where tableName = 'tab_w1' order by name")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns(
                            "tableName\tpartitionBy\tname\n" +
                                    "tab_w1\tDAY\t2023-01-01\n" +
                                    "tab_w1\tDAY\t2023-01-02\n");
        });
    }
}