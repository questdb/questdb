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

import io.questdb.cairo.PartitionBy;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class AllTablePartitionsTest extends AbstractCairoTest {

    @Test
    public void testAllPartitionsNoTables() throws Exception {
        assertMemoryLeak(() -> {
            assertAllPartitionTables(null);
        });
    }

    @Test
    public void testAllPartitionsEmptyTableOnly() throws Exception {
        String tableName = "empty_table";
        assertMemoryLeak(() -> {
            createTable(tableName, PartitionBy.DAY, 0);

            assertAllPartitionTables(null);
        });
    }


    @Test
    public void testAllPartitionsOneTable() throws Exception {
        String tableName = "aTable";
        String expected = "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\ttableName\n" +
                "0\tMONTH\t2023-01\t2023-01-02T00:00:00.000000Z\t2023-01-08T00:00:00.000000Z\t7\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tTABLE\n";

        assertMemoryLeak(() -> {
            createTable(tableName, PartitionBy.MONTH, 7);
            String finallyExpected = replaceSizeToMatchOS(expected, tableName);
            assertAllPartitionTables(finallyExpected);
        });
    }

    @Test
    public void testAllPartitionsOneTableWithMetadata() throws Exception {
        String tableName = "aTable";
        String expected = "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\ttableName\n" +
                "0\tMONTH\t2023-01\t2023-01-02T00:00:00.000000Z\t2023-01-08T00:00:00.000000Z\t7\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tTABLE\n";

        assertMemoryLeak(() -> {
            createTable(tableName, PartitionBy.MONTH, 7);
            String finallyExpected = replaceSizeToMatchOS(expected, tableName);
            assertQuery(
                    finallyExpected,
                    "select index, partitionBy, name, minTimestamp, maxTimestamp, numRows, diskSize, diskSizeHuman," +
                            " readOnly, active, attached, detached, attachable, tableName from all_table_partitions()" +
                            " where tableName='" + tableName + "'",
                    null,
                    false,
                    false,
                    true
            );
        });
    }


    @Test
    public void testAllPartitionsFewTables() throws Exception {
        String expectedHeader = "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\ttableName";

        String tableName1 = "aTable1";
        String expected1 = "\n0\tMONTH\t2023-01\t2023-01-02T00:00:00.000000Z\t2023-01-08T00:00:00.000000Z\t7\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tTABLE";

        String tableName2 = "aTable2";
        String expected2 = "\n0\tDAY\t2023-01-02\t2023-01-02T00:00:00.000000Z\t2023-01-02T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "1\tDAY\t2023-01-03\t2023-01-03T00:00:00.000000Z\t2023-01-03T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "2\tDAY\t2023-01-04\t2023-01-04T00:00:00.000000Z\t2023-01-04T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "3\tDAY\t2023-01-05\t2023-01-05T00:00:00.000000Z\t2023-01-05T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "4\tDAY\t2023-01-06\t2023-01-06T00:00:00.000000Z\t2023-01-06T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "5\tDAY\t2023-01-07\t2023-01-07T00:00:00.000000Z\t2023-01-07T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "6\tDAY\t2023-01-08\t2023-01-08T00:00:00.000000Z\t2023-01-08T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "7\tDAY\t2023-01-09\t2023-01-09T00:00:00.000000Z\t2023-01-09T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "8\tDAY\t2023-01-10\t2023-01-10T00:00:00.000000Z\t2023-01-10T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "9\tDAY\t2023-01-11\t2023-01-11T00:00:00.000000Z\t2023-01-11T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tTABLE";

        assertMemoryLeak(() -> {
            createTable(tableName1, PartitionBy.MONTH, 7);
            createTable(tableName2, PartitionBy.DAY, 10);
            String finallyExpected1 = replaceSizeToMatchOS(expected1, tableName1).replaceAll("\n$", "");

            String finallyExpected2 = replaceSizeToMatchOS(expected2, tableName2);
            assertAllPartitionTables(expectedHeader + finallyExpected1 + finallyExpected2);
        });
    }

    @Test
    public void testAllPartitionsFewTablesWithEmptyTables() throws Exception {

        String expectedHeader = "index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\ttableName";

        String tableName1 = "aTable1";
        String expected1 = "\n0\tMONTH\t2023-01\t2023-01-02T00:00:00.000000Z\t2023-01-08T00:00:00.000000Z\t7\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tTABLE";

        String tableName2 = "aTable2";
        String expected2 = "\n0\tDAY\t2023-01-02\t2023-01-02T00:00:00.000000Z\t2023-01-02T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "1\tDAY\t2023-01-03\t2023-01-03T00:00:00.000000Z\t2023-01-03T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "2\tDAY\t2023-01-04\t2023-01-04T00:00:00.000000Z\t2023-01-04T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "3\tDAY\t2023-01-05\t2023-01-05T00:00:00.000000Z\t2023-01-05T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "4\tDAY\t2023-01-06\t2023-01-06T00:00:00.000000Z\t2023-01-06T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "5\tDAY\t2023-01-07\t2023-01-07T00:00:00.000000Z\t2023-01-07T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "6\tDAY\t2023-01-08\t2023-01-08T00:00:00.000000Z\t2023-01-08T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "7\tDAY\t2023-01-09\t2023-01-09T00:00:00.000000Z\t2023-01-09T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "8\tDAY\t2023-01-10\t2023-01-10T00:00:00.000000Z\t2023-01-10T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tTABLE\n" +
                "9\tDAY\t2023-01-11\t2023-01-11T00:00:00.000000Z\t2023-01-11T00:00:00.000000Z\t1\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tTABLE";

        String tableName3 = "aTable3";
        String tableName4 = "aTable4";
        assertMemoryLeak(() -> {
            createTable(tableName1, PartitionBy.MONTH, 7);
            createTable(tableName3, PartitionBy.DAY, 0);
            createTable(tableName2, PartitionBy.DAY, 10);
            createTable(tableName4, PartitionBy.DAY, 0);
            String finallyExpected1 = replaceSizeToMatchOS(expected1, tableName1).replaceAll("\n$", "");
            String finallyExpected2 = replaceSizeToMatchOS(expected2, tableName2);
            assertAllPartitionTables(expectedHeader + finallyExpected1 + finallyExpected2);
        });
    }

    private String replaceSizeToMatchOS(String expected, String tableName) {
        return ShowPartitionsTest.replaceSizeToMatchOS(expected, new Utf8String(configuration.getRoot()), tableName, engine);
    }

    private void assertAllPartitionTables(String expected) throws SqlException {
        assertQuery(
                expected,
                "select * from all_table_partitions()",
                null,
                false,
                false,
                true
        );
    }

    public static void createTable(String tableName, int partitionBy, int nRows) throws SqlException {
        String createTable = "CREATE TABLE " + tableName + " AS (" +
                "    SELECT" +
                "        to_timestamp('2023-01-01', 'yyyy-MM-dd') + x * 24 * 3600 * 1000000L timestamp" +
                "    FROM long_sequence(" + nRows + ")" +
                ") TIMESTAMP(timestamp) PARTITION BY " + PartitionBy.toString(partitionBy);
        ddl(createTable);
    }
}

