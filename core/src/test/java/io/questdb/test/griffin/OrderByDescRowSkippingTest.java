/*******************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.cairo.FullPartitionFrameCursorFactory;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRowCursorFactory;
import io.questdb.std.IntList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * This class tests row skipping (in ascending order) optimizations for tables:
 * - with and without designated timestamps,
 * - non-partitioned and partitioned .
 */
public class OrderByDescRowSkippingTest extends AbstractCairoTest {

    private static final String DATA = """
            10\t2022-01-13T10:00:00.000000Z
            9\t2022-01-12T06:13:20.000000Z
            8\t2022-01-11T02:26:40.000000Z
            7\t2022-01-09T22:40:00.000000Z
            6\t2022-01-08T18:53:20.000000Z
            """;
    private static final String EXPECTED = "rectype\tcreaton\n" + DATA;

    // partitioned table with designated timestamp and two partitions, 5 rows per partition
    @Test
    public void test2partitionsSelectAll() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQueryExpectSize("l\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n", "select l from tab order by ts desc");
        });
    }

    @Test
    public void test2partitionsSelectFirstN() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n10\n9\n8\n", "select l from tab order by ts desc limit 3", true);
        });
    }

    @Test
    public void test2partitionsSelectFirstNwithSameLoHireturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n", "select l from tab order by ts desc limit 8,8", true);
        });
    }

    @Test
    public void test2partitionsSelectLastN() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n3\n2\n1\n", "select l from tab order by ts desc limit -3", true);
        });
    }

    @Test
    public void test2partitionsSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n", "select l from tab order by ts desc limit -8,-8", true);
        });
    }

    @Test
    public void test2partitionsSelectMiddleNfromBothDirections() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 4,-4", true);
        });
    }

    @Test
    public void test2partitionsSelectMiddleNfromEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n8\n7\n6\n", "select l from tab order by ts desc limit -8,-5", true);
        });
    }

    @Test
    public void test2partitionsSelectMiddleNfromStart() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n5\n4\n3\n", "select l from tab order by ts desc limit 5,8", true);
        });
    }

    @Test
    public void test2partitionsSelectNbeforeStartReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("select l from tab order by ts desc limit -11,-15");
        });
    }

    @Test
    public void test2partitionsSelectNbeyondEndReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n", "select l from tab order by ts desc limit 11,12", true);
        });
    }

    @Test
    public void test2partitionsSelectNintersectingEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n2\n1\n", "select l from tab order by ts desc limit 8,12", true);
        });
    }

    @Test
    public void test2partitionsSelectNintersectingStart() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n10\n9\n", "select l from tab order by ts desc limit -12,-8", true);
        });
    }

    @Test
    public void testEmptyTableSelectFirstNReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("l\n", "select l from tab order by ts desc limit 3", true);
        });
    }

    @Test
    public void testEmptyTableSelectFirstNwithSameLoHireturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("l\n", "select l from tab order by ts desc limit 8,8", true);
        });
    }

    @Test
    public void testEmptyTableSelectLastNReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("l\n", "select l from tab order by ts desc limit -3", true);
        });
    }

    @Test
    public void testEmptyTableSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("l\n", "select l from tab order by ts desc limit -8,-8", true);
        });
    }

    @Test
    public void testEmptyTableSelectMiddleNfromBothDirections() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("l\n", "select l from tab order by ts desc limit 4,-4", true);
        });
    }

    @Test
    public void testEmptyTableSelectMiddleNfromEndReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("l\n", "select l from tab order by ts desc limit -8,-5", true);
        });
    }

    @Test
    public void testEmptyTableSelectMiddleNfromStartReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("l\n", "select l from tab order by ts desc limit 5,8", true);
        });
    }

    @Test
    public void testEmptyTableSelectNbeforeStartReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("l\n", "select l from tab order by ts desc limit -11,-15", true);
        });
    }

    @Test
    public void testEmptyTableSelectNbeyondEndReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("l\n", "select l from tab order by ts desc limit 11,12", true);
        });
    }

    @Test
    public void testEmptyTableSelectNintersectingEndReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("l\n", "select l from tab order by ts desc limit 8,12", true);
        });
    }

    @Test
    public void testEmptyTableSelectNintersectingStartReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("l\n", "select l from tab order by ts desc limit -12,-8", true);
        });
    }

    // empty table with designated timestamp
    @Test
    public void testEmptyTableSelect_allReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("select l from tab order by ts desc");
        });
    }

    // normal table without designated timestamp with rows in ascending
    @Test
    public void testNoDesignatedTsTableSelectAll() throws Exception {
        assertMemoryLeak(() -> {
            prepareNonDesignatedTsTable();

            assertQueryExpectSize("l\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n", "select l from tab order by ts desc");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectFirstN() throws Exception {
        assertMemoryLeak(() -> {
            prepareNonDesignatedTsTable();

            assertQueryExpectSize("l\n10\n9\n8\n", "select l from tab order by ts desc limit 3");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectFirstNwithSameLoHireturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepareNonDesignatedTsTable();

            assertQuery("select l from tab order by ts desc limit 8,8");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectLastN() throws Exception {
        assertMemoryLeak(() -> {
            prepareNonDesignatedTsTable();

            assertQueryExpectSize("l\n3\n2\n1\n", "select l from tab order by ts desc limit -3");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepareNonDesignatedTsTable();

            assertQuery("l\n", "select l from tab order by ts desc limit -8,-8", true);
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectMiddleNfromBothDirections() throws Exception {
        assertMemoryLeak(() -> {
            prepareNonDesignatedTsTable();

            assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 4,-4", true);
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectMiddleNfromEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepareNonDesignatedTsTable();

            assertQueryExpectSize("l\n8\n7\n6\n", "select l from tab order by ts desc limit -8,-5");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectMiddleNfromStart() throws Exception {
        assertMemoryLeak(() -> {
            prepareNonDesignatedTsTable();

            assertQueryExpectSize("l\n5\n4\n3\n", "select l from tab order by ts desc limit 5,8");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectNbeforeStartReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepareNonDesignatedTsTable();

            assertQuery("select l from tab order by ts desc limit -11,-15");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectNbeyondEndReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepareNonDesignatedTsTable();

            assertQuery("select l from tab order by ts desc limit 11,12");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectNintersectingEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepareNonDesignatedTsTable();

            assertQueryExpectSize("l\n2\n1\n", "select l from tab order by ts desc limit 8,12");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectNintersectingStart() throws Exception {
        assertMemoryLeak(() -> {
            prepareNonDesignatedTsTable();

            assertQueryExpectSize("l\n10\n9\n", "select l from tab order by ts desc limit -12,-8");
        });
    }

    // normal table without designated timestamp with rows (including duplicates) in descending order
    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectAll() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQueryExpectSize("l\n10\n10\n9\n9\n8\n8\n7\n7\n6\n6\n5\n5\n4\n4\n3\n3\n2\n2\n1\n1\n", "select l from tab order by ts desc");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectFirstN() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQueryExpectSize("l\n10\n10\n9\n", "select l from tab order by ts desc limit 3");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectFirstNwithSameLoHireturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQuery("select l from tab order by ts desc limit 8,8");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectLastN() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQueryExpectSize("l\n2\n1\n1\n", "select l from tab order by ts desc limit -3");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQuery("select l from tab order by ts desc limit -8,-8");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectMiddleNfromBothDirections() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 9,-9", true);
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectMiddleNfromEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQueryExpectSize("l\n9\n9\n8\n", "select l from tab order by ts desc limit -18,-15");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectMiddleNfromStart() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQueryExpectSize("l\n3\n2\n2\n", "select l from tab order by ts desc limit 15,18");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNbeforeStartReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQuery("select l from tab order by ts desc limit -25,-21");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNbeyondEndReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQuery("select l from tab order by ts desc limit 21,22");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNintersectingEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQueryExpectSize("l\n1\n1\n", "select l from tab order by ts desc limit 18,22");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNintersectingStart() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQueryExpectSize("l\n10\n10\n", "select l from tab order by ts desc limit -22,-18");
        });
    }

    // regular table with designated timestamp and  one partition
    @Test
    public void testNormalTableSelectAll() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQueryExpectSize("l\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n", "select l from tab order by ts desc");
        });
    }

    @Test
    public void testNormalTableSelectFirstN() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n10\n9\n8\n", "select l from tab order by ts desc limit 3", true);
        });
    }

    @Test
    public void testNormalTableSelectFirstNwithSameLoHireturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n", "select l from tab order by ts desc limit 8,8", true);
        });
    }

    @Test
    public void testNormalTableSelectLastN() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n3\n2\n1\n", "select l from tab order by ts desc limit -3", true);
        });
    }

    @Test
    public void testNormalTableSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n", "select l from tab order by ts desc limit -8,-8", true);
        });
    }

    @Test
    public void testNormalTableSelectMiddleNfromBothDirections() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 4,-4", true);
        });
    }

    @Test
    public void testNormalTableSelectMiddleNfromEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n8\n7\n6\n", "select l from tab order by ts desc limit -8,-5", true);
        });
    }

    @Test
    public void testNormalTableSelectMiddleNfromStart() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n5\n4\n3\n", "select l from tab order by ts desc limit 5,8", true);
        });
    }

    @Test
    public void testNormalTableSelectNbeforeStartReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n", "select l from tab order by ts desc limit -11,-15", true);
        });
    }

    @Test
    public void testNormalTableSelectNbeyondEndReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n", "select l from tab order by ts desc limit 11,12", true);
        });
    }

    @Test
    public void testNormalTableSelectNintersectingEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n2\n1\n", "select l from tab order by ts desc limit 8,12", true);
        });
    }

    @Test
    public void testNormalTableSelectNintersectingStart() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n10\n9\n", "select l from tab order by ts desc limit -12,-8", true);
        });
    }

    // partitioned table with designated timestamp and 10 partitions one row per partition
    @Test
    public void testPartitionPerRowSelectAll() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQueryExpectSize("l\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n", "select l from tab order by ts desc");
        });
    }

    @Test
    public void testPartitionPerRowSelectFirstN() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n10\n9\n8\n", "select l from tab order by ts desc limit 3", true);
        });
    }

    // special cases
    @Test
    public void testPartitionPerRowSelectFirstNorderedByMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQueryExpectSize("l\n10\n9\n8\n", "select l from tab order by ts desc, l desc limit 3");
        });
    }

    @Test
    public void testPartitionPerRowSelectFirstNorderedByNonTsColumn() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQueryExpectSize("l\n10\n9\n8\n", "select l from tab order by l desc limit 3");
        });
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentCaseInOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTableWithLongNames();

            assertQuery("record_type\n10\n9\n8\n7\n6\n", "select record_type from trips order by created_ON desc limit 5", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentCaseInSelectAndOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTableWithLongNames();

            assertQueryNoLeakCheck(
                    "record_Type\tCREATED_on\n" + DATA,
                    "select record_Type, CREATED_on from trips order by created_ON desc limit 5",
                    null,
                    "CREATED_on###DESC",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentCaseInSelectAndOrderByV2() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTableWithLongNames();

            assertQueryNoLeakCheck(
                    "record_Type\tCREATED_ON\n" + DATA,
                    "select record_Type, CREATED_ON from trips order by created_on desc limit 5",
                    null,
                    "CREATED_ON###DESC",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentCaseInSelectAndOrderByWithAlias() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTableWithLongNames();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertQueryNoLeakCheck(
                        compiler,
                        "record_Type\tcre_on\n" + DATA,
                        "select record_Type, CREATED_ON as cre_on from trips order by created_on desc limit 5",
                        "cre_on###DESC",
                        true,
                        sqlExecutionContext,
                        true
                );
            }
        });
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentCaseInSelectAndOrderByWithAliasV2() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTableWithLongNames();

            assertQueryNoLeakCheck(
                    "record_Type\tcre_on\n" + DATA,
                    "select record_Type, CREATED_ON cre_on from trips order by created_on desc limit 5",
                    null,
                    "cre_on###DESC",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentNameInSubquery() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTableWithLongNames();

            assertQuery(
                    EXPECTED,
                    "select rectype, creaton from " +
                            "( select record_Type as rectype, CREATED_ON creaton from trips order by created_on desc limit 5)",
                    null,
                    "creaton###DESC",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentNameInSubqueryV2() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTableWithLongNames();

            assertQuery(
                    EXPECTED,
                    "select rectype, creaton from " +
                            "( select record_Type as rectype, CREATED_ON creaton " +
                            "from trips " +
                            "order by created_on desc) " +
                            "limit 5",
                    null,
                    "creaton###DESC",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentNameInSubqueryV3() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTableWithLongNames();
            // order by advice is not available in the sub-query ... sort performed by limitRecordCursor
            assertQueryNoLeakCheck(
                    EXPECTED,
                    "select rectype, creaton from " +
                            "( select record_Type as rectype, CREATED_ON creaton from trips) " +
                            "order by creaton desc limit 5",
                    null,
                    "creaton###DESC",
                    true,
                    true
            );
        });
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithSameLoHireturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("select l from tab order by ts desc limit 8,8");
        });
    }

    @Test
    public void testPartitionPerRowSelectLastN() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n3\n2\n1\n", "select l from tab order by ts desc limit -3", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectLastNorderedByMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQueryExpectSize("l\n3\n2\n1\n", "select l from tab order by ts desc, l desc limit -3");
        });
    }

    @Test
    public void testPartitionPerRowSelectLastNorderedByNonTsColumn() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQueryExpectSize("l\n3\n2\n1\n", "select l from tab order by l desc limit -3");
        });
    }

    @Test
    public void testPartitionPerRowSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n", "select l from tab order by ts desc limit -8,-8", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectMiddleNfromBothDirections() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 4,-4", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectMiddleNfromEnd() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n8\n7\n6\n", "select l from tab order by ts desc limit -8,-5", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectMiddleNfromStart() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n5\n4\n3\n", "select l from tab order by ts desc limit 5,8", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectNbeforeStartReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n", "select l from tab order by ts desc limit -11,-15", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectNbeyondEndReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n", "select l from tab order by ts desc limit 11,12", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectNintersectingEnd() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n2\n1\n", "select l from tab order by ts desc limit 8,12", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectNintersectingStart() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n10\n9\n", "select l from tab order by ts desc limit -12,-8", true);
        });
    }

    // tests "partitionIndex == partitionCount - 1" conditional in FullBwdPartitionFrameCursor.skipTo()
    @Test
    public void testSkipBeyondEndOfNonemptyTableReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTableWithLongNames();

            try (
                    TableReader reader = getReader("trips");
                    RecordCursorFactory factory = prepareFactory(reader);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                RecordCursor.Counter counter = new RecordCursor.Counter();
                counter.set(11);
                cursor.skipRows(counter);

                Assert.assertTrue(counter.get() > 0);
                Assert.assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testSkipOverEmptyTableWith1EmptyPartitionReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            runQueries("CREATE TABLE trips(record_type long, created_on TIMESTAMP) timestamp(created_on) partition by none;");

            try (TableWriter writer = getWriter("trips")) {
                TableWriter.Row row = writer.newRow(0L);
                row.putLong(0, 0L);
                row.append();

                row = writer.newRow(100L);
                row.putLong(0, 1L);
                row.append();

                try (
                        TableReader reader = getReader("trips");
                        RecordCursorFactory factory = prepareFactory(reader);
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    RecordCursor.Counter counter = new RecordCursor.Counter();
                    counter.set(1);
                    cursor.skipRows(counter);

                    Assert.assertEquals(1, counter.get());
                    Assert.assertFalse(cursor.hasNext());
                }

                writer.rollback();
            }
        });
    }

    // tests "partitionCount < 1" conditional in FullBwdPartitionFrameCursor.skipTo()
    @Test
    public void testSkipOverEmptyTableWithNoPartitionsReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            runQueries("CREATE TABLE trips(record_type long, created_on TIMESTAMP) timestamp(created_on) partition by day;");

            try (
                    TableReader reader = getReader("trips");
                    RecordCursorFactory factory = prepareFactory(reader);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                RecordCursor.Counter counter = new RecordCursor.Counter();
                counter.set(1);
                cursor.skipRows(counter);

                Assert.assertEquals(1, counter.get());
                Assert.assertFalse(cursor.hasNext());
            }
        });
    }

    private void assertQuery(String query) throws Exception {
        assertQueryNoLeakCheck(
                "l\n",
                query,
                null,
                null,
                true,
                false
        );
    }

    private void assertQueryExpectSize(String expected, String query) throws Exception {
        assertQueryNoLeakCheck(
                expected,
                query,
                null,
                null,
                true,
                true
        );
    }

    private void createEmptyTable() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts);");
    }

    private void prepare2partitionsTable() throws Exception {
        runQueries(
                "CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts) partition by day;",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 17280000000) " +
                        "  from long_sequence(10);"
        );
    }

    private RecordCursorFactory prepareFactory(TableReader reader) {
        TableReaderMetadata metadata = reader.getMetadata();
        IntList columnIndexes = new IntList();
        columnIndexes.add(0);
        columnIndexes.add(1);

        IntList columnSizes = new IntList();
        columnSizes.add(3);
        columnSizes.add(3);

        return new PageFrameRecordCursorFactory(
                engine.getConfiguration(),
                metadata,
                new FullPartitionFrameCursorFactory(reader.getTableToken(), reader.getMetadataVersion(), GenericRecordMetadata.copyOfNew(metadata), PartitionFrameCursorFactory.ORDER_DESC, null, 0, false),
                new PageFrameRowCursorFactory(PartitionFrameCursorFactory.ORDER_DESC),
                false,
                null,
                true,
                columnIndexes,
                columnSizes,
                true,
                false
        );
    }

    // creates test table in descending and then ascending order 10,9,..,1, 1,2,..,10
    private void prepareNoDesignatedTsTableWithDuplicates() throws Exception {
        runQueries(
                "CREATE TABLE tab(l long, ts TIMESTAMP);",
                "insert into tab " +
                        "  select 11-x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:10', 'yyyy-MM-ddTHH:mm:ss'), -1000000) " +
                        "  from long_sequence(10);",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:01', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                        "  from long_sequence(10);"
        );
    }

    private void prepareNonDesignatedTsTable() throws Exception {
        runQueries(
                "CREATE TABLE tab(l long, ts TIMESTAMP);",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                        "  from long_sequence(10);"
        );
    }

    private void prepareNormalTable() throws Exception {
        runQueries(
                "CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts);",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                        "  from long_sequence(10);"
        );
    }

    private void preparePartitionPerRowTable() throws Exception {
        runQueries(
                "CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts) partition by day;",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);"
        );
    }

    private void preparePartitionPerRowTableWithLongNames() throws Exception {
        runQueries(
                "CREATE TABLE trips(record_type long, created_on TIMESTAMP) timestamp(created_on) partition by day;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);"
        );
    }

    private void runQueries(String... queries) throws Exception {
        for (String query : queries) {
            execute(query);
        }
    }
}
