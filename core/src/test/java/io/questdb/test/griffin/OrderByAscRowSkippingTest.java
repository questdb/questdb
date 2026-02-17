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
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRowCursorFactory;
import io.questdb.std.IntList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * This class tests row skipping (in ascending order) optimizations for tables:
 * - with and without designated timestamps,
 * - non-partitioned and partitioned.
 */
public class OrderByAscRowSkippingTest extends AbstractCairoTest {

    // partitionedTable with two partitions, 5 rows per partition
    @Test
    public void test2partitionsSelectAll() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQueryExpectSize("l\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", "select l from tab order by ts");
        });
    }

    @Test
    public void test2partitionsSelectFirstN() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n1\n2\n3\n", "select l from tab order by ts limit 3", true);
        });
    }

    @Test
    public void test2partitionsSelectFirstNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("select l from tab order by ts limit 8,8");
        });
    }

    @Test
    public void test2partitionsSelectLastN() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();
            assertQuery("l\n8\n9\n10\n", "select l from tab order by ts limit -3", true);
        });
    }

    @Test
    public void test2partitionsSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("select l from tab order by ts limit -8,-8");
        });
    }

    @Test
    public void test2partitionsSelectMiddleNfromBothDirections() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n5\n6\n", "select l from tab order by ts limit 4,-4", true);
        });
    }

    @Test
    public void test2partitionsSelectMiddleNfromEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n3\n4\n5\n", "select l from tab order by ts limit -8,-5", true);
        });
    }

    @Test
    public void test2partitionsSelectMiddleNfromStart() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n6\n7\n8\n", "select l from tab order by ts limit 5,8", true);
        });
    }

    @Test
    public void test2partitionsSelectNbeforeStartReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("select l from tab order by ts limit -11,-15");
        });
    }

    @Test
    public void test2partitionsSelectNbeyondEndReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("select l from tab order by ts limit 11,12");
        });
    }

    @Test
    public void test2partitionsSelectNintersectingEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n9\n10\n", "select l from tab order by ts limit 8,12", true);
        });
    }

    @Test
    public void test2partitionsSelectNintersectingStart() throws Exception {
        assertMemoryLeak(() -> {
            prepare2partitionsTable();

            assertQuery("l\n1\n2\n", "select l from tab order by ts limit -12,-8", true);
        });
    }

    // empty table
    @Test
    public void testEmptyTableSelectAllReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("select l from tab order by ts");
        });
    }

    @Test
    public void testEmptyTableSelectFirstNreturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("select l from tab order by ts limit 3");
        });
    }

    @Test
    public void testEmptyTableSelectFirstNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("select l from tab order by ts limit 8,8");
        });
    }

    @Test
    public void testEmptyTableSelectLastNreturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("select l from tab order by ts limit -3");
        });
    }

    @Test
    public void testEmptyTableSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("select l from tab order by ts limit -8,-8");
        });
    }

    @Test
    public void testEmptyTableSelectMiddleNfromBothDirections() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("select l from tab order by ts limit 4,-4");
        });
    }

    @Test
    public void testEmptyTableSelectMiddleNfromEndReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("select l from tab order by ts limit -8,-5");
        });
    }

    @Test
    public void testEmptyTableSelectMiddleNfromStartReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("select l from tab order by ts limit 5,8");
        });
    }

    @Test
    public void testEmptyTableSelectNbeforeStartReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("select l from tab order by ts limit -11,-15");
        });
    }

    @Test
    public void testEmptyTableSelectNbeyondEndReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("select l from tab order by ts limit 11,12");
        });
    }

    @Test
    public void testEmptyTableSelectNintersectingEndReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("select l from tab order by ts limit 8,12");
        });
    }

    @Test
    public void testEmptyTable_select_N_intersecting_start_returns_empty_result() throws Exception {
        assertMemoryLeak(() -> {
            createEmptyTable();

            assertQuery("select l from tab order by ts limit -8,-12");
        });
    }

    // normal table without designated timestamp with rows in descending order
    @Test
    public void testNoDesignatedTsTableSelectAll() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTable();

            assertQueryExpectSize("l\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", "select l from tab order by ts");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectFirstN() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTable();

            assertQueryExpectSize("l\n1\n2\n3\n", "select l from tab order by ts limit 3");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectFirstNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTable();

            assertQuery("select l from tab order by ts limit 8,8");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectLastN() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTable();

            assertQueryExpectSize("l\n8\n9\n10\n", "select l from tab order by ts limit -3");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTable();

            assertQuery("select l from tab order by ts limit -8,-8");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectMiddleNfromBothDirections() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTable();

            assertQuery("l\n5\n6\n", "select l from tab order by ts limit 4,-4", true);
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectMiddleNfromEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTable();

            assertQueryExpectSize("l\n3\n4\n5\n", "select l from tab order by ts limit -8,-5");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectMiddleNfromStart() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTable();

            assertQueryExpectSize("l\n6\n7\n8\n", "select l from tab order by ts limit 5,8");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectNbeforeStartReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTable();

            assertQuery("select l from tab order by ts limit -11,-15");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectNbeyondEndReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTable();

            assertQuery("select l from tab order by ts limit 11,12");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectNintersectingEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTable();

            assertQueryExpectSize("l\n9\n10\n", "select l from tab order by ts limit 8,12");
        });
    }

    @Test
    public void testNoDesignatedTsTableSelectNintersectingStart() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTable();

            assertQueryExpectSize("l\n1\n2\n", "select l from tab order by ts limit -12,-8");
        });
    }

    // normal table without designated timestamp with rows (including duplicates) in descending order
    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectAll() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQueryExpectSize("l\n1\n1\n2\n2\n3\n3\n4\n4\n5\n5\n6\n6\n7\n7\n8\n8\n9\n9\n10\n10\n", "select l from tab order by ts");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectFirstN() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQueryExpectSize("l\n1\n1\n2\n", "select l from tab order by ts limit 3");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectFirstNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQuery("select l from tab order by ts limit 8,8");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectLastN() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQueryExpectSize("l\n9\n10\n10\n", "select l from tab order by ts limit -3");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQuery("select l from tab order by ts limit -8,-8");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectMiddleNfromBothDirections() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQuery("l\n5\n6\n", "select l from tab order by ts limit 9,-9", true);
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectMiddleNfromEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQueryExpectSize("l\n2\n2\n3\n", "select l from tab order by ts limit -18,-15");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectMiddleNfromStart() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQueryExpectSize("l\n8\n9\n9\n", "select l from tab order by ts limit 15,18");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNbeforeStartReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQuery("select l from tab order by ts limit -25,-21");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNbeyondEndReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQuery("select l from tab order by ts limit 21,22");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNintersectingEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQueryExpectSize("l\n10\n10\n", "select l from tab order by ts limit 18,22");
        });
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNintersectingStart() throws Exception {
        assertMemoryLeak(() -> {
            prepareNoDesignatedTsTableWithDuplicates();

            assertQueryExpectSize("l\n1\n1\n", "select l from tab order by ts limit -22,-18");
        });
    }

    // regular table with one partition
    @Test
    public void testNormalTableSelectAll() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQueryExpectSize("l\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", "select l from tab order by ts");
        });
    }

    @Test
    public void testNormalTableSelectFirstN() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n1\n2\n3\n", "select l from tab order by ts limit 3", true);
        });
    }

    @Test
    public void testNormalTableSelectFirstNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("select l from tab order by ts limit 8,8");
        });
    }

    @Test
    public void testNormalTableSelectLastN() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n8\n9\n10\n", "select l from tab order by ts limit -3", true);
        });
    }

    @Test
    public void testNormalTableSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n", "select l from tab order by ts limit -8,-8", true);
        });
    }

    @Test
    public void testNormalTableSelectMiddleNfromBothDirections() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n5\n6\n", "select l from tab order by ts limit 4,-4", true);
        });
    }

    @Test
    public void testNormalTableSelectMiddleNfromEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n3\n4\n5\n", "select l from tab order by ts limit -8,-5", true);
        });
    }

    @Test
    public void testNormalTableSelectMiddleNfromStart() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n6\n7\n8\n", "select l from tab order by ts limit 5,8", true);
        });
    }

    @Test
    public void testNormalTableSelectNbeforeStartReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n", "select l from tab order by ts limit -11,-15", true);
        });
    }

    @Test
    public void testNormalTableSelectNbeyondEndReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n", "select l from tab order by ts limit 11,12", true);
        });
    }

    @Test
    public void testNormalTableSelectNintersectingEnd() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n9\n10\n", "select l from tab order by ts limit 8,12", true);
        });
    }

    @Test
    public void testNormalTableSelectNintersectingStart() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();

            assertQuery("l\n1\n2\n", "select l from tab order by ts limit -12,-8", true);
        });
    }

    @Test
    public void testNormalTableSelectWithLimitOffset() throws Exception {
        assertMemoryLeak(() -> {
            prepareNormalTable();
            assertQuery("l\n8\n9\n10\n", "select l from tab where l > 5 order by ts limit 2, 10", false);
        });
    }

    // partitioned table with one row per partition
    @Test
    public void testPartitionPerRowSelectAll() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQueryExpectSize("l\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", "select l from tab order by ts");
        });
    }

    @Test
    public void testPartitionPerRowSelectFirstN() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n1\n2\n3\n", "select l from tab order by ts limit 3", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("select l from tab order by ts limit 8,8");
        });
    }

    @Test
    public void testPartitionPerRowSelectLastN() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n8\n9\n10\n", "select l from tab order by ts limit -3", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n", "select l from tab order by ts limit -8,-8", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectMiddleNfromBothDirections() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n5\n6\n", "select l from tab order by ts limit 4,-4", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectMiddleNfromEnd() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n3\n4\n5\n", "select l from tab order by ts limit -8,-5", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectMiddleNfromStart() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n6\n7\n8\n", "select l from tab order by ts limit 5,8", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectNbeyondEndReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("select l from tab order by ts limit 11,12");
        });
    }

    @Test
    public void testPartitionPerRowSelectNintersectingEnd() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n9\n10\n", "select l from tab order by ts limit 8,12", true);
        });
    }

    @Test
    public void testPartitionPerRowSelectNintersectingStart() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("l\n1\n2\n", "select l from tab order by ts limit -12,-8", true);
        });
    }

    // special cases
    @Test
    public void testPartitionPerRow_select_first_N_ordered_by_multiple_columns() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQueryExpectSize("l\n1\n2\n3\n", "select l from tab order by ts asc, l asc limit 3");
        });
    }

    @Test
    public void testPartitionPerRow_select_first_N_ordered_by_nonTs_column() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQueryExpectSize("l\n1\n2\n3\n", "select l from tab order by l asc limit 3");
        });
    }

    @Test
    public void testPartitionPerRow_select_last_N_ordered_by_multiple_columns() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQueryExpectSize("l\n8\n9\n10\n", "select l from tab order by ts asc, l asc limit -3");
        });
    }

    @Test
    public void testPartitionPerRow_select_last_N_ordered_by_nonTs_column() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();
            assertQueryExpectSize("l\n8\n9\n10\n", "select l from tab order by l asc limit -3");
        });
    }

    @Test
    public void testPartitionPerRowsSelectNbeforeStartReturnsEmptyResult() throws Exception {
        assertMemoryLeak(() -> {
            preparePartitionPerRowTable();

            assertQuery("select l from tab order by ts limit -11,-15");
        });
    }

    // tests "partitionIndex == partitionCount - 1" conditional in FullFwdPartitionFrameCursor.skipTo()
    @Test
    public void testSkipBeyondEndOfNonEmptyTableReturnsNoRows() throws Exception {
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
    public void testSkipOverEmptyTableWith1emptyPartitionReturnsNoRows() throws Exception {
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

    // tests "partitionCount < 1" conditional in FullFwdPartitionFrameCursor.skipTo()
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
                new FullPartitionFrameCursorFactory(metadata.getTableToken(), reader.getMetadataVersion(), GenericRecordMetadata.copyOf(metadata), PartitionFrameCursorFactory.ORDER_ASC, null, 0, false),
                new PageFrameRowCursorFactory(PartitionFrameCursorFactory.ORDER_ASC),
                false,
                null,
                true,
                columnIndexes,
                columnSizes,
                true,
                false
        );
    }

    // creates test table in descending order 10,9,..,1
    private void prepareNoDesignatedTsTable() throws Exception {
        runQueries(
                "CREATE TABLE tab(l long, ts TIMESTAMP);",
                "insert into tab " +
                        "  select 11-x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), -1000000) " +
                        "  from long_sequence(10);"
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
                        "  select 10-x," +
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
