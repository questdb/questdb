/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.FullFwdDataFrameCursorFactory;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.table.DataFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.DataFrameRowCursorFactory;
import io.questdb.std.IntList;
import org.junit.Assert;
import org.junit.Test;

/**
 * This class tests row skipping (in ascending order) optimizations for tables:
 * - with and without designated timestamps,
 * - non-partitioned and partitioned .
 */
public class OrderByAscRowSkippingTest extends AbstractGriffinTest {

    //normal table without designated timestamp with rows (including duplicates) in descending order
    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectAll() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n1\n1\n2\n2\n3\n3\n4\n4\n5\n5\n6\n6\n7\n7\n8\n8\n9\n9\n10\n10\n", "select l from tab order by ts");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectFirstN() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n1\n1\n2\n", "select l from tab order by ts limit 3");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectMiddleNfromStart() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n8\n9\n9\n", "select l from tab order by ts limit 15,18");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNbeyondEndReturnsEmptyResult() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n", "select l from tab order by ts limit 21,22");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNbeforeStartReturnsEmptyResult() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n", "select l from tab order by ts limit -25,-21");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectLastN() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n9\n10\n10\n", "select l from tab order by ts limit -3");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectMiddleNfromEnd() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n2\n2\n3\n", "select l from tab order by ts limit -18,-15");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectMiddleNfromBothDirections() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n5\n6\n", "select l from tab order by ts limit 9,-9");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNintersectingEnd() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n10\n10\n", "select l from tab order by ts limit 18,22");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNintersectingStart() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n1\n1\n", "select l from tab order by ts limit -22,-18");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectFirstNwithSameLoHiReturnsNoRows() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n", "select l from tab order by ts limit 8,8");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n", "select l from tab order by ts limit -8,-8");
    }

    //creates test table in descending and then ascending order order 10,9,..,1, 1,2,..,10
    private void prepareNoDesignatedTsTableWithDuplicates() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP);",
                "insert into tab " +
                        "  select 11-x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:10', 'yyyy-MM-ddTHH:mm:ss'), -1000000) " +
                        "  from long_sequence(10);",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:01', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                        "  from long_sequence(10);");
    }

    //normal table without designated timestamp with rows in descending order
    @Test
    public void testNoDesignatedTsTableSelectAll() throws Exception {
        prepareNoDesignatedTsTable();

        assertQuery("l\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", "select l from tab order by ts");
    }

    @Test
    public void testNoDesignatedTsTableSelectFirstN() throws Exception {
        prepareNoDesignatedTsTable();

        assertQuery("l\n1\n2\n3\n", "select l from tab order by ts limit 3");
    }

    @Test
    public void testNoDesignatedTsTableSelectMiddleNfromStart() throws Exception {
        prepareNoDesignatedTsTable();

        assertQuery("l\n6\n7\n8\n", "select l from tab order by ts limit 5,8");
    }

    @Test
    public void testNoDesignatedTsTableSelectNbeyondEndReturnsEmptyResult() throws Exception {
        prepareNoDesignatedTsTable();

        assertQuery("l\n", "select l from tab order by ts limit 11,12");
    }

    @Test
    public void testNoDesignatedTsTableSelectNbeforeStartReturnsEmptyResult() throws Exception {
        prepareNoDesignatedTsTable();

        assertQuery("l\n", "select l from tab order by ts limit -11,-15");
    }

    @Test
    public void testNoDesignatedTsTableSelectLastN() throws Exception {
        prepareNoDesignatedTsTable();

        assertQuery("l\n8\n9\n10\n", "select l from tab order by ts limit -3");
    }

    @Test
    public void testNoDesignatedTsTableSelectMiddleNfromEnd() throws Exception {
        prepareNoDesignatedTsTable();

        assertQuery("l\n3\n4\n5\n", "select l from tab order by ts limit -8,-5");
    }

    @Test
    public void testNoDesignatedTsTableSelectMiddleNfromBothDirections() throws Exception {
        prepareNoDesignatedTsTable();

        assertQuery("l\n5\n6\n", "select l from tab order by ts limit 4,-4");
    }

    @Test
    public void testNoDesignatedTsTableSelectNintersectingEnd() throws Exception {
        prepareNoDesignatedTsTable();

        assertQuery("l\n9\n10\n", "select l from tab order by ts limit 8,12");
    }

    @Test
    public void testNoDesignatedTsTableSelectNintersectingStart() throws Exception {
        prepareNoDesignatedTsTable();

        assertQuery("l\n1\n2\n", "select l from tab order by ts limit -12,-8");
    }

    @Test
    public void testNoDesignatedTsTableSelectFirstNwithSameLoHiReturnsNoRows() throws Exception {
        prepareNoDesignatedTsTable();

        assertQuery("l\n", "select l from tab order by ts limit 8,8");
    }

    @Test
    public void testNoDesignatedTsTableSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        prepareNoDesignatedTsTable();

        assertQuery("l\n", "select l from tab order by ts limit -8,-8");
    }

    //creates test table in descending order 10,9,..,1
    private void prepareNoDesignatedTsTable() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP);",
                "insert into tab " +
                        "  select 11-x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), -1000000) " +
                        "  from long_sequence(10);");
    }

    //empty table
    @Test
    public void testEmptyTableSelectAllReturnsNoRows() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts");
    }

    @Test
    public void testEmptyTableSelectFirstNreturnsNoRows() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts limit 3");
    }

    @Test
    public void testEmptyTableSelectMiddleNfromStartReturnsNoRows() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts limit 5,8");
    }

    @Test
    public void testEmptyTableSelectNintersectingEndReturnsEmptyResult() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts limit 8,12");
    }

    @Test
    public void testEmptyTableSelectNbeyondEndReturnsEmptyResult() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts limit 11,12");
    }

    @Test
    public void testEmptyTableSelectNbeforeStartReturnsEmptyResult() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts limit -11,-15");
    }

    @Test
    public void testEmptyTable_select_N_intersecting_start_returns_empty_result() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts limit -8,-12");
    }

    @Test
    public void testEmptyTableSelectLastNreturnsNoRows() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts limit -3");
    }

    @Test
    public void testEmptyTableSelectMiddleNfromEndReturnsNoRows() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts limit -8,-5");
    }

    @Test
    public void testEmptyTableSelectMiddleNfromBothDirections() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts limit 4,-4");
    }

    @Test
    public void testEmptyTableSelectFirstNwithSameLoHiReturnsNoRows() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts limit 8,8");
    }

    @Test
    public void testEmptyTableSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts limit -8,-8");
    }

    private void createEmptyTable() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts);");
    }

    //regular table with one partition
    @Test
    public void testNormalTableSelectAll() throws Exception {
        prepareNormalTable();

        assertQuery("l\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", "select l from tab order by ts");
    }

    @Test
    public void testNormalTableSelectFirstN() throws Exception {
        prepareNormalTable();

        assertQuery("l\n1\n2\n3\n", "select l from tab order by ts limit 3");
    }

    @Test
    public void testNormalTableSelectMiddleNfromStart() throws Exception {
        prepareNormalTable();

        assertQuery("l\n6\n7\n8\n", "select l from tab order by ts limit 5,8");
    }

    @Test
    public void testNormalTableSelectNbeyondEndReturnsEmptyResult() throws Exception {
        prepareNormalTable();

        assertQuery("l\n", "select l from tab order by ts limit 11,12");
    }

    @Test
    public void testNormalTableSelectNbeforeStartReturnsEmptyResult() throws Exception {
        prepareNormalTable();

        assertQuery("l\n", "select l from tab order by ts limit -11,-15");
    }

    @Test
    public void testNormalTableSelectLastN() throws Exception {
        prepareNormalTable();

        assertQuery("l\n8\n9\n10\n", "select l from tab order by ts limit -3");
    }

    @Test
    public void testNormalTableSelectMiddleNfromEnd() throws Exception {
        prepareNormalTable();

        assertQuery("l\n3\n4\n5\n", "select l from tab order by ts limit -8,-5");
    }

    @Test
    public void testNormalTableSelectMiddleNfromBothDirections() throws Exception {
        prepareNormalTable();

        assertQuery("l\n5\n6\n", "select l from tab order by ts limit 4,-4");
    }

    @Test
    public void testNormalTableSelectNintersectingEnd() throws Exception {
        prepareNormalTable();

        assertQuery("l\n9\n10\n", "select l from tab order by ts limit 8,12");
    }

    @Test
    public void testNormalTableSelectNintersectingStart() throws Exception {
        prepareNormalTable();

        assertQuery("l\n1\n2\n", "select l from tab order by ts limit -12,-8");
    }

    @Test
    public void testNormalTableSelectFirstNwithSameLoHiReturnsNoRows() throws Exception {
        prepareNormalTable();

        assertQuery("l\n", "select l from tab order by ts limit 8,8");
    }

    @Test
    public void testNormalTableSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        prepareNormalTable();

        assertQuery("l\n", "select l from tab order by ts limit -8,-8");
    }

    private void prepareNormalTable() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts);",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                        "  from long_sequence(10);");
    }

    //partitionedTable with two partitions, 5 rows per partition
    @Test
    public void test2partitionsSelectAll() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", "select l from tab order by ts");
    }

    @Test
    public void test2partitionsSelectFirstN() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n1\n2\n3\n", "select l from tab order by ts limit 3");
    }

    @Test
    public void test2partitionsSelectMiddleNfromStart() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n6\n7\n8\n", "select l from tab order by ts limit 5,8");
    }

    @Test
    public void test2partitionsSelectNbeyondEndReturnsEmptyResult() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n", "select l from tab order by ts limit 11,12");
    }

    @Test
    public void test2partitionsSelectNbeforeStartReturnsEmptyResult() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n", "select l from tab order by ts limit -11,-15");
    }

    @Test
    public void test2partitionsSelectLastN() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n8\n9\n10\n", "select l from tab order by ts limit -3");
    }

    @Test
    public void test2partitionsSelectMiddleNfromEnd() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n3\n4\n5\n", "select l from tab order by ts limit -8,-5");
    }

    @Test
    public void test2partitionsSelectMiddleNfromBothDirections() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n5\n6\n", "select l from tab order by ts limit 4,-4");
    }

    @Test
    public void test2partitionsSelectFirstNwithSameLoHiReturnsNoRows() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n", "select l from tab order by ts limit 8,8");
    }

    @Test
    public void test2partitionsSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n", "select l from tab order by ts limit -8,-8");
    }

    @Test
    public void test2partitionsSelectNintersectingEnd() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n9\n10\n", "select l from tab order by ts limit 8,12");
    }

    @Test
    public void test2partitionsSelectNintersectingStart() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n1\n2\n", "select l from tab order by ts limit -12,-8");
    }

    private void prepare2partitionsTable() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts) partition by day;",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 17280000000) " +
                        "  from long_sequence(10);");
    }

    //partitioned table with one row per partition
    @Test
    public void testPartitionPerRowSelectAll() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", "select l from tab order by ts");
    }

    @Test
    public void testPartitionPerRowSelectFirstN() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n1\n2\n3\n", "select l from tab order by ts limit 3");
    }

    @Test
    public void testPartitionPerRowSelectMiddleNfromStart() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n6\n7\n8\n", "select l from tab order by ts limit 5,8");
    }

    @Test
    public void testPartitionPerRowSelectNbeyondEndReturnsEmptyResult() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n", "select l from tab order by ts limit 11,12");
    }

    @Test
    public void testPartitionPerRowsSelectNbeforeStartReturnsEmptyResult() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n", "select l from tab order by ts limit -11,-15");
    }

    @Test
    public void testPartitionPerRowSelectLastN() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n8\n9\n10\n", "select l from tab order by ts limit -3");
    }

    @Test
    public void testPartitionPerRowSelectMiddleNfromEnd() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n3\n4\n5\n", "select l from tab order by ts limit -8,-5");
    }

    @Test
    public void testPartitionPerRowSelectMiddleNfromBothDirections() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n5\n6\n", "select l from tab order by ts limit 4,-4");
    }

    @Test
    public void testPartitionPerRowSelectNintersectingEnd() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n9\n10\n", "select l from tab order by ts limit 8,12");
    }

    @Test
    public void testPartitionPerRowSelectNintersectingStart() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n1\n2\n", "select l from tab order by ts limit -12,-8");
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithSameLoHiReturnsNoRows() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n", "select l from tab order by ts limit 8,8");
    }

    @Test
    public void testPartitionPerRowSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n", "select l from tab order by ts limit -8,-8");
    }

    //special cases
    @Test
    public void testPartitionPerRow_select_first_N_ordered_by_multiple_columns() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n1\n2\n3\n", "select l from tab order by ts asc, l asc limit 3");
    }

    @Test
    public void testPartitionPerRow_select_last_N_ordered_by_multiple_columns() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n8\n9\n10\n", "select l from tab order by ts asc, l asc limit -3");
    }

    @Test
    public void testPartitionPerRow_select_first_N_ordered_by_nonTs_column() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n1\n2\n3\n", "select l from tab order by l asc limit 3");
    }

    @Test
    public void testPartitionPerRow_select_last_N_ordered_by_nonTs_column() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n8\n9\n10\n", "select l from tab order by l asc limit -3");
    }

    //tests "partitionCount < 1" conditional in FullFwdDataFrameCursor.skipTo()
    @Test
    public void testSkipOverEmptyTableWithNoPartitionsReturnsNoRows() throws Exception {
        runQueries("CREATE TABLE trips(record_type long, created_on TIMESTAMP) timestamp(created_on) partition by day;");

        try (TableReader reader = sqlExecutionContext.getCairoEngine().getReader(AllowAllCairoSecurityContext.INSTANCE, "trips");
             RecordCursor cursor = prepareCursor(reader)) {
            cursor.skipTo(1);
            Assert.assertFalse(cursor.hasNext());
        }
    }

    //tests "partitionIndex == partitionCount - 1" conditional in FullFwdDataFrameCursor.skipTo()
    @Test
    public void testSkipBeyondEndOfNonEmptyTableReturnsNoRows() throws Exception {
        preparePartitionPerRowTableWithLongNames();

        try (TableReader reader = sqlExecutionContext.getCairoEngine().getReader(AllowAllCairoSecurityContext.INSTANCE, "trips");
             RecordCursor cursor = prepareCursor(reader)) {
            cursor.skipTo(11);
            Assert.assertFalse(cursor.hasNext());
        }
    }

    @Test
    public void testSkipOverEmptyTableWith1emptyPartitionReturnsNoRows() throws Exception {
        runQueries("CREATE TABLE trips(record_type long, created_on TIMESTAMP) timestamp(created_on) partition by none;");

        assertMemoryLeak(() -> {
            try (TableWriter writer = sqlExecutionContext.getCairoEngine().getWriter(AllowAllCairoSecurityContext.INSTANCE, "trips", "test")) {
                TableWriter.Row row = writer.newRow(0L);
                row.putLong(0, 0L);
                row.append();

                row = writer.newRow(100L);
                row.putLong(0, 1L);
                row.append();

                try (TableReader reader = sqlExecutionContext.getCairoEngine().getReader(AllowAllCairoSecurityContext.INSTANCE, "trips");
                     RecordCursor cursor = prepareCursor(reader)) {
                    cursor.skipTo(1);
                    Assert.assertFalse(cursor.hasNext());
                }

                writer.rollback();
            }
        });
    }

    private RecordCursor prepareCursor(TableReader reader) throws SqlException {
        TableReaderMetadata metadata = reader.getMetadata();
        IntList columnIndexes = new IntList();
        columnIndexes.add(0);
        columnIndexes.add(1);

        IntList columnSizes = new IntList();
        columnSizes.add(3);
        columnSizes.add(3);

        DataFrameRecordCursorFactory factory = new DataFrameRecordCursorFactory(engine.getConfiguration(), metadata,
                new FullFwdDataFrameCursorFactory(engine, "trips", metadata.getId(), reader.getVersion()),
                new DataFrameRowCursorFactory(),
                false,
                null,
                true,
                columnIndexes,
                columnSizes,
                true
        );

        return factory.getCursor(sqlExecutionContext);
    }

    private void preparePartitionPerRowTableWithLongNames() throws Exception {
        runQueries("CREATE TABLE trips(record_type long, created_on TIMESTAMP) timestamp(created_on) partition by day;",
                "insert into trips " +
                        "  select 10-x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");
    }

    private void preparePartitionPerRowTable() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts) partition by day;",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");
    }

    private void runQueries(String... queries) throws Exception {
        assertMemoryLeak(() -> {
            for (String query : queries) {
                compiler.compile(query, sqlExecutionContext);
            }
        });
    }

    private void assertQuery(String expected, String query) throws Exception {
        assertQuery(expected,
                query,
                null, null, true, false, true);
    }
}
