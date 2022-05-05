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

import io.questdb.cairo.FullBwdDataFrameCursorFactory;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.table.BwdDataFrameRowCursorFactory;
import io.questdb.griffin.engine.table.DataFrameRecordCursorFactory;
import io.questdb.std.IntList;
import org.junit.Assert;
import org.junit.Test;

/**
 * This class tests row skipping (in ascending order) optimizations for tables:
 * - with and without designated timestamps,
 * - non-partitioned and partitioned .
 */
public class OrderByDescRowSkippingTest extends AbstractGriffinTest {

    //normal table without designated timestamp with rows (including duplicates) in descending order
    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectAll() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n10\n10\n9\n9\n8\n8\n7\n7\n6\n6\n5\n5\n4\n4\n3\n3\n2\n2\n1\n1\n", "select l from tab order by ts desc");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectFirstN() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n10\n10\n9\n", "select l from tab order by ts desc limit 3");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectMiddleNfromStart() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n3\n2\n2\n", "select l from tab order by ts desc limit 15,18");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNbeyondEndReturnsEmptyResult() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n", "select l from tab order by ts desc limit 21,22");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNbeforeStartReturnsEmptyResult() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n", "select l from tab order by ts desc limit -25,-21");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectLastN() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n2\n1\n1\n", "select l from tab order by ts desc limit -3");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectMiddleNfromEnd() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n9\n9\n8\n", "select l from tab order by ts desc limit -18,-15");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectMiddleNfromBothDirections() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 9,-9");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNintersectingEnd() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n1\n1\n", "select l from tab order by ts desc limit 18,22");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectNintersectingStart() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n10\n10\n", "select l from tab order by ts desc limit -22,-18");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectFirstNwithSameLoHireturnsNoRows() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n", "select l from tab order by ts desc limit 8,8");
    }

    @Test
    public void testNoDesignatedTsTableWithDuplicatesSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        prepareNoDesignatedTsTableWithDuplicates();

        assertQuery("l\n", "select l from tab order by ts desc limit -8,-8");
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

    //normal table without designated timestamp with rows in ascending
    @Test
    public void testNoDesignatedTsTableSelectAll() throws Exception {
        prepareNonDesignatedTsTable();

        assertQuery("l\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n", "select l from tab order by ts desc");
    }

    @Test
    public void testNoDesignatedTsTableSelectFirstN() throws Exception {
        prepareNonDesignatedTsTable();

        assertQuery("l\n10\n9\n8\n", "select l from tab order by ts desc limit 3");
    }

    @Test
    public void testNoDesignatedTsTableSelectMiddleNfromStart() throws Exception {
        prepareNonDesignatedTsTable();

        assertQuery("l\n5\n4\n3\n", "select l from tab order by ts desc limit 5,8");
    }

    @Test
    public void testNoDesignatedTsTableSelectNbeyondEndReturnsEmptyResult() throws Exception {
        prepareNonDesignatedTsTable();

        assertQuery("l\n", "select l from tab order by ts desc limit 11,12");
    }

    @Test
    public void testNoDesignatedTsTableSelectNbeforeStartReturnsEmptyResult() throws Exception {
        prepareNonDesignatedTsTable();

        assertQuery("l\n", "select l from tab order by ts desc limit -11,-15");
    }

    @Test
    public void testNoDesignatedTsTableSelectLastN() throws Exception {
        prepareNonDesignatedTsTable();

        assertQuery("l\n3\n2\n1\n", "select l from tab order by ts desc limit -3");
    }

    @Test
    public void testNoDesignatedTsTableSelectMiddleNfromEnd() throws Exception {
        prepareNonDesignatedTsTable();

        assertQuery("l\n8\n7\n6\n", "select l from tab order by ts desc limit -8,-5");
    }

    @Test
    public void testNoDesignatedTsTableSelectMiddleNfromBothDirections() throws Exception {
        prepareNonDesignatedTsTable();

        assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 4,-4");
    }

    @Test
    public void testNoDesignatedTsTableSelectFirstNwithSameLoHireturnsNoRows() throws Exception {
        prepareNonDesignatedTsTable();

        assertQuery("l\n", "select l from tab order by ts desc limit 8,8");
    }

    @Test
    public void testNoDesignatedTsTableSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        prepareNonDesignatedTsTable();

        assertQuery("l\n", "select l from tab order by ts desc limit -8,-8");
    }

    @Test
    public void testNoDesignatedTsTableSelectNintersectingEnd() throws Exception {
        prepareNonDesignatedTsTable();

        assertQuery("l\n2\n1\n", "select l from tab order by ts desc limit 8,12");
    }

    @Test
    public void testNoDesignatedTsTableSelectNintersectingStart() throws Exception {
        prepareNonDesignatedTsTable();

        assertQuery("l\n10\n9\n", "select l from tab order by ts desc limit -12,-8");
    }

    private void prepareNonDesignatedTsTable() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP);",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                        "  from long_sequence(10);");
    }

    //empty table with designated timestamp
    @Test
    public void testEmptyTableSelect_allReturnsNoRows() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts desc");
    }

    @Test
    public void testEmptyTableSelectFirstNReturnsNoRows() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts desc limit 3");
    }

    @Test
    public void testEmptyTableSelectMiddleNfromStartReturnsNoRows() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts desc limit 5,8");
    }


    @Test
    public void testEmptyTableSelectNbeyondEndReturnsEmptyResult() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts desc limit 11,12");
    }

    @Test
    public void testEmptyTableSelectNbeforeStartReturnsEmptyResult() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts desc limit -11,-15");
    }

    @Test
    public void testEmptyTableSelectNintersectingEndReturnsEmptyResult() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts desc limit 8,12");
    }

    @Test
    public void testEmptyTableSelectNintersectingStartReturnsEmptyResult() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts desc limit -12,-8");
    }

    @Test
    public void testEmptyTableSelectLastNReturnsNoRows() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts desc limit -3");
    }

    @Test
    public void testEmptyTableSelectMiddleNfromEndReturnsNoRows() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts desc limit -8,-5");
    }

    @Test
    public void testEmptyTableSelectMiddleNfromBothDirections() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts desc limit 4,-4");
    }

    @Test
    public void testEmptyTableSelectFirstNwithSameLoHireturnsNoRows() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts desc limit 8,8");
    }

    @Test
    public void testEmptyTableSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        createEmptyTable();

        assertQuery("l\n", "select l from tab order by ts desc limit -8,-8");
    }

    private void createEmptyTable() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts);");
    }

    //regular table with designated timestamp and  one partition
    @Test
    public void testNormalTableSelectAll() throws Exception {
        prepareNormalTable();

        assertQuery("l\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n", "select l from tab order by ts desc");
    }

    @Test
    public void testNormalTableSelectFirstN() throws Exception {
        prepareNormalTable();

        assertQuery("l\n10\n9\n8\n", "select l from tab order by ts desc limit 3");
    }

    @Test
    public void testNormalTableSelectMiddleNfromStart() throws Exception {
        prepareNormalTable();

        assertQuery("l\n5\n4\n3\n", "select l from tab order by ts desc limit 5,8");
    }

    @Test
    public void testNormalTableSelectNbeyondEndReturnsEmptyResult() throws Exception {
        prepareNormalTable();

        assertQuery("l\n", "select l from tab order by ts desc limit 11,12");
    }

    @Test
    public void testNormalTableSelectNbeforeStartReturnsEmptyResult() throws Exception {
        prepareNormalTable();

        assertQuery("l\n", "select l from tab order by ts desc limit -11,-15");
    }

    @Test
    public void testNormalTableSelectLastN() throws Exception {
        prepareNormalTable();

        assertQuery("l\n3\n2\n1\n", "select l from tab order by ts desc limit -3");
    }

    @Test
    public void testNormalTableSelectMiddleNfromEnd() throws Exception {
        prepareNormalTable();

        assertQuery("l\n8\n7\n6\n", "select l from tab order by ts desc limit -8,-5");
    }

    @Test
    public void testNormalTableSelectMiddleNfromBothDirections() throws Exception {
        prepareNormalTable();

        assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 4,-4");
    }

    @Test
    public void testNormalTableSelectFirstNwithSameLoHireturnsNoRows() throws Exception {
        prepareNormalTable();

        assertQuery("l\n", "select l from tab order by ts desc limit 8,8");
    }

    @Test
    public void testNormalTableSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        prepareNormalTable();

        assertQuery("l\n", "select l from tab order by ts desc limit -8,-8");
    }

    @Test
    public void testNormalTableSelectNintersectingEnd() throws Exception {
        prepareNormalTable();

        assertQuery("l\n2\n1\n", "select l from tab order by ts desc limit 8,12");
    }

    @Test
    public void testNormalTableSelectNintersectingStart() throws Exception {
        prepareNormalTable();

        assertQuery("l\n10\n9\n", "select l from tab order by ts desc limit -12,-8");
    }

    private void prepareNormalTable() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts);",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                        "  from long_sequence(10);");
    }

    //partitioned table with designated timestamp and two partitions, 5 rows per partition
    @Test
    public void test2partitionsSelectAll() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n", "select l from tab order by ts desc");
    }

    @Test
    public void test2partitionsSelectFirstN() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n10\n9\n8\n", "select l from tab order by ts desc limit 3");
    }

    @Test
    public void test2partitionsSelectMiddleNfromStart() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n5\n4\n3\n", "select l from tab order by ts desc limit 5,8");
    }

    @Test
    public void test2partitionsSelectNbeyondEndReturnsEmptyResult() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n", "select l from tab order by ts desc limit 11,12");
    }

    @Test
    public void test2partitionsSelectNbeforeStartReturnsEmptyResult() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n", "select l from tab order by ts desc limit -11,-15");
    }

    @Test
    public void test2partitionsSelectLastN() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n3\n2\n1\n", "select l from tab order by ts desc limit -3");
    }

    @Test
    public void test2partitionsSelectMiddleNfromEnd() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n8\n7\n6\n", "select l from tab order by ts desc limit -8,-5");
    }

    @Test
    public void test2partitionsSelectMiddleNfromBothDirections() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 4,-4");
    }

    @Test
    public void test2partitionsSelectFirstNwithSameLoHireturnsNoRows() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n", "select l from tab order by ts desc limit 8,8");
    }

    @Test
    public void test2partitionsSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n", "select l from tab order by ts desc limit -8,-8");
    }

    @Test
    public void test2partitionsSelectNintersectingEnd() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n2\n1\n", "select l from tab order by ts desc limit 8,12");
    }

    @Test
    public void test2partitionsSelectNintersectingStart() throws Exception {
        prepare2partitionsTable();

        assertQuery("l\n10\n9\n", "select l from tab order by ts desc limit -12,-8");
    }

    private void prepare2partitionsTable() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts) partition by day;",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 17280000000) " +
                        "  from long_sequence(10);");
    }

    //partitioned table with designated timestamp and 10 partitions one row per partition
    @Test
    public void testPartitionPerRowSelectAll() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n", "select l from tab order by ts desc");
    }

    @Test
    public void testPartitionPerRowSelectFirstN() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n10\n9\n8\n", "select l from tab order by ts desc limit 3");
    }

    @Test
    public void testPartitionPerRowSelectMiddleNfromStart() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n5\n4\n3\n", "select l from tab order by ts desc limit 5,8");
    }

    @Test
    public void testPartitionPerRowSelectNbeyondEndReturnsEmptyResult() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n", "select l from tab order by ts desc limit 11,12");
    }

    @Test
    public void testPartitionPerRowSelectNbeforeStartReturnsEmptyResult() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n", "select l from tab order by ts desc limit -11,-15");
    }

    @Test
    public void testPartitionPerRowSelectLastN() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n3\n2\n1\n", "select l from tab order by ts desc limit -3");
    }

    @Test
    public void testPartitionPerRowSelectMiddleNfromEnd() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n8\n7\n6\n", "select l from tab order by ts desc limit -8,-5");
    }

    @Test
    public void testPartitionPerRowSelectMiddleNfromBothDirections() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n6\n5\n", "select l from tab order by ts desc limit 4,-4");
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithSameLoHireturnsNoRows() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n", "select l from tab order by ts desc limit 8,8");
    }

    @Test
    public void testPartitionPerRowSelectLastNwithSameLoHiReturnsNoRows() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n", "select l from tab order by ts desc limit -8,-8");
    }

    @Test
    public void testPartitionPerRowSelectNintersectingEnd() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n2\n1\n", "select l from tab order by ts desc limit 8,12");
    }

    @Test
    public void testPartitionPerRowSelectNintersectingStart() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n10\n9\n", "select l from tab order by ts desc limit -12,-8");
    }

    private void preparePartitionPerRowTable() throws Exception {
        runQueries("CREATE TABLE tab(l long, ts TIMESTAMP) timestamp(ts) partition by day;",
                "insert into tab " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");
    }

    //special cases
    @Test
    public void testPartitionPerRowSelectFirstNorderedByMultipleColumns() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n10\n9\n8\n", "select l from tab order by ts desc, l desc limit 3");
    }

    @Test
    public void testPartitionPerRowSelectLastNorderedByMultipleColumns() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n3\n2\n1\n", "select l from tab order by ts desc, l desc limit -3");
    }

    @Test
    public void testPartitionPerRowSelectFirstNorderedByNonTsColumn() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n10\n9\n8\n", "select l from tab order by l desc limit 3");
    }

    @Test
    public void testPartitionPerRowSelectLastNorderedByNonTsColumn() throws Exception {
        preparePartitionPerRowTable();

        assertQuery("l\n3\n2\n1\n", "select l from tab order by l desc limit -3");
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentCaseInOrderBy() throws Exception {//here
        preparePartitionPerRowTableWithLongNames();

        assertQuery("record_type\n10\n9\n8\n7\n6\n", "select record_type from trips order by created_ON desc limit 5");
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentCaseInSelectAndOrderBy() throws Exception {
        preparePartitionPerRowTableWithLongNames();

        assertQuery("record_Type\tCREATED_on\n" + DATA,
                "select record_Type, CREATED_on from trips order by created_ON desc limit 5",
                null, "CREATED_on###DESC", true, false, true);
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentCaseInSelectAndOrderByV2() throws Exception {
        preparePartitionPerRowTableWithLongNames();

        assertQuery("record_Type\tCREATED_ON\n" + DATA,
                "select record_Type, CREATED_ON from trips order by created_on desc limit 5",
                null, "CREATED_ON###DESC", true, false, true);
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentCaseInSelectAndOrderByWithAlias() throws Exception {
        preparePartitionPerRowTableWithLongNames();

        assertQuery(
                compiler,
                "record_Type\tcre_on\n" + DATA,
                "select record_Type, CREATED_ON as cre_on from trips order by created_on desc limit 5",
                "cre_on###DESC",
                sqlExecutionContext,
                true,
                true,
                true);
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentCaseInSelectAndOrderByWithAliasV2() throws Exception {
        preparePartitionPerRowTableWithLongNames();

        assertQuery("record_Type\tcre_on\n" + DATA,
                "select record_Type, CREATED_ON cre_on from trips order by created_on desc limit 5",
                null, "cre_on###DESC", true, false, true);
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentNameInSubquery() throws Exception {
        preparePartitionPerRowTableWithLongNames();

        assertQuery(EXPECTED,
                "select rectype, creaton from " +
                        "( select record_Type as rectype, CREATED_ON creaton from trips order by created_on desc limit 5)",
                null, "creaton###DESC", true, false, true);
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentNameInSubqueryV2() throws Exception {
        preparePartitionPerRowTableWithLongNames();

        assertQuery(EXPECTED,
                "select rectype, creaton from " +
                        "( select record_Type as rectype, CREATED_ON creaton " +
                        "from trips " +
                        "order by created_on desc) " +
                        "limit 5",
                null, "creaton###DESC", true, false, true);
    }

    @Test
    public void testPartitionPerRowSelectFirstNwithDifferentNameInSubqueryV3() throws Exception {
        preparePartitionPerRowTableWithLongNames();
        //order by advice is not available in the subquery ... sort performed by limitRecordCursor
        assertQuery(EXPECTED, "select rectype, creaton from " +
                "( select record_Type as rectype, CREATED_ON creaton from trips) " +
                "order by creaton desc limit 5");
    }

    //tests "partitionCount < 1" conditional in FullBwdDataFrameCursor.skipTo()
    @Test
    public void testSskipOverEemptyTableWithNoPartitionsReturnsNoRows() throws Exception {
        runQueries("CREATE TABLE trips(record_type long, created_on TIMESTAMP) timestamp(created_on) partition by day;");

        try (TableReader reader = sqlExecutionContext.getCairoEngine().getReader(AllowAllCairoSecurityContext.INSTANCE, "trips");
             RecordCursor cursor = prepareCursor(reader)) {
            cursor.skipTo(1);
            Assert.assertFalse(cursor.hasNext());
        }
    }

    //tests "partitionIndex == partitionCount - 1" conditional in FullBwdDataFrameCursor.skipTo()
    @Test
    public void testSkipBeyondEndOfNonemptyTableReturnsNoRows() throws Exception {
        preparePartitionPerRowTableWithLongNames();

        try (TableReader reader = sqlExecutionContext.getCairoEngine().getReader(AllowAllCairoSecurityContext.INSTANCE, "trips");
             RecordCursor cursor = prepareCursor(reader)) {
            cursor.skipTo(11);
            Assert.assertFalse(cursor.hasNext());
        }
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
                new FullBwdDataFrameCursorFactory(engine, "trips", metadata.getId(), reader.getVersion()),
                new BwdDataFrameRowCursorFactory(),
                false,
                null,
                true,
                columnIndexes,
                columnSizes,
                true
        );

        return factory.getCursor(sqlExecutionContext);
    }

    @Test
    public void testSkipOverEmptyTableWith1EmptyPartitionReturnsNoRows() throws Exception {
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

    static final String DATA = "10\t2022-01-13T10:00:00.000000Z\n" +
            "9\t2022-01-12T06:13:20.000000Z\n" +
            "8\t2022-01-11T02:26:40.000000Z\n" +
            "7\t2022-01-09T22:40:00.000000Z\n" +
            "6\t2022-01-08T18:53:20.000000Z\n";

    static final String EXPECTED = "rectype\tcreaton\n" + DATA;

    private void preparePartitionPerRowTableWithLongNames() throws Exception {
        runQueries("CREATE TABLE trips(record_type long, created_on TIMESTAMP) timestamp(created_on) partition by day;",
                "insert into trips " +
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
