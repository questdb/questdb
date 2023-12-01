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

package io.questdb.test.duckdb;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.duckdb.*;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.GcUtf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class DuckDBTest extends AbstractCairoTest {
//    @Test
//    public void testCreate() {
//        long db = DuckDB.databaseOpen(0, 0);
//        Assert.assertNotEquals(0, db);
//        long conn = DuckDB.databaseConnect(db);
//        Assert.assertNotEquals(0, conn);
//
//        DirectUtf8Sequence n = new GcUtf8String("CREATE TABLE integers(i INTEGER)");
//        DirectUtf8Sequence i = new GcUtf8String("INSERT INTO integers VALUES (1), (2), (3), (4), (5)");
//        DirectUtf8Sequence s = new GcUtf8String("SELECT MAX(i), MIN(i) FROM integers");
//
//        DuckDB.connectionExec(conn, n.ptr(), n.size());
//        DuckDB.connectionExec(conn, i.ptr(), i.size());
//        long result = DuckDB.connectionQuery(conn, s.ptr(), s.size());
//        Assert.assertNotEquals(0, result);
//
//        Assert.assertEquals(2, DuckDB.resultColumnCount(result));
//        Assert.assertEquals(1, DuckDB.resultRowCount(result));
//
//        long chunkCount = DuckDB.resultDataChunkCount(result);
//        Assert.assertEquals(1, chunkCount);
//
//        long chunk = DuckDB.resultGetDataChunk(result, 0);
//        Assert.assertNotEquals(0, chunk);
//
//        long columnCount = DuckDB.dataChunkGetColumnCount(chunk);
//        Assert.assertEquals(2, columnCount);
//
//        DirectUtf8StringZ name = new DirectUtf8StringZ();
//
//        int columnType0 = DuckDB.resultColumnType(result, 0);
//        Assert.assertEquals(DuckDB.DUCKDB_TYPE_INTEGER, columnType0);
//        name.of(DuckDB.resultColumnName(result, 0));
//        Assert.assertEquals("max(i)", name.toString());
//
//        int columnType1 = DuckDB.resultColumnType(result, 1);
//        Assert.assertEquals(DuckDB.DUCKDB_TYPE_INTEGER, columnType1);
//        name.of(DuckDB.resultColumnName(result, 1));
//        Assert.assertEquals("min(i)", name.toString());
//
//        long chunkSize = DuckDB.dataChunkGetSize(chunk);
//        Assert.assertEquals(1, chunkSize);
//
//        long v1 = DuckDB.dataChunkGetVector(chunk, 0);
//        Assert.assertNotEquals(0, v1);
//        Assert.assertEquals(DuckDB.DUCKDB_TYPE_INTEGER, DuckDB.vectorGetColumnType(v1));
//
//        long data1 = DuckDB.vectorGetData(v1);
//        Assert.assertNotEquals(0, data1);
//        for (int j = 0; j < chunkSize; j++) {
//            int v = Unsafe.getUnsafe().getInt(data1 + j * 4L);
//            Assert.assertEquals(5, v);
//        }
//
//        long nulls = DuckDB.vectorGetValidity(v1);
//        Assert.assertTrue(DuckDB.validityRowIsValid(nulls, 0));
//
//        long v2 = DuckDB.dataChunkGetVector(chunk, 1);
//        Assert.assertNotEquals(0, v2);
//        Assert.assertEquals(DuckDB.DUCKDB_TYPE_INTEGER, DuckDB.vectorGetColumnType(v2));
//
//        long data2 = DuckDB.vectorGetData(v2);
//        for (int j = 0; j < chunkSize; j++) {
//            int v = Unsafe.getUnsafe().getInt(data2 + j * 4L);
//            Assert.assertEquals(1, v);
//        }
//
//        long nulls2 = DuckDB.vectorGetValidity(v2);
//        Assert.assertTrue(DuckDB.validityRowIsValid(nulls2, 0));
//
//        DuckDB.dataChunkDestroy(chunk);
//        DuckDB.resultDestroy(result);
//        DuckDB.connectionDisconnect(conn);
//        DuckDB.databaseClose(db);
//    }
//
//    @Test
//    public void testSimple() throws Exception {
//        assertMemoryLeak(
//            () -> {
//                try (DuckDBInstance ddb = new DuckDBInstance()) {
//                    try(DuckDBConnection connection = ddb.getConnection()) {
//                        long rows = 20992;
//                        long chunks = rows / 2048 + 1;
//                        DirectUtf8Sequence n = new GcUtf8String("CREATE TABLE integers AS select generate_series i from generate_series(1,"+rows+");");
//                        DuckDBResult createRes = new DuckDBResult();
//                        boolean b = connection.query(n, createRes);
//                        if (!b) {
//                            DirectUtf8StringZ error = new DirectUtf8StringZ();
//                            createRes.getError(error);
//                        }
//                        Assert.assertTrue(b);
////                        DirectUtf8Sequence ins = new GcUtf8String("INSERT INTO integers VALUES (1), (2), (3), (4), (5)");
////                        connection.execute(ins);
//                        DirectUtf8Sequence q = new GcUtf8String("SELECT i FROM integers");
//                        try(DuckDBResult result = new DuckDBResult()) {
//                            boolean ok = connection.query(q, result);
//                            if (!ok) {
//                                DirectUtf8StringZ error = new DirectUtf8StringZ();
//                                createRes.getError(error);
//                            }
//                            Assert.assertTrue(ok);
//                            result.isClosed();
//                            Assert.assertFalse(result.isClosed());
//
//                            long dcCount = result.getDataChunkCount();
//                            Assert.assertEquals(chunks, dcCount);
//                            long rowCount = result.getRowCount();
//                            Assert.assertEquals(rows, rowCount);
//                            long columnCount = result.getColumnCount();
//                            Assert.assertEquals(1, columnCount);
//
//                            long columnType = DuckDB.getQdbColumnType(result.getColumnType(0));
//                            Assert.assertEquals(ColumnType.LONG, columnType);
//
//                            try(DuckDBPageFrameCursor cursor = new DuckDBPageFrameCursor(result)) {
//                                PageFrame frame;
//                                long counter = 1;
//                                while ((frame = cursor.next()) != null) {
//                                    long address = frame.getPageAddress(0);
//                                    Assert.assertNotEquals(0, address);
//                                    long idxAddress = frame.getIndexPageAddress(0);
//                                    Assert.assertEquals(0, idxAddress);
//                                    long count = frame.getPartitionHi() - frame.getPartitionLo();
////                                    Assert.assertEquals(rows, count);
//                                    long size = frame.getPageSize(0);
//                                    long bitshift = frame.getColumnShiftBits(0);
//                                    Assert.assertEquals(count << bitshift, size);
//                                    for (int i = 0; i < count; i++) {
//                                        long v = Unsafe.getUnsafe().getLong(address + i * 8L);
//                                        Assert.assertEquals(counter++, v);
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        );
//    }
//
//
//    //    public static native long connectionPrepare(long connection, long query_ptr, long query_size);
//    //    public static native long preparedExecute(long stmt);
//    //    public static native void preparedDestroy(long stmt);
//    //    public static native long preparedGetError(long stmt);
//    //    public static native long preparedGetQueryText(long stmt);
//    //    public static native int preparedGetStatementType(long stmt);
//    //    public static native int preparedGetStatementReturnType(long stmt);
//    //    public static native boolean preparedAllowStreamResult(long stmt);
//    //    public static native long preparedParameterCount(long stmt);
//    //    public static native long preparedGetColumnCount(long stmt);
//    //    public static native int preparedGetColumnLogicalType(long stmt, long col);
//    //    public static native int preparedGetColumnPhysicalType(long stmt, long col);
//    //    public static native long preparedGetColumnName(long stmt, long col);
//    //    public static native long resultGetError(long result);
//    //    public static native long resultFetchChunk(long result);
//    @Test
//    public void testPreparedStatement() throws Exception {
//        assertMemoryLeak(
//            () -> {
//                try (DuckDBInstance ddb = new DuckDBInstance()) {
//                    try (DuckDBConnection connection = ddb.getConnection()) {
//                        long rows = 20992;
//                        long chunks = rows / 2048 + 1;
//                        DirectUtf8Sequence n = new GcUtf8String("CREATE TABLE integers AS select generate_series i from generate_series(1," + rows + ");");
//                        DuckDBResult createRes = new DuckDBResult();
//                        boolean b = connection.query(n, createRes);
//                        if (!b) {
//                            DirectUtf8StringZ error = new DirectUtf8StringZ();
//                            createRes.getError(error);
//                        }
//                        Assert.assertTrue(b);
//
//                        DirectUtf8Sequence q = new GcUtf8String("SELECT i FROM integers");
//                        long stmt = connection.prepare(q);
//                        Assert.assertNotEquals(0, stmt);
//                        DirectUtf8StringZ str = new DirectUtf8StringZ();
//                        long error = DuckDB.preparedGetError(stmt);
////                        str.of(error);
////                        Assert.assertEquals("", str.toString());
//                        long queryText = DuckDB.preparedGetQueryText(stmt);
//                        str.of(queryText);
//                        Assert.assertEquals(q.toString(), str.toString());
//                        int statementType = DuckDB.preparedGetStatementType(stmt);
////                        Assert.assertEquals(DuckDB.DUCKDB_STATEMENT_TYPE_SELECT, statementType);
//                        int statementReturnType = DuckDB.preparedGetStatementReturnType(stmt);
////                        Assert.assertEquals(DuckDB.DUCKDB_STATEMENT_TYPE_SELECT, statementReturnType);
//                        boolean allowStreamResult = DuckDB.preparedAllowStreamResult(stmt);
//                        Assert.assertTrue(allowStreamResult);
//                        long parameterCount = DuckDB.preparedParameterCount(stmt);
//                        Assert.assertEquals(0, parameterCount);
//                        long columnCount = DuckDB.preparedGetColumnCount(stmt);
//                        Assert.assertEquals(1, columnCount);
//                        int columnLogicalType = DuckDB.preparedGetColumnLogicalType(stmt, 0);
////                        Assert.assertEquals(DuckDB.DUCKDB_TYPE_INTEGER, columnLogicalType);
//                        int columnPhysicalType = DuckDB.preparedGetColumnPhysicalType(stmt, 0);
////                        Assert.assertEquals(DuckDB.DUCKDB_TYPE_INTEGER, columnPhysicalType);
//                        long columnName = DuckDB.preparedGetColumnName(stmt, 0);
//                        str.of(columnName);
//                        Assert.assertEquals("i", str.toString());
//                        long res = DuckDB.preparedExecute(stmt);
//                        Assert.assertNotEquals(0, res);
//
//                        long counter = 1;
//                        while (true) {
//                            long chunk = DuckDB.resultFetchChunk(res);
//                            if (chunk == 0) {
//                                break;
//                            }
//                            long chunkSize = DuckDB.dataChunkGetSize(chunk);
//                            long v1 = DuckDB.dataChunkGetVector(chunk, 0);
//                            Assert.assertNotEquals(0, v1);
//                            long ct = DuckDB.vectorGetColumnType(v1);
//                            long data1 = DuckDB.vectorGetData(v1);
//                            Assert.assertNotEquals(0, data1);
//                            for (int j = 0; j < chunkSize; j++) {
//                                long v = Unsafe.getUnsafe().getLong(data1 + j * 8L);
//                                Assert.assertEquals(counter++, v);
//                            }
//                            long nulls = DuckDB.vectorGetValidity(v1);
//                            Assert.assertTrue(DuckDB.validityRowIsValid(nulls, 0));
//                            DuckDB.dataChunkDestroy(chunk);
//                        }
//
//                        DuckDB.preparedDestroy(stmt);
//                    }
//                }
//            });
//    }

    @Test
    public void testFactory() throws Exception {
        assertMemoryLeak(
            () -> {
                long rows = 10000;
                try (DuckDBInstance ddb = new DuckDBInstance()) {
                    DuckDBConnection connection = ddb.getConnection();
                    DirectUtf8Sequence n = new GcUtf8String("CREATE TABLE integers AS select generate_series i from generate_series(10," + rows + ");");
                    DuckDBResult createRes = new DuckDBResult();
                    boolean b = connection.query(n, createRes);
                    if (!b) {
                        DirectUtf8StringZ err = createRes.getErrorText();
                    }
                    Assert.assertTrue(b);
                    DirectUtf8Sequence q = new GcUtf8String("SELECT i, i - 1 as j FROM integers");
                    long stmt = connection.prepare(q);
                    Assert.assertNotEquals(0, stmt);
                    DuckDBPreparedStatement ps = new DuckDBPreparedStatement(stmt);
                    DuckDBRecordCursorFactory factory = new DuckDBRecordCursorFactory(ps, connection);
                    RecordMetadata metadata = factory.getMetadata();
                    Assert.assertEquals(2, metadata.getColumnCount());
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                    Record record = cursor.getRecord();
                    long counter = 1;
                    while (cursor.hasNext()) {
                        long v1 = record.getLong(0);
                        long v2 = record.getLong(1);
                        System.err.println(v1 + " " + v2);
//                                Assert.assertEquals(counter++, v);
                    }
                    cursor.close();
                    factory.close();
                }}
        );
    }

    @Test
    public void testLongCursor() throws Exception {
        assertQuery(
                "",
                "select * from quack('SELECT i, i - 1 as j FROM integers')",
                "create table x as " +
                        "(" +
                        "  select" +
                        "    rnd_long(0, 100, 0) a," +
                        "    timestamp_sequence(0, 10000) k" +
                        "  from long_sequence(3000)" +
                        ") timestamp(k)",
                null,
                false,
                false
        );

    }

    static {
        Os.init();
    }
}