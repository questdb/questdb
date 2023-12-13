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

import io.questdb.duckdb.*;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.GcUtf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DuckDBTest extends AbstractCairoTest {
    public static void checkText(long message, CharSequence expected) {
        if (message != 0) {
            DirectUtf8StringZ txt = new DirectUtf8StringZ();
            txt.of(message);
            Assert.assertEquals(expected.toString(), txt.toString());
        } else {
            Assert.fail("Expected: " + expected);
        }
    }

    public static int setConfigOption(long config, CharSequence name, CharSequence option) {
        DirectUtf8Sequence name8 = new GcUtf8String(name.toString());
        DirectUtf8Sequence option8 = new GcUtf8String(option.toString());
        return DuckDB.configSet(config, name8.ptr(), name8.size(), option8.ptr(), option8.size());
    }

    public static long queryNoFail(long conn, CharSequence query) {
        DirectUtf8Sequence utf8Sequence = new GcUtf8String(query.toString());
        final long res = DuckDB.connectionQuery(conn, utf8Sequence.ptr(), utf8Sequence.size());
        Assert.assertNotEquals(0, res); // result of error
        final long err = DuckDB.resultGetError(res);
        if (err != 0) {
            DirectUtf8StringZ error = new DirectUtf8StringZ();
            error.of(err);
            Assert.fail("Query failed with message: " + error);
        }
        return res;
    }

    public static long prepareNoFail(long conn, CharSequence stmt) {
        DirectUtf8Sequence utf8Sequence = new GcUtf8String(stmt.toString());
        final long res = DuckDB.connectionPrepare(conn, utf8Sequence.ptr(), utf8Sequence.size());
        Assert.assertNotEquals(0, res); // result of error
        final long err = DuckDB.preparedGetError(res);
        if (err != 0) {
            DirectUtf8StringZ error = new DirectUtf8StringZ();
            error.of(err);
            Assert.fail("Prepared Statement failed with message: " + error);
        }
        return res;
    }

    public static long executeNoFail(long stmt) {
        final long res = DuckDB.preparedExecute(stmt);
        if (res == 0) {
            DirectUtf8StringZ error = new DirectUtf8StringZ();
            error.of(DuckDB.errorMessage());
            Assert.fail("Prepared Statement execute failed [error=" + DuckDB.errorType() + ", message=" + error + "]");
        }
        final long err = DuckDB.resultGetError(res);
        Assert.assertEquals(0, err);
        return res;
    }

    @Test
    public void testConnectionLifetime() {
        final long db = DuckDB.databaseOpen(0, 0);
        Assert.assertNotEquals(0, db);
        final long conn = DuckDB.databaseConnect(db);
        Assert.assertNotEquals(0, conn);
        DuckDB.databaseClose(db);

        final long createStmt = prepareNoFail(conn, "CREATE TABLE integers(i INTEGER)");
        DuckDB.resultDestroy(executeNoFail(createStmt));
        DuckDB.preparedDestroy(createStmt);

        final long insertStmt1 = prepareNoFail(conn, "INSERT INTO integers VALUES (1), (2), (3), (4), (5)");
        final long insertStmt2 = prepareNoFail(conn, "INSERT INTO integers VALUES (6), (7), (8), (9), (10)");
        final long selectStmt = prepareNoFail(conn, "SELECT * FROM integers");

        DuckDB.connectionDisconnect(conn);

        DuckDB.resultDestroy(executeNoFail(insertStmt1));
        DuckDB.resultDestroy(executeNoFail(insertStmt1));
        DuckDB.resultDestroy(executeNoFail(insertStmt2));
        DuckDB.preparedDestroy(insertStmt1);
        DuckDB.preparedDestroy(insertStmt2);

        final long selectResult = executeNoFail(selectStmt);
        DuckDB.preparedDestroy(selectStmt);

        long materialized = DuckDB.resultGetMaterialized(selectResult);
        long rows = DuckDB.resultRowCount(materialized);
        Assert.assertEquals(15, rows);
        DuckDB.resultDestroy(selectResult);
        DuckDB.resultDestroy(materialized);
    }

    @Test
    public void testConfig() {
        final long configPtr = DuckDB.configCreate();
        Assert.assertNotEquals(0, configPtr);

        Assert.assertEquals(1, setConfigOption(configPtr, "user", "questdb"));
        Assert.assertEquals(0, setConfigOption(configPtr, "user1", "questdb")); // unknown option
        Assert.assertEquals(-1, setConfigOption(configPtr, "access_mode", "all"));
        Assert.assertEquals(DuckDB.ERROR_TYPE_INVALID_INPUT, DuckDB.errorType());

        checkText(DuckDB.errorMessage(), "Invalid Input Error: Unrecognized parameter for option ACCESS_MODE \"all\". Expected READ_ONLY or READ_WRITE.");
        DuckDB.configDestroy(configPtr);
    }

    @Test
    public void testCreateInsertQueryTable() {
        final long db = DuckDB.databaseOpen(0, 0);
        Assert.assertNotEquals(0, db);
        final long conn = DuckDB.databaseConnect(db);
        Assert.assertNotEquals(0, conn);

        final long createStmt = prepareNoFail(conn, "CREATE TABLE integers(i INTEGER)");
        checkText(DuckDB.preparedGetQueryText(createStmt), "CREATE TABLE integers(i INTEGER)");

        // test properties
        final long properties = DuckDB.preparedGetStatementProperties(createStmt);
        Assert.assertEquals(DuckDB.STMT_TYPE_CREATE_STATEMENT, DuckDB.decodeStatementType(properties));
        Assert.assertEquals(DuckDB.STMT_RETURN_TYPE_NOTHING, DuckDB.decodeStatementReturnType(properties));
        Assert.assertEquals(0, DuckDB.decodeStatementParameterCount(properties));

        final long res = executeNoFail(createStmt);
        Assert.assertEquals(DuckDB.QUERY_MATERIALIZED_RESULT, DuckDB.resultGetQueryResultType(res));

        final long rowCount = DuckDB.resultRowCount(res);
        Assert.assertEquals(0, rowCount);

        final long insertStmt = prepareNoFail(conn, "INSERT INTO integers VALUES (1), (2), (3), (4), (5)");
        final long insProperties = DuckDB.preparedGetStatementProperties(insertStmt);
        Assert.assertEquals(DuckDB.STMT_TYPE_INSERT_STATEMENT, DuckDB.decodeStatementType(insProperties));
        Assert.assertEquals(DuckDB.STMT_RETURN_TYPE_CHANGED_ROWS, DuckDB.decodeStatementReturnType(insProperties));
        Assert.assertEquals(0, DuckDB.decodeStatementParameterCount(insProperties));

        final long insRes = executeNoFail(insertStmt);
        Assert.assertEquals(DuckDB.QUERY_MATERIALIZED_RESULT, DuckDB.resultGetQueryResultType(insRes));

        final long selectStmt = prepareNoFail(conn, "SELECT MAX(i), MIN(i) FROM integers");
        final long selProperties = DuckDB.preparedGetStatementProperties(selectStmt);
        Assert.assertEquals(DuckDB.STMT_TYPE_SELECT_STATEMENT, DuckDB.decodeStatementType(selProperties));
        Assert.assertEquals(DuckDB.STMT_RETURN_TYPE_QUERY_RESULT, DuckDB.decodeStatementReturnType(selProperties));
        Assert.assertEquals(0, DuckDB.decodeStatementParameterCount(selProperties));

        final long columnCount = DuckDB.preparedGetColumnCount(selectStmt);
        Assert.assertEquals(2, columnCount);

        final long colTypes1 = DuckDB.preparedGetColumnTypes(selectStmt, 0);
        Assert.assertEquals(DuckDB.COLUMN_TYPE_INTEGER, DuckDB.decodeLogicalTypeId(colTypes1));

        final long colTypes2 = DuckDB.preparedGetColumnTypes(selectStmt, 0);
        Assert.assertEquals(DuckDB.COLUMN_TYPE_INTEGER, DuckDB.decodeLogicalTypeId(colTypes2));

        checkText(DuckDB.preparedGetColumnName(selectStmt, 0), "max(i)");
        checkText(DuckDB.preparedGetColumnName(selectStmt, 1), "min(i)");

        final long selRes = executeNoFail(selectStmt);
        Assert.assertEquals(DuckDB.QUERY_STREAM_RESULT, DuckDB.resultGetQueryResultType(selRes));

        while (true) {
            long chunk = DuckDB.resultFetchChunk(selRes);
            if (chunk == 0) {
                break;
            }
            long rows = DuckDB.dataChunkGetSize(chunk);
            Assert.assertEquals(1, rows);
            long cols = DuckDB.dataChunkGetColumnCount(chunk);
            Assert.assertEquals(2, cols);

            // check max column
            long maxVec = DuckDB.dataChunkGetVector(chunk, 0);
            Assert.assertNotEquals(0, maxVec);
            Assert.assertEquals(DuckDB.COLUMN_TYPE_INTEGER, DuckDB.decodeLogicalTypeId(DuckDB.vectorGetColumnTypes(maxVec)));
            Assert.assertTrue(DuckDB.validityRowIsValid(DuckDB.vectorGetValidity(maxVec), 0));
            long maxData = DuckDB.vectorGetData(maxVec);
            Assert.assertNotEquals(0, maxData);
            Assert.assertEquals(5, Unsafe.getUnsafe().getInt(maxData));

            // check min column
            long minVec = DuckDB.dataChunkGetVector(chunk, 1);
            Assert.assertNotEquals(0, minVec);
            Assert.assertEquals(DuckDB.COLUMN_TYPE_INTEGER, DuckDB.decodeLogicalTypeId(DuckDB.vectorGetColumnTypes(minVec)));
            Assert.assertTrue(DuckDB.validityRowIsValid(DuckDB.vectorGetValidity(minVec), 1));
            long minData = DuckDB.vectorGetData(minVec);
            Assert.assertNotEquals(0, minData);
            Assert.assertEquals(1, Unsafe.getUnsafe().getInt(minData));

            DuckDB.dataChunkDestroy(chunk);
        }

        DuckDB.preparedDestroy(createStmt);
        DuckDB.preparedDestroy(insertStmt);
        DuckDB.preparedDestroy(selectStmt);

        DuckDB.connectionDisconnect(conn);
        DuckDB.databaseClose(db);
    }

    @Test
    public void testMaterializedQueryResult() {
        final long db = DuckDB.databaseOpen(0, 0);
        final long conn = DuckDB.databaseConnect(db);

        long rows = 20992;
        long chunks = rows / 2048 + 1;
        DuckDB.resultDestroy(queryNoFail(conn, "CREATE TABLE integers AS select generate_series i from generate_series(1,"+rows+");"));

        final long materialized = queryNoFail(conn, "SELECT i FROM integers");
        Assert.assertEquals(DuckDB.QUERY_MATERIALIZED_RESULT, DuckDB.resultGetQueryResultType(materialized));
        Assert.assertEquals(rows, DuckDB.resultRowCount(materialized));
        Assert.assertEquals(1, DuckDB.resultColumnCount(materialized));
        Assert.assertEquals(DuckDB.COLUMN_TYPE_BIGINT, DuckDB.decodeLogicalTypeId(DuckDB.resultColumnTypes(materialized, 0)));
        checkText(DuckDB.resultColumnName(materialized, 0), "i");
        Assert.assertEquals(chunks, DuckDB.resultDataChunkCount(materialized));

        long sequence = 1;
        // scan materialized result twice
        getResultData(chunks, materialized, sequence);
        sequence = getResultData(chunks, materialized, sequence);

        Assert.assertEquals(sequence, rows + 1);
        DuckDB.resultDestroy(materialized);

        DuckDB.connectionDisconnect(conn);
        DuckDB.databaseClose(db);
    }

    private static long getResultData(long chunks, long materialized, long sequence) {
        for (int chunkIdx = 0; chunkIdx < chunks; chunkIdx++) {
            final long chunk = DuckDB.resultGetDataChunk(materialized, chunkIdx);
            Assert.assertNotEquals(0, chunk);

            long column = DuckDB.dataChunkGetVector(chunk, 0);
            Assert.assertNotEquals(0, column);
            Assert.assertEquals(DuckDB.COLUMN_TYPE_BIGINT, DuckDB.decodeLogicalTypeId(DuckDB.vectorGetColumnTypes(column)));
            Assert.assertTrue(DuckDB.validityRowIsValid(DuckDB.vectorGetValidity(column), 0));
            long data = DuckDB.vectorGetData(column);
            Assert.assertNotEquals(0, data);
            long chunkSize = DuckDB.dataChunkGetSize(chunk);
            for (int j = 0; j < chunkSize; j++) {
                Assert.assertEquals(sequence++, Unsafe.getUnsafe().getLong(data + j * 8L));
            }
            DuckDB.dataChunkDestroy(chunk);
        }
        return sequence;
    }

    @Test
    public void testStreamingQueryResult() {
        final long db = DuckDB.databaseOpen(0, 0);
        final long conn = DuckDB.databaseConnect(db);

        long rows = 20992;
        long chunks = rows / 2048 + 1;
        DuckDB.resultDestroy(queryNoFail(conn, "CREATE TABLE integers AS select generate_series i from generate_series(1,"+rows+");"));

        final long streaming = prepareNoFail(conn, "SELECT i FROM integers");
        Assert.assertEquals(DuckDB.COLUMN_TYPE_BIGINT, DuckDB.decodeLogicalTypeId(DuckDB.preparedGetColumnTypes(streaming, 0)));
        Assert.assertEquals(1, DuckDB.preparedGetColumnCount(streaming));
        checkText(DuckDB.preparedGetColumnName(streaming, 0), "i");

        final long streamingResult = executeNoFail(streaming);
        DuckDB.preparedDestroy(streaming);

        Assert.assertEquals(DuckDB.QUERY_STREAM_RESULT, DuckDB.resultGetQueryResultType(streamingResult));
        Assert.assertEquals(DuckDB.COLUMN_TYPE_BIGINT, DuckDB.decodeLogicalTypeId(DuckDB.resultColumnTypes(streamingResult, 0)));
        long sequence = 1;
        long chunkCount = 0;
        while (true) {
            final long chunk = DuckDB.resultFetchChunk(streamingResult);
            if (chunk == 0) {
                break;
            }
            Assert.assertNotEquals(0, chunk);

            chunkCount++;
            long column = DuckDB.dataChunkGetVector(chunk, 0);
            Assert.assertNotEquals(0, column);
            Assert.assertEquals(DuckDB.COLUMN_TYPE_BIGINT, DuckDB.decodeLogicalTypeId(DuckDB.vectorGetColumnTypes(column)));
            Assert.assertTrue(DuckDB.validityRowIsValid(DuckDB.vectorGetValidity(column), 0));
            long data = DuckDB.vectorGetData(column);
            Assert.assertNotEquals(0, data);
            long chunkSize = DuckDB.dataChunkGetSize(chunk);
            for (int j = 0; j < chunkSize; j++) {
                Assert.assertEquals(sequence++, Unsafe.getUnsafe().getLong(data + j * 8L));
            }
            DuckDB.dataChunkDestroy(chunk);
        }

        Assert.assertEquals(chunks, chunkCount);
        Assert.assertEquals(sequence, rows + 1);
        DuckDB.resultDestroy(streamingResult);

        DuckDB.connectionDisconnect(conn);
        DuckDB.databaseClose(db);
    }

    @Test
    public void concurrentSequenceRead() throws InterruptedException {
        final long db = DuckDB.databaseOpen(0, 0);
        final long conn = DuckDB.databaseConnect(db);

        final int threads = 4;
        final int iters = 100;
        DuckDB.resultDestroy(queryNoFail(conn, "CREATE SEQUENCE seq;"));
        List<Long> serial = new ArrayList<>(threads * iters);
        List<Long> concurrent = Collections.synchronizedList(new ArrayList<>(threads * iters));

        for (int i = 0; i < threads; i++) {
            updateCollection(iters, conn, serial);
        }

        DuckDB.resultDestroy(queryNoFail(conn, "DROP SEQUENCE seq;"));
        DuckDB.resultDestroy(queryNoFail(conn, "CREATE SEQUENCE seq;"));

        Thread[] ts = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            Thread th = new Thread(() -> {
                long conn1 = DuckDB.databaseConnect(db);
                updateCollection(iters, conn1, concurrent);
                DuckDB.connectionDisconnect(conn1);
            });
            th.start();
            ts[i] = th;
        }

        for (Thread t : ts) {
            t.join();
        }

        concurrent.sort(Long::compareTo);
        Assert.assertEquals(serial.size(), concurrent.size());
        for (int i = 0; i < serial.size(); i++) {
            Assert.assertEquals(serial.get(i), concurrent.get(i));
        }

        DuckDB.connectionDisconnect(conn);
        DuckDB.databaseClose(db);
    }

    private static void updateCollection(int iters, long conn, List<Long> longs) {
        for (int j = 0; j < iters; j++) {
            long res = queryNoFail(conn, "SELECT nextval('seq')");
            long chunk = DuckDB.resultGetDataChunk(res, 0);
            Assert.assertNotEquals(0, chunk);
            long column = DuckDB.dataChunkGetVector(chunk, 0);
            Assert.assertNotEquals(0, column);
            long data = DuckDB.vectorGetData(column);
            Assert.assertNotEquals(0, data);
            longs.add(Unsafe.getUnsafe().getLong(data));
            DuckDB.dataChunkDestroy(chunk);
        }
    }

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

//    @Test
//    public void testFactory() throws Exception {
//        assertMemoryLeak(
//            () -> {
//                long rows = 10000;
//                try (DuckDBInstance ddb = new DuckDBInstance()) {
//                    DuckDBConnection connection = ddb.getConnection();
//                    DirectUtf8Sequence n = new GcUtf8String("CREATE TABLE integers AS select generate_series i from generate_series(10," + rows + ");");
//                    DuckDBResult createRes = new DuckDBResult();
//                    boolean b = connection.query(n, createRes);
//                    if (!b) {
//                        DirectUtf8StringZ err = createRes.getErrorText();
//                    }
//                    Assert.assertTrue(b);
//                    DirectUtf8Sequence q = new GcUtf8String("SELECT i, i - 1 as j FROM integers");
//                    long stmt = connection.prepare(q);
//                    Assert.assertNotEquals(0, stmt);
//                    DuckDBPreparedStatement ps = new DuckDBPreparedStatement(stmt);
//                    DuckDBRecordCursorFactory factory = new DuckDBRecordCursorFactory(ps, connection);
//                    RecordMetadata metadata = factory.getMetadata();
//                    Assert.assertEquals(2, metadata.getColumnCount());
//                    RecordCursor cursor = factory.getCursor(sqlExecutionContext);
//                    Record record = cursor.getRecord();
//                    long counter = 1;
//                    while (cursor.hasNext()) {
//                        long v1 = record.getLong(0);
//                        long v2 = record.getLong(1);
//                        System.err.println(v1 + " " + v2);
////                                Assert.assertEquals(counter++, v);
//                    }
//                    cursor.close();
//                    factory.close();
//                }}
//        );
//    }
//
    static {
        Os.init();
    }
}