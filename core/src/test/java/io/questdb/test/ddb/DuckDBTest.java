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

package io.questdb.test.ddb;

import io.questdb.duckdb.DuckDB;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.GcUtf8String;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class DuckDBTest extends AbstractTest {
    @Test
    public void testCreate() {
        long db = DuckDB.databaseOpen(0, 0);
        Assert.assertNotEquals(0, db);
        long conn = DuckDB.databaseConnect(db);
        Assert.assertNotEquals(0, conn);

        DirectUtf8Sequence n = new GcUtf8String("CREATE TABLE integers(i INTEGER)");
        DirectUtf8Sequence i = new GcUtf8String("INSERT INTO integers VALUES (1), (2), (3), (4), (5)");
        DirectUtf8Sequence s = new GcUtf8String("SELECT MAX(i), MIN(i) FROM integers");

        DuckDB.connectionExec(conn, n.ptr(), n.size());
        DuckDB.connectionExec(conn, i.ptr(), i.size());
        long result = DuckDB.connectionQuery(conn, s.ptr(), s.size());
        Assert.assertNotEquals(0, result);

        Assert.assertEquals(2, DuckDB.resultColumnCount(result));
        Assert.assertEquals(1, DuckDB.resultRowCount(result));

        long chunkCount = DuckDB.resultDataChunkCount(result);
        Assert.assertEquals(1, chunkCount);

        long chunk = DuckDB.resultGetDataChunk(result, 0);
        Assert.assertNotEquals(0, chunk);

        long columnCount = DuckDB.dataChunkGetColumnCount(chunk);
        Assert.assertEquals(2, columnCount);

        DirectUtf8StringZ name = new DirectUtf8StringZ();

        int columnType0 = DuckDB.resultColumnType(result, 0);
        Assert.assertEquals(DuckDB.DUCKDB_TYPE_INTEGER, columnType0);
        name.of(DuckDB.resultColumnName(result, 0));
        Assert.assertEquals("max(i)", name.toString());

        int columnType1 = DuckDB.resultColumnType(result, 1);
        Assert.assertEquals(DuckDB.DUCKDB_TYPE_INTEGER, columnType1);
        name.of(DuckDB.resultColumnName(result, 1));
        Assert.assertEquals("min(i)", name.toString());

        long chunkSize = DuckDB.dataChunkGetSize(chunk);
        Assert.assertEquals(1, chunkSize);

        long v1 = DuckDB.dataChunkGetVector(chunk, 0);
        Assert.assertNotEquals(0, v1);
        Assert.assertEquals(DuckDB.DUCKDB_TYPE_INTEGER, DuckDB.vectorGetColumnType(v1));

        long data1 = DuckDB.vectorGetData(v1);
        Assert.assertNotEquals(0, data1);
        for (int j = 0; j < chunkSize; j++) {
            int v = Unsafe.getUnsafe().getInt(data1 + j * 4L);
            Assert.assertEquals(5, v);
        }

        long nulls = DuckDB.vectorGetValidity(v1);
        Assert.assertTrue(DuckDB.validityRowIsValid(nulls, 0));

        long v2 = DuckDB.dataChunkGetVector(chunk, 1);
        Assert.assertNotEquals(0, v2);
        Assert.assertEquals(DuckDB.DUCKDB_TYPE_INTEGER, DuckDB.vectorGetColumnType(v2));

        long data2 = DuckDB.vectorGetData(v2);
        for (int j = 0; j < chunkSize; j++) {
            int v = Unsafe.getUnsafe().getInt(data2 + j * 4L);
            Assert.assertEquals(1, v);
        }

        long nulls2 = DuckDB.vectorGetValidity(v2);
        Assert.assertTrue(DuckDB.validityRowIsValid(nulls2, 0));

        DuckDB.dataChunkDestroy(chunk);
        DuckDB.resultDestroy(result);
        DuckDB.connectionDisconnect(conn);
        DuckDB.databaseClose(db);
    }

    static {
        Os.init();
    }
}