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

package io.questdb.cliutil;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import org.junit.Assert;

public class TestUtils {
    public static void assertEquals(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            String expectedSql,
            String actualSql
    ) throws SqlException {
        try (
                RecordCursorFactory f1 = engine.select(expectedSql, sqlExecutionContext);
                RecordCursorFactory f2 = engine.select(actualSql, sqlExecutionContext);
                RecordCursor c1 = f1.getCursor(sqlExecutionContext);
                RecordCursor c2 = f2.getCursor(sqlExecutionContext)
        ) {
            assertEquals(c1, f1.getMetadata(), c2, f2.getMetadata(), true);
        }
    }

    public static void assertEquals(
            RecordCursor cursorExpected,
            RecordMetadata metadataExpected,
            RecordCursor cursorActual,
            RecordMetadata metadataActual,
            boolean genericStringMatch
    ) {
        assertEquals(metadataExpected, metadataActual, genericStringMatch);
        Record r = cursorExpected.getRecord();
        Record l = cursorActual.getRecord();

        long rowIndex = 0;
        while (cursorExpected.hasNext()) {
            if (!cursorActual.hasNext()) {
                Assert.fail("Actual cursor does not have record at " + rowIndex);
            }
            rowIndex++;
            assertColumnValues(metadataExpected, metadataActual, l, r, rowIndex, genericStringMatch);
        }

        Assert.assertFalse("Expected cursor misses record " + rowIndex, cursorActual.hasNext());
    }

    private static void assertColumnValues(
            RecordMetadata metadataExpected,
            RecordMetadata metadataActual,
            Record lr,
            Record rr,
            long rowIndex,
            boolean genericStringMatch
    ) {
        int columnType = 0;
        for (int i = 0, n = metadataExpected.getColumnCount(); i < n; i++) {
            String columnName = metadataExpected.getColumnName(i);
            try {
                columnType = metadataExpected.getColumnType(i);
                int tagType = ColumnType.tagOf(columnType);
                switch (tagType) {
                    case ColumnType.DATE:
                        Assert.assertEquals(rr.getDate(i), lr.getDate(i));
                        break;
                    case ColumnType.TIMESTAMP:
                        Assert.assertEquals(rr.getTimestamp(i), lr.getTimestamp(i));
                        break;
                    case ColumnType.DOUBLE:
                        Assert.assertEquals(rr.getDouble(i), lr.getDouble(i), 1E-6);
                        break;
                    case ColumnType.FLOAT:
                        Assert.assertEquals(rr.getFloat(i), lr.getFloat(i), 1E-3);
                        break;
                    case ColumnType.INT:
                    case ColumnType.IPv4:
                        Assert.assertEquals(rr.getInt(i), lr.getInt(i));
                        break;
                    case ColumnType.GEOINT:
                        Assert.assertEquals(rr.getGeoInt(i), lr.getGeoInt(i));
                        break;
                    case ColumnType.STRING:
                        CharSequence actual = genericStringMatch && ColumnType.isSymbol(metadataActual.getColumnType(i)) ? lr.getSymA(i) : lr.getStrA(i);
                        CharSequence expected = rr.getStrA(i);
                        if (expected != actual && !Chars.equalsNc(actual, expected)) {
                            Assert.assertEquals(expected, actual);
                        }
                        break;
                    case ColumnType.SYMBOL:
                        Assert.assertEquals(rr.getSymA(i), lr.getSymA(i));
                        break;
                    case ColumnType.SHORT:
                        Assert.assertEquals(rr.getShort(i), lr.getShort(i));
                        break;
                    case ColumnType.CHAR:
                        Assert.assertEquals(rr.getChar(i), lr.getChar(i));
                        break;
                    case ColumnType.GEOSHORT:
                        Assert.assertEquals(rr.getGeoShort(i), lr.getGeoShort(i));
                        break;
                    case ColumnType.LONG:
                        Assert.assertEquals(rr.getLong(i), lr.getLong(i));
                        break;
                    case ColumnType.GEOLONG:
                        Assert.assertEquals(rr.getGeoLong(i), lr.getGeoLong(i));
                        break;
                    case ColumnType.GEOBYTE:
                        Assert.assertEquals(rr.getGeoByte(i), lr.getGeoByte(i));
                        break;
                    case ColumnType.BYTE:
                        Assert.assertEquals(rr.getByte(i), lr.getByte(i));
                        break;
                    case ColumnType.BOOLEAN:
                        Assert.assertEquals(rr.getBool(i), lr.getBool(i));
                        break;
                    case ColumnType.LONG256:
                    case ColumnType.BINARY:
                        throw new UnsupportedOperationException();
                    case ColumnType.UUID:
                        // fall through
                    case ColumnType.LONG128:
                        Assert.assertEquals(rr.getLong128Hi(i), lr.getLong128Hi(i));
                        Assert.assertEquals(rr.getLong128Lo(i), lr.getLong128Hi(i));
                        break;
                    default:
                        // Unknown record type.
                        assert false;
                        break;
                }
            } catch (AssertionError e) {
                throw new AssertionError(String.format("Row %d column %s[%s] %s", rowIndex, columnName, ColumnType.nameOf(columnType), e.getMessage()));
            }
        }
    }

    private static void assertEquals(RecordMetadata metadataExpected, RecordMetadata metadataActual, boolean genericStringMatch) {
        Assert.assertEquals("Column count must be same", metadataExpected.getColumnCount(), metadataActual.getColumnCount());
        for (int i = 0, n = metadataExpected.getColumnCount(); i < n; i++) {
            Assert.assertEquals("Column name " + i, metadataExpected.getColumnName(i), metadataActual.getColumnName(i));
            int columnType1 = metadataExpected.getColumnType(i);
            columnType1 = genericStringMatch && ColumnType.isSymbol(columnType1) ? ColumnType.STRING : columnType1;
            int columnType2 = metadataActual.getColumnType(i);
            columnType2 = genericStringMatch && ColumnType.isSymbol(columnType2) ? ColumnType.STRING : columnType2;
            Assert.assertEquals("Column type " + i, columnType1, columnType2);
        }
    }
}
