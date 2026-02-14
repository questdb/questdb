/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.line.tcp.QwpWalAppender;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for QwpWalAppender.
 */
public class QwpWalAppenderTest {

    // ==================== Type Mapping Tests ====================

    @Test
    public void testMapQwpTypeToQuestDBBoolean() {
        assertEquals(ColumnType.BOOLEAN, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_BOOLEAN));
    }

    @Test
    public void testMapQwpTypeToQuestDBByte() {
        assertEquals(ColumnType.BYTE, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_BYTE));
    }

    @Test
    public void testMapQwpTypeToQuestDBShort() {
        assertEquals(ColumnType.SHORT, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_SHORT));
    }

    @Test
    public void testMapQwpTypeToQuestDBInt() {
        assertEquals(ColumnType.INT, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_INT));
    }

    @Test
    public void testMapQwpTypeToQuestDBLong() {
        assertEquals(ColumnType.LONG, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_LONG));
    }

    @Test
    public void testMapQwpTypeToQuestDBFloat() {
        assertEquals(ColumnType.FLOAT, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_FLOAT));
    }

    @Test
    public void testMapQwpTypeToQuestDBDouble() {
        assertEquals(ColumnType.DOUBLE, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_DOUBLE));
    }

    @Test
    public void testMapQwpTypeToQuestDBString() {
        // ILP v4 TYPE_STRING maps to VARCHAR (not STRING) for consistency
        assertEquals(ColumnType.VARCHAR, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_STRING));
    }

    @Test
    public void testMapQwpTypeToQuestDBVarchar() {
        assertEquals(ColumnType.VARCHAR, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_VARCHAR));
    }

    @Test
    public void testMapQwpTypeToQuestDBSymbol() {
        assertEquals(ColumnType.SYMBOL, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_SYMBOL));
    }

    @Test
    public void testMapQwpTypeToQuestDBTimestamp() {
        assertEquals(ColumnType.TIMESTAMP, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_TIMESTAMP));
    }

    @Test
    public void testMapQwpTypeToQuestDBDate() {
        assertEquals(ColumnType.DATE, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_DATE));
    }

    @Test
    public void testMapQwpTypeToQuestDBUUID() {
        assertEquals(ColumnType.UUID, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_UUID));
    }

    @Test
    public void testMapQwpTypeToQuestDBLong256() {
        assertEquals(ColumnType.LONG256, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_LONG256));
    }

    @Test
    public void testMapQwpTypeToQuestDBGeoHash() {
        assertEquals(ColumnType.GEOLONG, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_GEOHASH));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapQwpTypeToQuestDBUnknown() {
        QwpWalAppender.mapQwpTypeToQuestDB(0xFF);
    }

    // ==================== Reverse Mapping Tests ====================

    @Test
    public void testMapQuestDBTypeToQwpBoolean() {
        assertEquals(QwpConstants.TYPE_BOOLEAN, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.BOOLEAN));
    }

    @Test
    public void testMapQuestDBTypeToQwpByte() {
        assertEquals(QwpConstants.TYPE_BYTE, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.BYTE));
    }

    @Test
    public void testMapQuestDBTypeToQwpShort() {
        assertEquals(QwpConstants.TYPE_SHORT, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.SHORT));
    }

    @Test
    public void testMapQuestDBTypeToQwpInt() {
        assertEquals(QwpConstants.TYPE_INT, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.INT));
    }

    @Test
    public void testMapQuestDBTypeToQwpLong() {
        assertEquals(QwpConstants.TYPE_LONG, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.LONG));
    }

    @Test
    public void testMapQuestDBTypeToQwpFloat() {
        assertEquals(QwpConstants.TYPE_FLOAT, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.FLOAT));
    }

    @Test
    public void testMapQuestDBTypeToQwpDouble() {
        assertEquals(QwpConstants.TYPE_DOUBLE, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.DOUBLE));
    }

    @Test
    public void testMapQuestDBTypeToQwpString() {
        assertEquals(QwpConstants.TYPE_STRING, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.STRING));
    }

    @Test
    public void testMapQuestDBTypeToQwpVarchar() {
        assertEquals(QwpConstants.TYPE_VARCHAR, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.VARCHAR));
    }

    @Test
    public void testMapQuestDBTypeToQwpSymbol() {
        assertEquals(QwpConstants.TYPE_SYMBOL, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.SYMBOL));
    }

    @Test
    public void testMapQuestDBTypeToQwpTimestamp() {
        assertEquals(QwpConstants.TYPE_TIMESTAMP, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.TIMESTAMP));
    }

    @Test
    public void testMapQuestDBTypeToQwpDate() {
        assertEquals(QwpConstants.TYPE_DATE, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.DATE));
    }

    @Test
    public void testMapQuestDBTypeToQwpUUID() {
        assertEquals(QwpConstants.TYPE_UUID, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.UUID));
    }

    @Test
    public void testMapQuestDBTypeToQwpLong256() {
        assertEquals(QwpConstants.TYPE_LONG256, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.LONG256));
    }

    @Test
    public void testMapQuestDBTypeToQwpGeoByte() {
        assertEquals(QwpConstants.TYPE_GEOHASH, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.GEOBYTE));
    }

    @Test
    public void testMapQuestDBTypeToQwpGeoShort() {
        assertEquals(QwpConstants.TYPE_GEOHASH, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.GEOSHORT));
    }

    @Test
    public void testMapQuestDBTypeToQwpGeoInt() {
        assertEquals(QwpConstants.TYPE_GEOHASH, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.GEOINT));
    }

    @Test
    public void testMapQuestDBTypeToQwpGeoLong() {
        assertEquals(QwpConstants.TYPE_GEOHASH, QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.GEOLONG));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapQuestDBTypeToQwpUnsupported() {
        // IPV4 is not supported in ILP v4
        QwpWalAppender.mapQuestDBTypeToQwp(ColumnType.IPv4);
    }

    // ==================== Round-trip Tests ====================

    @Test
    public void testRoundTripAllTypes() {
        // Test that supported types round-trip correctly
        // Note: TYPE_STRING is excluded because it intentionally maps to VARCHAR
        // (both TYPE_STRING and TYPE_VARCHAR map to QuestDB VARCHAR)
        int[] ilpTypes = {
                QwpConstants.TYPE_BOOLEAN,
                QwpConstants.TYPE_BYTE,
                QwpConstants.TYPE_SHORT,
                QwpConstants.TYPE_INT,
                QwpConstants.TYPE_LONG,
                QwpConstants.TYPE_FLOAT,
                QwpConstants.TYPE_DOUBLE,
                QwpConstants.TYPE_VARCHAR,
                QwpConstants.TYPE_SYMBOL,
                QwpConstants.TYPE_TIMESTAMP,
                QwpConstants.TYPE_DATE,
                QwpConstants.TYPE_UUID,
                QwpConstants.TYPE_LONG256
        };

        for (int ilpType : ilpTypes) {
            int questdbType = QwpWalAppender.mapQwpTypeToQuestDB(ilpType);
            byte mappedBack = QwpWalAppender.mapQuestDBTypeToQwp(questdbType);
            assertEquals("Round-trip failed for ILP type " + ilpType, ilpType, mappedBack & 0xFF);
        }
    }

    @Test
    public void testStringToVarcharMapping() {
        // TYPE_STRING intentionally maps to VARCHAR (lossy conversion)
        // This is by design: both STRING and VARCHAR in ILP v4 become VARCHAR in QuestDB
        int questdbType = QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_STRING);
        assertEquals(ColumnType.VARCHAR, questdbType);

        byte mappedBack = QwpWalAppender.mapQuestDBTypeToQwp(questdbType);
        assertEquals(QwpConstants.TYPE_VARCHAR, mappedBack);
    }

    // ==================== Constructor Test ====================

    @Test
    public void testConstructor() {
        QwpWalAppender appender = new QwpWalAppender(true, 127);
        assertNotNull(appender);
    }

    @Test
    public void testConstructorAutoCreateDisabled() {
        QwpWalAppender appender = new QwpWalAppender(false, 255);
        assertNotNull(appender);
    }
}
