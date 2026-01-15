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
import io.questdb.cutlass.line.tcp.IlpV4WalAppender;
import io.questdb.cutlass.http.ilpv4.IlpV4Constants;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for IlpV4WalAppender.
 */
public class IlpV4WalAppenderTest {

    // ==================== Type Mapping Tests ====================

    @Test
    public void testMapIlpV4TypeToQuestDBBoolean() {
        assertEquals(ColumnType.BOOLEAN, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_BOOLEAN));
    }

    @Test
    public void testMapIlpV4TypeToQuestDBByte() {
        assertEquals(ColumnType.BYTE, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_BYTE));
    }

    @Test
    public void testMapIlpV4TypeToQuestDBShort() {
        assertEquals(ColumnType.SHORT, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_SHORT));
    }

    @Test
    public void testMapIlpV4TypeToQuestDBInt() {
        assertEquals(ColumnType.INT, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_INT));
    }

    @Test
    public void testMapIlpV4TypeToQuestDBLong() {
        assertEquals(ColumnType.LONG, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_LONG));
    }

    @Test
    public void testMapIlpV4TypeToQuestDBFloat() {
        assertEquals(ColumnType.FLOAT, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_FLOAT));
    }

    @Test
    public void testMapIlpV4TypeToQuestDBDouble() {
        assertEquals(ColumnType.DOUBLE, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_DOUBLE));
    }

    @Test
    public void testMapIlpV4TypeToQuestDBString() {
        // ILP v4 TYPE_STRING maps to VARCHAR (not STRING) for consistency
        assertEquals(ColumnType.VARCHAR, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_STRING));
    }

    @Test
    public void testMapIlpV4TypeToQuestDBVarchar() {
        assertEquals(ColumnType.VARCHAR, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_VARCHAR));
    }

    @Test
    public void testMapIlpV4TypeToQuestDBSymbol() {
        assertEquals(ColumnType.SYMBOL, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_SYMBOL));
    }

    @Test
    public void testMapIlpV4TypeToQuestDBTimestamp() {
        assertEquals(ColumnType.TIMESTAMP, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_TIMESTAMP));
    }

    @Test
    public void testMapIlpV4TypeToQuestDBDate() {
        assertEquals(ColumnType.DATE, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_DATE));
    }

    @Test
    public void testMapIlpV4TypeToQuestDBUUID() {
        assertEquals(ColumnType.UUID, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_UUID));
    }

    @Test
    public void testMapIlpV4TypeToQuestDBLong256() {
        assertEquals(ColumnType.LONG256, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_LONG256));
    }

    @Test
    public void testMapIlpV4TypeToQuestDBGeoHash() {
        assertEquals(ColumnType.GEOLONG, IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_GEOHASH));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapIlpV4TypeToQuestDBUnknown() {
        IlpV4WalAppender.mapIlpV4TypeToQuestDB(0xFF);
    }

    // ==================== Reverse Mapping Tests ====================

    @Test
    public void testMapQuestDBTypeToIlpV4Boolean() {
        assertEquals(IlpV4Constants.TYPE_BOOLEAN, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.BOOLEAN));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4Byte() {
        assertEquals(IlpV4Constants.TYPE_BYTE, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.BYTE));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4Short() {
        assertEquals(IlpV4Constants.TYPE_SHORT, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.SHORT));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4Int() {
        assertEquals(IlpV4Constants.TYPE_INT, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.INT));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4Long() {
        assertEquals(IlpV4Constants.TYPE_LONG, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.LONG));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4Float() {
        assertEquals(IlpV4Constants.TYPE_FLOAT, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.FLOAT));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4Double() {
        assertEquals(IlpV4Constants.TYPE_DOUBLE, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.DOUBLE));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4String() {
        assertEquals(IlpV4Constants.TYPE_STRING, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.STRING));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4Varchar() {
        assertEquals(IlpV4Constants.TYPE_VARCHAR, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.VARCHAR));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4Symbol() {
        assertEquals(IlpV4Constants.TYPE_SYMBOL, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.SYMBOL));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4Timestamp() {
        assertEquals(IlpV4Constants.TYPE_TIMESTAMP, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.TIMESTAMP));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4Date() {
        assertEquals(IlpV4Constants.TYPE_DATE, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.DATE));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4UUID() {
        assertEquals(IlpV4Constants.TYPE_UUID, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.UUID));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4Long256() {
        assertEquals(IlpV4Constants.TYPE_LONG256, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.LONG256));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4GeoByte() {
        assertEquals(IlpV4Constants.TYPE_GEOHASH, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.GEOBYTE));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4GeoShort() {
        assertEquals(IlpV4Constants.TYPE_GEOHASH, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.GEOSHORT));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4GeoInt() {
        assertEquals(IlpV4Constants.TYPE_GEOHASH, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.GEOINT));
    }

    @Test
    public void testMapQuestDBTypeToIlpV4GeoLong() {
        assertEquals(IlpV4Constants.TYPE_GEOHASH, IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.GEOLONG));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapQuestDBTypeToIlpV4Unsupported() {
        // IPV4 is not supported in ILP v4
        IlpV4WalAppender.mapQuestDBTypeToIlpV4(ColumnType.IPv4);
    }

    // ==================== Round-trip Tests ====================

    @Test
    public void testRoundTripAllTypes() {
        // Test that supported types round-trip correctly
        // Note: TYPE_STRING is excluded because it intentionally maps to VARCHAR
        // (both TYPE_STRING and TYPE_VARCHAR map to QuestDB VARCHAR)
        int[] ilpTypes = {
                IlpV4Constants.TYPE_BOOLEAN,
                IlpV4Constants.TYPE_BYTE,
                IlpV4Constants.TYPE_SHORT,
                IlpV4Constants.TYPE_INT,
                IlpV4Constants.TYPE_LONG,
                IlpV4Constants.TYPE_FLOAT,
                IlpV4Constants.TYPE_DOUBLE,
                IlpV4Constants.TYPE_VARCHAR,
                IlpV4Constants.TYPE_SYMBOL,
                IlpV4Constants.TYPE_TIMESTAMP,
                IlpV4Constants.TYPE_DATE,
                IlpV4Constants.TYPE_UUID,
                IlpV4Constants.TYPE_LONG256
        };

        for (int ilpType : ilpTypes) {
            int questdbType = IlpV4WalAppender.mapIlpV4TypeToQuestDB(ilpType);
            byte mappedBack = IlpV4WalAppender.mapQuestDBTypeToIlpV4(questdbType);
            assertEquals("Round-trip failed for ILP type " + ilpType, ilpType, mappedBack & 0xFF);
        }
    }

    @Test
    public void testStringToVarcharMapping() {
        // TYPE_STRING intentionally maps to VARCHAR (lossy conversion)
        // This is by design: both STRING and VARCHAR in ILP v4 become VARCHAR in QuestDB
        int questdbType = IlpV4WalAppender.mapIlpV4TypeToQuestDB(IlpV4Constants.TYPE_STRING);
        assertEquals(ColumnType.VARCHAR, questdbType);

        byte mappedBack = IlpV4WalAppender.mapQuestDBTypeToIlpV4(questdbType);
        assertEquals(IlpV4Constants.TYPE_VARCHAR, mappedBack);
    }

    // ==================== Constructor Test ====================

    @Test
    public void testConstructor() {
        IlpV4WalAppender appender = new IlpV4WalAppender(true, 127);
        assertNotNull(appender);
    }

    @Test
    public void testConstructorAutoCreateDisabled() {
        IlpV4WalAppender appender = new IlpV4WalAppender(false, 255);
        assertNotNull(appender);
    }
}
