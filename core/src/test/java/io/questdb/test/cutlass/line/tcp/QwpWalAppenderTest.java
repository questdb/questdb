/*+*****************************************************************************
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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.cutlass.line.tcp.QwpWalAppender;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import io.questdb.std.Unsafe;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for QwpWalAppender.
 */
public class QwpWalAppenderTest {

    @Test
    public void testConstructor() {
        new QwpWalAppender(true, 127, 50).close();
    }

    @Test
    public void testConstructorAutoCreateDisabled() {
        new QwpWalAppender(false, 255, 50).close();
    }

    @Test
    public void testMapQuestDBTypeToQwpBoolean() {
        assertEquals(QwpConstants.TYPE_BOOLEAN, mapQuestDBTypeToQwp(ColumnType.BOOLEAN));
    }

    @Test
    public void testMapQuestDBTypeToQwpByte() {
        assertEquals(QwpConstants.TYPE_BYTE, mapQuestDBTypeToQwp(ColumnType.BYTE));
    }

    @Test
    public void testMapQuestDBTypeToQwpDate() {
        assertEquals(QwpConstants.TYPE_DATE, mapQuestDBTypeToQwp(ColumnType.DATE));
    }

    @Test
    public void testMapQuestDBTypeToQwpDouble() {
        assertEquals(QwpConstants.TYPE_DOUBLE, mapQuestDBTypeToQwp(ColumnType.DOUBLE));
    }

    @Test
    public void testMapQuestDBTypeToQwpFloat() {
        assertEquals(QwpConstants.TYPE_FLOAT, mapQuestDBTypeToQwp(ColumnType.FLOAT));
    }

    @Test
    public void testMapQuestDBTypeToQwpGeoByte() {
        assertEquals(QwpConstants.TYPE_GEOHASH, mapQuestDBTypeToQwp(ColumnType.GEOBYTE));
    }

    @Test
    public void testMapQuestDBTypeToQwpGeoInt() {
        assertEquals(QwpConstants.TYPE_GEOHASH, mapQuestDBTypeToQwp(ColumnType.GEOINT));
    }

    @Test
    public void testMapQuestDBTypeToQwpGeoLong() {
        assertEquals(QwpConstants.TYPE_GEOHASH, mapQuestDBTypeToQwp(ColumnType.GEOLONG));
    }

    @Test
    public void testMapQuestDBTypeToQwpGeoShort() {
        assertEquals(QwpConstants.TYPE_GEOHASH, mapQuestDBTypeToQwp(ColumnType.GEOSHORT));
    }

    @Test
    public void testMapQuestDBTypeToQwpInt() {
        assertEquals(QwpConstants.TYPE_INT, mapQuestDBTypeToQwp(ColumnType.INT));
    }

    @Test
    public void testMapQuestDBTypeToQwpLong() {
        assertEquals(QwpConstants.TYPE_LONG, mapQuestDBTypeToQwp(ColumnType.LONG));
    }

    @Test
    public void testMapQuestDBTypeToQwpLong256() {
        assertEquals(QwpConstants.TYPE_LONG256, mapQuestDBTypeToQwp(ColumnType.LONG256));
    }

    @Test
    public void testMapQuestDBTypeToQwpShort() {
        assertEquals(QwpConstants.TYPE_SHORT, mapQuestDBTypeToQwp(ColumnType.SHORT));
    }

    @Test
    public void testMapQuestDBTypeToQwpString() {
        // QuestDB STRING maps to wire TYPE_VARCHAR -- the wire protocol has no separate STRING code.
        assertEquals(QwpConstants.TYPE_VARCHAR, mapQuestDBTypeToQwp(ColumnType.STRING));
    }

    @Test
    public void testMapQuestDBTypeToQwpSymbol() {
        assertEquals(QwpConstants.TYPE_SYMBOL, mapQuestDBTypeToQwp(ColumnType.SYMBOL));
    }

    @Test
    public void testMapQuestDBTypeToQwpTimestamp() {
        assertEquals(QwpConstants.TYPE_TIMESTAMP, mapQuestDBTypeToQwp(ColumnType.TIMESTAMP));
    }

    @Test
    public void testMapQuestDBTypeToQwpTimestampNanos() {
        assertEquals(QwpConstants.TYPE_TIMESTAMP_NANOS, mapQuestDBTypeToQwp(ColumnType.TIMESTAMP_NANO));
    }

    @Test
    public void testMapQuestDBTypeToQwpUUID() {
        assertEquals(QwpConstants.TYPE_UUID, mapQuestDBTypeToQwp(ColumnType.UUID));
    }

    @Test(expected = CairoException.class)
    public void testMapQuestDBTypeToQwpUnsupported() {
        // IPV4 is not supported in QWP v1
        mapQuestDBTypeToQwp(ColumnType.IPv4);
    }

    @Test
    public void testMapQuestDBTypeToQwpVarchar() {
        assertEquals(QwpConstants.TYPE_VARCHAR, mapQuestDBTypeToQwp(ColumnType.VARCHAR));
    }

    @Test
    public void testMapQwpTypeToQuestDBBoolean() {
        assertEquals(ColumnType.BOOLEAN, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_BOOLEAN));
    }

    @Test
    public void testMapQwpTypeToQuestDBByte() {
        assertEquals(ColumnType.BYTE, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_BYTE));
    }

    @Test
    public void testMapQwpTypeToQuestDBChar() {
        assertEquals(ColumnType.CHAR, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_CHAR));
    }

    @Test
    public void testMapQwpTypeToQuestDBDate() {
        assertEquals(ColumnType.DATE, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_DATE));
    }

    @Test
    public void testMapQwpTypeToQuestDBDecimal128() {
        assertEquals(ColumnType.DECIMAL128, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_DECIMAL128));
    }

    @Test
    public void testMapQwpTypeToQuestDBDecimal256() {
        assertEquals(ColumnType.DECIMAL256, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_DECIMAL256));
    }

    @Test
    public void testMapQwpTypeToQuestDBDecimal64() {
        assertEquals(ColumnType.DECIMAL64, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_DECIMAL64));
    }

    @Test
    public void testMapQwpTypeToQuestDBDecimalScaleComesOnlyFromValues() throws Exception {
        // A decimal column that holds only NULLs carries a scale byte that means
        // nothing: the Java client sends 255, the Rust row API 0, and the Rust
        // column sender its own scale. Only a block with a value sets the scale.
        final byte[] typeCodes = {TYPE_DECIMAL64, TYPE_DECIMAL128, TYPE_DECIMAL256};
        final int[] tags = {ColumnType.DECIMAL64, ColumnType.DECIMAL128, ColumnType.DECIMAL256};
        final String[] namesWithValue = {"DECIMAL(18,2)", "DECIMAL(38,2)", "DECIMAL(76,2)"};
        final int[] nullBlockScales = {0, 1, 2, 76, 77, 255};
        final long clientMemUsedBefore = io.questdb.client.std.Unsafe.getMemUsed();
        assertMemoryLeak(() -> {
            try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                for (int t = 0; t < typeCodes.length; t++) {
                    for (int scale : nullBlockScales) {
                        final int columnType = mapDecimalColumn(decoder, typeCodes[t], false, scale);
                        assertEquals(
                                QwpConstants.getTypeName(typeCodes[t]) + ", scale byte " + scale
                                        + " mapped to " + ColumnType.nameOf(columnType),
                                ColumnType.UNDEFINED,
                                columnType
                        );
                    }
                    final int columnType = mapDecimalColumn(decoder, typeCodes[t], true, -1);
                    assertEquals(namesWithValue[t], ColumnType.nameOf(columnType));
                    assertEquals(namesWithValue[t], tags[t], ColumnType.tagOf(columnType));
                }
            }
        });
        assertEquals("client native memory leaked", clientMemUsedBefore, io.questdb.client.std.Unsafe.getMemUsed());
    }

    @Test
    public void testMapQwpTypeToQuestDBDoubleArray() {
        assertEquals(
                ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, false),
                QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_DOUBLE_ARRAY)
        );
    }

    @Test
    public void testMapQwpTypeToQuestDBDouble() {
        assertEquals(ColumnType.DOUBLE, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_DOUBLE));
    }

    @Test
    public void testMapQwpTypeToQuestDBFloat() {
        assertEquals(ColumnType.FLOAT, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_FLOAT));
    }

    @Test
    public void testMapQwpTypeToQuestDBGeoHash() {
        assertEquals(ColumnType.GEOLONG, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_GEOHASH));
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
    public void testMapQwpTypeToQuestDBLong256() {
        assertEquals(ColumnType.LONG256, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_LONG256));
    }

    @Test(expected = CairoException.class)
    public void testMapQwpTypeToQuestDBLongArrayRejected() {
        QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_LONG_ARRAY);
    }

    @Test
    public void testMapQwpTypeToQuestDBShort() {
        assertEquals(ColumnType.SHORT, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_SHORT));
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
    public void testMapQwpTypeToQuestDBTimestampNanos() {
        assertEquals(ColumnType.TIMESTAMP_NANO, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_TIMESTAMP_NANOS));
    }

    @Test
    public void testMapQwpTypeToQuestDBUUID() {
        assertEquals(ColumnType.UUID, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_UUID));
    }

    @Test(expected = CairoException.class)
    public void testMapQwpTypeToQuestDBUnknown() {
        QwpWalAppender.mapQwpTypeToQuestDB(0xFF);
    }

    @Test
    public void testMapQwpTypeToQuestDBVarchar() {
        assertEquals(ColumnType.VARCHAR, QwpWalAppender.mapQwpTypeToQuestDB(QwpConstants.TYPE_VARCHAR));
    }

    @Test
    public void testRoundTripAllTypes() {
        // Test that supported types round-trip correctly
        int[] ilpTypes = {
                QwpConstants.TYPE_BOOLEAN,
                QwpConstants.TYPE_BYTE,
                QwpConstants.TYPE_SHORT,
                QwpConstants.TYPE_CHAR,
                QwpConstants.TYPE_INT,
                QwpConstants.TYPE_LONG,
                QwpConstants.TYPE_FLOAT,
                QwpConstants.TYPE_DOUBLE,
                QwpConstants.TYPE_VARCHAR,
                QwpConstants.TYPE_SYMBOL,
                QwpConstants.TYPE_TIMESTAMP,
                QwpConstants.TYPE_TIMESTAMP_NANOS,
                QwpConstants.TYPE_DATE,
                QwpConstants.TYPE_UUID,
                QwpConstants.TYPE_LONG256
        };

        for (int ilpType : ilpTypes) {
            int questdbType = QwpWalAppender.mapQwpTypeToQuestDB(ilpType);
            byte mappedBack = mapQuestDBTypeToQwp(questdbType);
            assertEquals("Round-trip failed for ILP type " + ilpType, ilpType, mappedBack & 0xFF);
        }
    }

    // Encodes a three-row block [x LONG, d <decimal>] with the Java client and maps
    // d to the type column auto-creation would give it. Without a value every row
    // of d is NULL, and the block ends in d's scale byte, which this sets to
    // nullBlockScale. With one, only the middle row holds a value: 1.23.
    private static int mapDecimalColumn(
            QwpStreamingDecoder decoder,
            byte typeCode,
            boolean hasValue,
            int nullBlockScale
    ) throws QwpParseException {
        try (
                QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                QwpTableBuffer buffer = new QwpTableBuffer("t")
        ) {
            final QwpTableBuffer.ColumnBuffer x = buffer.getOrCreateColumn("x", TYPE_LONG, false);
            final QwpTableBuffer.ColumnBuffer d = buffer.getOrCreateColumn("d", typeCode, true);
            for (int row = 0; row < 3; row++) {
                x.addLong(row);
                if (hasValue && row == 1) {
                    switch (typeCode) {
                        case TYPE_DECIMAL64 -> d.addDecimal64(Decimal64.fromLong(123, 2));
                        case TYPE_DECIMAL128 -> d.addDecimal128(Decimal128.fromLong(123, 2));
                        default -> d.addDecimal256(Decimal256.fromLong(123, 2));
                    }
                }
                buffer.nextRow();
            }
            final int size = encoder.encode(buffer);
            final long address = encoder.getBuffer().getBufferPtr();
            if (!hasValue) {
                // [null bitmap present][rows 0-2 NULL][scale placeholder]
                assertEquals(1, Unsafe.getByte(address + size - 3));
                assertEquals(0b111, Unsafe.getByte(address + size - 2));
                assertEquals((byte) 0xFF, Unsafe.getByte(address + size - 1));
                Unsafe.putByte(address + size - 1, (byte) nullBlockScale);
            }
            final QwpMessageCursor message = decoder.decode(address, size);
            assertTrue(message.hasNextTable());
            final QwpTableBlockCursor table = message.nextTable();
            assertEquals(3, table.getRowCount());
            assertEquals("d", table.getColumnDef(1).getName());
            return QwpWalAppender.mapQwpTypeToQuestDB(typeCode, table, 1);
        }
    }

    private static byte mapQuestDBTypeToQwp(int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN -> TYPE_BOOLEAN;
            case ColumnType.BYTE -> TYPE_BYTE;
            case ColumnType.SHORT -> TYPE_SHORT;
            case ColumnType.INT -> TYPE_INT;
            case ColumnType.LONG -> TYPE_LONG;
            case ColumnType.FLOAT -> TYPE_FLOAT;
            case ColumnType.DOUBLE -> TYPE_DOUBLE;
            case ColumnType.CHAR -> TYPE_CHAR;
            case ColumnType.STRING, ColumnType.VARCHAR -> TYPE_VARCHAR;
            case ColumnType.SYMBOL -> TYPE_SYMBOL;
            case ColumnType.TIMESTAMP ->
                    columnType == ColumnType.TIMESTAMP_NANO ? TYPE_TIMESTAMP_NANOS : TYPE_TIMESTAMP;
            case ColumnType.DATE -> TYPE_DATE;
            case ColumnType.UUID -> TYPE_UUID;
            case ColumnType.LONG256 -> TYPE_LONG256;
            case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG -> TYPE_GEOHASH;
            default -> throw CairoException.nonCritical().put("unsupported QuestDB type: ").put(columnType);
        };
    }
}
