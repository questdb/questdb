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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.questdb.test.cutlass.qwp.e2e.QwpSchemaE2ETestUtils.connectLegacyClient;
import static io.questdb.test.cutlass.qwp.e2e.QwpSchemaE2ETestUtils.connectSchemaClient;
import static io.questdb.test.cutlass.qwp.e2e.QwpSchemaE2ETestUtils.receiveResponse;

/**
 * Pins which wire types the server converts into which column types.
 * <p>
 * A sender that asked for no schema encodes each setter's own type. That type says
 * nothing about the column, so the server converts it exactly as it did before
 * schema-aware ingestion and rejects every other pair, whatever the column's history.
 * <p>
 * A schema-aware sender encodes the column's type. After ALTER COLUMN TYPE its
 * frames in flight and in store-and-forward carry the column's old type, which the
 * server converts as the ALTER converted the rows already in the table; see
 * {@link QwpSchemaStaleAlterE2ETest}. Those conversions assume the wire type was
 * the column's type, so the server applies them only on a connection that
 * negotiated schema support.
 */
public class QwpWireTypeMatrixE2ETest extends AbstractQwpWebSocketTest {
    private static final String[] COLUMN_TYPES = {
            "BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP", "TIMESTAMP_NS",
            "STRING", "VARCHAR", "SYMBOL", "IPV4", "UUID", "CHAR",
            "DECIMAL(2,1)", "DECIMAL(4,2)", "DECIMAL(9,3)", "DECIMAL(18,2)", "DECIMAL(38,4)", "DECIMAL(76,6)",
            "LONG256", "GEOHASH(4c)", "BINARY", "DOUBLE[]"
    };
    private static final String DECIMALS = "DECIMAL(2,1), DECIMAL(4,2), DECIMAL(9,3), DECIMAL(18,2), DECIMAL(38,4), DECIMAL(76,6)";
    private static final String NUMBERS = "BYTE, SHORT, INT, LONG, FLOAT, DOUBLE";
    // Text a text wire type sends: one value for each column type that parses text.
    private static final String[] TEXT = {
            "true", "7", "1.5", "a", "1.2.3.4", "11111111-2222-3333-4444-555555555555",
            "1970-01-02T00:00:00.000Z", "2024-02-29T12:34:56.789123Z", "0x01", "sp05"
    };
    // Each wire type with the column types that take it from any connection, the
    // conversions the server had before schema-aware ingestion, and the column types
    // that take it on a connection with schema support only, as a stale wire type.
    private static final Wire[] WIRES = {
            new Wire("BOOLEAN", QwpConstants.TYPE_BOOLEAN, false, 1, (c, i) -> c.addBoolean(true),
                    "BOOLEAN, " + NUMBERS + ", STRING, VARCHAR",
                    "DATE, TIMESTAMP, TIMESTAMP_NS, SYMBOL"),
            new Wire("BYTE", QwpConstants.TYPE_BYTE, false, 1, (c, i) -> c.addByte((byte) 7),
                    NUMBERS + ", DATE, TIMESTAMP, TIMESTAMP_NS, STRING, VARCHAR, SYMBOL, " + DECIMALS,
                    "BOOLEAN"),
            new Wire("SHORT", QwpConstants.TYPE_SHORT, false, 1, (c, i) -> c.addShort((short) 7),
                    NUMBERS + ", DATE, TIMESTAMP, TIMESTAMP_NS, STRING, VARCHAR, SYMBOL, " + DECIMALS,
                    "BOOLEAN"),
            new Wire("INT", QwpConstants.TYPE_INT, true, 1, (c, i) -> c.addInt(7),
                    NUMBERS + ", DATE, TIMESTAMP, TIMESTAMP_NS, STRING, VARCHAR, SYMBOL, IPV4, " + DECIMALS,
                    "BOOLEAN"),
            new Wire("LONG", QwpConstants.TYPE_LONG, true, 1, (c, i) -> c.addLong(7),
                    NUMBERS + ", DATE, TIMESTAMP, TIMESTAMP_NS, STRING, VARCHAR, SYMBOL, " + DECIMALS,
                    "BOOLEAN"),
            new Wire("FLOAT", QwpConstants.TYPE_FLOAT, true, 1, (c, i) -> c.addFloat(7),
                    NUMBERS + ", STRING, VARCHAR, SYMBOL, " + DECIMALS,
                    "BOOLEAN, DATE, TIMESTAMP, TIMESTAMP_NS"),
            new Wire("DOUBLE", QwpConstants.TYPE_DOUBLE, true, 1, (c, i) -> c.addDouble(7),
                    NUMBERS + ", STRING, VARCHAR, SYMBOL, " + DECIMALS,
                    "BOOLEAN, DATE, TIMESTAMP, TIMESTAMP_NS"),
            new Wire("CHAR", QwpConstants.TYPE_CHAR, false, 1, (c, i) -> c.addShort((short) 'a'),
                    "STRING, VARCHAR, CHAR",
                    "SYMBOL"),
            new Wire("DATE", QwpConstants.TYPE_DATE, true, 1, (c, i) -> c.addLong(7),
                    "DATE, STRING, VARCHAR",
                    "BOOLEAN, " + NUMBERS + ", TIMESTAMP, TIMESTAMP_NS, SYMBOL"),
            new Wire("TIMESTAMP", QwpConstants.TYPE_TIMESTAMP, true, 1, (c, i) -> c.addLong(7),
                    "TIMESTAMP, TIMESTAMP_NS, STRING, VARCHAR",
                    "BOOLEAN, " + NUMBERS + ", DATE, SYMBOL"),
            new Wire("TIMESTAMP_NANOS", QwpConstants.TYPE_TIMESTAMP_NANOS, true, 1, (c, i) -> c.addLong(7),
                    "TIMESTAMP, TIMESTAMP_NS, STRING, VARCHAR",
                    "BOOLEAN, " + NUMBERS + ", DATE, SYMBOL"),
            new Wire("IPv4", QwpConstants.TYPE_IPv4, true, 1, (c, i) -> c.addIPv4(0x01020304),
                    "STRING, VARCHAR, IPV4",
                    "SYMBOL"),
            new Wire("UUID", QwpConstants.TYPE_UUID, true, 1, (c, i) -> c.addUuid(0x1111111122223333L, 0x4444555555555555L),
                    "STRING, VARCHAR, UUID",
                    "SYMBOL"),
            new Wire("LONG256", QwpConstants.TYPE_LONG256, true, 1, (c, i) -> c.addLong256(1, 2, 3, 4),
                    "STRING, VARCHAR, LONG256",
                    ""),
            new Wire("GEOHASH", QwpConstants.TYPE_GEOHASH, true, 1, (c, i) -> c.addGeoHash(0b1010_1010_1010_1010_1010L, 20),
                    "STRING, VARCHAR, GEOHASH(4c)",
                    ""),
            new Wire("DECIMAL64", QwpConstants.TYPE_DECIMAL64, true, 1, (c, i) -> c.addDecimal64(new Decimal64(15, 1)),
                    "STRING, VARCHAR, " + DECIMALS,
                    "FLOAT, DOUBLE"),
            new Wire("DECIMAL128", QwpConstants.TYPE_DECIMAL128, true, 1, (c, i) -> c.addDecimal128(Decimal128.fromLong(15, 1)),
                    "STRING, VARCHAR, " + DECIMALS,
                    "FLOAT, DOUBLE"),
            new Wire("DECIMAL256", QwpConstants.TYPE_DECIMAL256, true, 1, (c, i) -> c.addDecimal256(Decimal256.fromLong(15, 1)),
                    "STRING, VARCHAR, " + DECIMALS,
                    "FLOAT, DOUBLE"),
            new Wire("SYMBOL", QwpConstants.TYPE_SYMBOL, true, TEXT.length, (c, i) -> c.addSymbol(TEXT[i]),
                    "STRING, VARCHAR, SYMBOL",
                    "BOOLEAN, " + NUMBERS + ", DATE, TIMESTAMP, TIMESTAMP_NS, IPV4, UUID, CHAR"),
            new Wire("VARCHAR", QwpConstants.TYPE_VARCHAR, true, TEXT.length, (c, i) -> c.addString(TEXT[i]),
                    "BOOLEAN, " + NUMBERS + ", DATE, TIMESTAMP, TIMESTAMP_NS, STRING, VARCHAR, SYMBOL, UUID, CHAR, "
                            + DECIMALS + ", LONG256, GEOHASH(4c), BINARY",
                    "IPV4"),
            // The server reads a BINARY value as text wherever it parses text. The
            // bytes below are no text any column type takes, so only the NULL row
            // shows those pairs.
            new Wire("BINARY", QwpConstants.TYPE_BINARY, true, 1, (c, i) -> c.addBinary(new byte[]{1, 2}),
                    "BOOLEAN, " + NUMBERS + ", DATE, TIMESTAMP, TIMESTAMP_NS, UUID, " + DECIMALS + ", LONG256, GEOHASH(4c), BINARY",
                    ""),
            new Wire("DOUBLE_ARRAY", QwpConstants.TYPE_DOUBLE_ARRAY, true, 1, (c, i) -> c.addDoubleArray(new double[]{1.5, 2.5}),
                    "DOUBLE[]",
                    ""),
    };

    @Test
    public void testConnectionWithSchemaSupportAlsoTakesStaleWireTypes() throws Exception {
        assertWireTypeMatrix(true);
    }

    @Test
    public void testConnectionWithoutSchemaSupportTakesLegacyWireTypesOnly() throws Exception {
        assertWireTypeMatrix(false);
    }

    // Sends one row of the wire type to the table's column c, on a connection of its
    // own because the server answers a rejected frame with an error.
    private static boolean isTaken(
            int port,
            boolean isSchemaSupported,
            QwpWebSocketEncoder encoder,
            String table,
            Wire wire,
            int value,
            long key
    ) {
        try (WebSocketClient client = isSchemaSupported ? connectSchemaClient(port) : connectLegacyClient(port);
             QwpTableBuffer buffer = new QwpTableBuffer(table)) {
            buffer.getOrCreateColumn("k", QwpConstants.TYPE_LONG, true).addLong(key);
            QwpTableBuffer.ColumnBuffer column = buffer.getOrCreateColumn("c", wire.type, wire.hasNullBitmap);
            if (value < wire.valueCount) {
                wire.writer.write(column, value);
            } else {
                column.addNull();
            }
            buffer.nextRow();
            encoder.getBuffer().reset();
            int length = encoder.encode(buffer);
            client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
            return receiveResponse(client).isSuccess();
        }
    }

    // A column type takes a wire type when the server stores at least one of the wire
    // type's rows. A wire type with a null bitmap also sends a NULL row, which every
    // conversion the server has for the pair stores, whatever it makes of the values.
    private void assertWireTypeMatrix(boolean isSchemaSupported) throws Exception {
        runInContext(port -> {
            List<String> failures = new ArrayList<>();
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                for (int t = 0; t < COLUMN_TYPES.length; t++) {
                    String columnType = COLUMN_TYPES[t];
                    String table = "wire_matrix_" + t;
                    execute("CREATE TABLE " + table + " (k LONG, c " + columnType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    long storedCount = 0;
                    for (Wire wire : WIRES) {
                        boolean isTaken = false;
                        int rowCount = wire.hasNullBitmap ? wire.valueCount + 1 : wire.valueCount;
                        for (int v = 0; v < rowCount; v++) {
                            if (isTaken(port, isSchemaSupported, encoder, table, wire, v, storedCount)) {
                                isTaken = true;
                                storedCount++;
                            }
                        }
                        boolean isExpected = wire.legacyColumnTypes.contains(columnType)
                                || (isSchemaSupported && wire.staleColumnTypes.contains(columnType));
                        if (isTaken != isExpected) {
                            failures.add(wire.name + " -> " + columnType + (isTaken ? ": taken" : ": rejected"));
                        }
                    }
                    drainWalQueue();
                    // A rejected frame stores nothing and a taken frame stores its row.
                    assertQuery("SELECT count() FROM " + table)
                            .noLeakCheck().noRandomAccess().expectSize()
                            .returns("count\n" + storedCount + '\n');
                }
            }
            Assert.assertEquals(String.join("\n", failures), 0, failures.size());
        }, 65_536, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    @FunctionalInterface
    private interface ValueWriter {
        void write(QwpTableBuffer.ColumnBuffer column, int index);
    }

    private static final class Wire {
        private final boolean hasNullBitmap;
        private final List<String> legacyColumnTypes;
        private final String name;
        // Column types that take the wire type as a stale one: the old type of a
        // column that ALTER COLUMN TYPE changed.
        private final List<String> staleColumnTypes;
        private final byte type;
        private final int valueCount;
        private final ValueWriter writer;

        private Wire(
                String name,
                byte type,
                boolean hasNullBitmap,
                int valueCount,
                ValueWriter writer,
                String legacyColumnTypes,
                String staleColumnTypes
        ) {
            this.name = name;
            this.type = type;
            this.hasNullBitmap = hasNullBitmap;
            this.valueCount = valueCount;
            this.writer = writer;
            this.legacyColumnTypes = Arrays.asList(legacyColumnTypes.split(", "));
            this.staleColumnTypes = Arrays.asList(staleColumnTypes.split(", "));
        }
    }
}
