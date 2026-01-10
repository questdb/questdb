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

package io.questdb.test.cutlass.line.tcp.v4;

import io.questdb.cutlass.line.tcp.v4.*;
import io.questdb.std.ObjList;
import org.junit.Test;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;
import static org.junit.Assert.*;

/**
 * End-to-end integration tests for ILP v4 protocol components.
 * <p>
 * These tests verify the complete pipeline works together correctly.
 */
public class IlpV4EndToEndTest {

    // ==================== Component Integration Tests ====================

    @Test
    public void testHandshakeToResponseFlow() throws Exception {
        IlpV4ReceiverConfiguration config = new DefaultTestConfig();
        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            // 1. Perform handshake
            performHandshake(ctx, FLAG_GORILLA);

            // 2. Verify handshake result
            assertTrue(ctx.isHandshakeComplete());
            assertTrue(ctx.isGorillaEnabled());
            assertEquals((short) FLAG_GORILLA, ctx.getNegotiatedCapabilities());

            // 3. Create and encode a response
            IlpV4Response response = IlpV4Response.ok();
            byte[] encoded = IlpV4ResponseEncoder.encode(response);

            // 4. Verify response can be decoded
            IlpV4Response decoded = IlpV4ResponseEncoder.decode(encoded, 0, encoded.length);
            assertEquals(IlpV4StatusCode.OK, decoded.getStatusCode());
        }
    }

    @Test
    public void testHandshakeWithNoGorilla() {
        IlpV4ReceiverConfiguration config = new IlpV4ReceiverConfiguration() {
            @Override
            public boolean isGorillaSupported() {
                return false;
            }
        };

        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            performHandshake(ctx, FLAG_GORILLA); // Client requests Gorilla

            assertTrue(ctx.isHandshakeComplete());
            assertFalse(ctx.isGorillaEnabled()); // But server doesn't support it
        }
    }

    @Test
    public void testSchemaCacheIntegration() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache(100);

        // Create schemas for multiple tables
        for (int i = 0; i < 10; i++) {
            IlpV4ColumnDef[] cols = new IlpV4ColumnDef[]{
                    new IlpV4ColumnDef("id", TYPE_LONG, false),
                    new IlpV4ColumnDef("value_" + i, TYPE_DOUBLE, true)
            };
            IlpV4Schema schema = IlpV4Schema.create(cols);
            cache.put("table_" + i, schema);
        }

        // Verify all schemas can be retrieved
        for (int i = 0; i < 10; i++) {
            String tableName = "table_" + i;
            IlpV4ColumnDef[] cols = new IlpV4ColumnDef[]{
                    new IlpV4ColumnDef("id", TYPE_LONG, false),
                    new IlpV4ColumnDef("value_" + i, TYPE_DOUBLE, true)
            };
            IlpV4Schema expected = IlpV4Schema.create(cols);

            IlpV4Schema retrieved = cache.get(tableName, expected.getSchemaHash());
            assertNotNull("Schema for " + tableName + " should exist", retrieved);
            assertEquals(2, retrieved.getColumnCount());
        }
    }

    @Test
    public void testResponseRoundTripAllTypes() throws Exception {
        // Test all response types round-trip correctly
        IlpV4Response[] responses = {
                IlpV4Response.ok(),
                IlpV4Response.schemaRequired("test_table"),
                IlpV4Response.schemaMismatch("table", "column"),
                IlpV4Response.tableNotFound("missing"),
                IlpV4Response.parseError("invalid message"),
                IlpV4Response.internalError("server error"),
                IlpV4Response.overloaded()
        };

        for (IlpV4Response original : responses) {
            byte[] encoded = IlpV4ResponseEncoder.encode(original);
            IlpV4Response decoded = IlpV4ResponseEncoder.decode(encoded, 0, encoded.length);

            assertEquals("Status code mismatch for " + IlpV4StatusCode.name(original.getStatusCode()),
                    original.getStatusCode(), decoded.getStatusCode());

            if (original.getErrorMessage() != null) {
                assertEquals(original.getErrorMessage(), decoded.getErrorMessage());
            }
        }
    }

    @Test
    public void testPartialResponseRoundTrip() throws Exception {
        ObjList<IlpV4Response.TableError> errors = new ObjList<>();
        errors.add(new IlpV4Response.TableError(0, IlpV4StatusCode.SCHEMA_MISMATCH, "type mismatch on col1"));
        errors.add(new IlpV4Response.TableError(3, IlpV4StatusCode.TABLE_NOT_FOUND, "table does not exist"));
        errors.add(new IlpV4Response.TableError(7, IlpV4StatusCode.PARSE_ERROR, "invalid data format"));

        IlpV4Response original = IlpV4Response.partial(errors);
        byte[] encoded = IlpV4ResponseEncoder.encode(original);
        IlpV4Response decoded = IlpV4ResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(IlpV4StatusCode.PARTIAL, decoded.getStatusCode());
        assertEquals(3, decoded.getTableErrors().size());

        for (int i = 0; i < 3; i++) {
            assertEquals(errors.get(i).getTableIndex(), decoded.getTableErrors().get(i).getTableIndex());
            assertEquals(errors.get(i).getErrorCode(), decoded.getTableErrors().get(i).getErrorCode());
            assertEquals(errors.get(i).getErrorMessage(), decoded.getTableErrors().get(i).getErrorMessage());
        }
    }

    @Test
    public void testCapabilityNegotiationMatrix() {
        // Test various capability combinations
        short[][] testCases = {
                {FLAG_GORILLA, FLAG_GORILLA, FLAG_GORILLA},           // Both support Gorilla
                {FLAG_GORILLA, 0, 0},                                  // Server doesn't support
                {0, FLAG_GORILLA, 0},                                  // Client doesn't support
                {FLAG_LZ4 | FLAG_GORILLA, FLAG_GORILLA, FLAG_GORILLA}, // Client has more, intersect
                {FLAG_GORILLA, FLAG_LZ4 | FLAG_GORILLA, FLAG_GORILLA}, // Server has more, intersect
                {FLAG_LZ4, FLAG_ZSTD, 0},                              // No overlap
        };

        for (short[] testCase : testCases) {
            short clientCaps = testCase[0];
            short serverCaps = testCase[1];
            short expected = testCase[2];

            IlpV4Negotiator negotiator = new IlpV4Negotiator(VERSION_1, VERSION_1, serverCaps);

            // Use the overloaded negotiate method that takes values directly
            IlpV4Negotiator.NegotiationResult result = negotiator.negotiate(VERSION_1, VERSION_1, clientCaps);

            assertTrue("Negotiation should succeed", result.success);
            assertEquals("Capability mismatch for client=" + clientCaps + " server=" + serverCaps,
                    expected, (short) result.negotiatedFlags);
        }
    }

    @Test
    public void testMessageHeaderValidation() {
        // Test valid header
        IlpV4MessageHeader header = new IlpV4MessageHeader();
        byte[] valid = createValidHeader(1, 100, false);

        try {
            header.parse(valid, 0, valid.length);
            assertEquals(1, header.getTableCount());
            assertEquals(100, header.getPayloadLength());
        } catch (IlpV4ParseException e) {
            fail("Should not throw for valid header: " + e.getMessage());
        }
    }

    @Test
    public void testMessageHeaderInvalidMagic() {
        IlpV4MessageHeader header = new IlpV4MessageHeader();
        byte[] invalid = createValidHeader(1, 100, false);
        invalid[0] = 'X'; // Corrupt magic

        try {
            header.parse(invalid, 0, invalid.length);
            fail("Should throw for invalid magic");
        } catch (IlpV4ParseException e) {
            assertEquals(IlpV4ParseException.ErrorCode.INVALID_MAGIC, e.getErrorCode());
        }
    }

    @Test
    public void testMessageHeaderUnsupportedVersion() {
        IlpV4MessageHeader header = new IlpV4MessageHeader();
        byte[] invalid = createValidHeader(1, 100, false);
        invalid[4] = 99; // Unsupported version

        try {
            header.parse(invalid, 0, invalid.length);
            fail("Should throw for unsupported version");
        } catch (IlpV4ParseException e) {
            assertEquals(IlpV4ParseException.ErrorCode.UNSUPPORTED_VERSION, e.getErrorCode());
        }
    }

    // ==================== Type Mapping Tests ====================

    @Test
    public void testTypeMappingConsistency() {
        // Verify type mapping is consistent in both directions for all supported types
        byte[] ilpTypes = {
                TYPE_BOOLEAN, TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG,
                TYPE_FLOAT, TYPE_DOUBLE, TYPE_STRING, TYPE_VARCHAR, TYPE_SYMBOL,
                TYPE_TIMESTAMP, TYPE_DATE, TYPE_UUID, TYPE_LONG256
        };

        for (byte ilpType : ilpTypes) {
            int questdbType = io.questdb.cutlass.line.tcp.IlpV4WalAppender.mapIlpV4TypeToQuestDB(ilpType);
            byte mappedBack = io.questdb.cutlass.line.tcp.IlpV4WalAppender.mapQuestDBTypeToIlpV4(questdbType);
            assertEquals("Round-trip failed for type " + ilpType, ilpType, mappedBack);
        }
    }

    // ==================== Error Response Tests ====================

    @Test
    public void testRetriableErrors() {
        assertTrue(IlpV4StatusCode.isRetriable(IlpV4StatusCode.SCHEMA_REQUIRED));
        assertTrue(IlpV4StatusCode.isRetriable(IlpV4StatusCode.OVERLOADED));

        assertFalse(IlpV4StatusCode.isRetriable(IlpV4StatusCode.OK));
        assertFalse(IlpV4StatusCode.isRetriable(IlpV4StatusCode.PARTIAL));
        assertFalse(IlpV4StatusCode.isRetriable(IlpV4StatusCode.SCHEMA_MISMATCH));
        assertFalse(IlpV4StatusCode.isRetriable(IlpV4StatusCode.TABLE_NOT_FOUND));
        assertFalse(IlpV4StatusCode.isRetriable(IlpV4StatusCode.PARSE_ERROR));
        assertFalse(IlpV4StatusCode.isRetriable(IlpV4StatusCode.INTERNAL_ERROR));
    }

    @Test
    public void testSuccessStatuses() {
        assertTrue(IlpV4StatusCode.isSuccess(IlpV4StatusCode.OK));
        assertTrue(IlpV4StatusCode.isSuccess(IlpV4StatusCode.PARTIAL));

        assertFalse(IlpV4StatusCode.isSuccess(IlpV4StatusCode.SCHEMA_REQUIRED));
        assertFalse(IlpV4StatusCode.isSuccess(IlpV4StatusCode.SCHEMA_MISMATCH));
        assertFalse(IlpV4StatusCode.isSuccess(IlpV4StatusCode.TABLE_NOT_FOUND));
        assertFalse(IlpV4StatusCode.isSuccess(IlpV4StatusCode.PARSE_ERROR));
        assertFalse(IlpV4StatusCode.isSuccess(IlpV4StatusCode.INTERNAL_ERROR));
        assertFalse(IlpV4StatusCode.isSuccess(IlpV4StatusCode.OVERLOADED));
    }

    // ==================== Connection Context State Tests ====================

    @Test
    public void testConnectionContextStateTransitions() {
        IlpV4ReceiverConfiguration config = new DefaultTestConfig();
        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            // Initial state
            assertEquals(IlpV4ConnectionContext.STATE_HANDSHAKE_WAIT_REQUEST, ctx.getState());

            // After handshake request received
            long recvAddr = ctx.getRecvAddress();
            IlpV4CapabilityRequest.encode(recvAddr, VERSION_1, VERSION_1, FLAG_GORILLA);
            ctx.onDataReceived(CAPABILITY_REQUEST_SIZE);
            IlpV4ConnectionContext.ProcessResult result = ctx.process();

            assertEquals(IlpV4ConnectionContext.ProcessResult.NEEDS_WRITE, result);
            assertEquals(IlpV4ConnectionContext.STATE_HANDSHAKE_SEND_RESPONSE, ctx.getState());

            // After response sent
            ctx.onDataSent(ctx.getSendRemaining());
            assertEquals(IlpV4ConnectionContext.STATE_RECEIVING, ctx.getState());
        }
    }

    @Test
    public void testConnectionContextStatistics() {
        IlpV4ReceiverConfiguration config = new DefaultTestConfig();
        try (IlpV4ConnectionContext ctx = new IlpV4ConnectionContext(config)) {
            assertEquals(0, ctx.getBytesReceived());
            assertEquals(0, ctx.getBytesSent());

            // Receive handshake request
            ctx.onDataReceived(CAPABILITY_REQUEST_SIZE);
            assertEquals(CAPABILITY_REQUEST_SIZE, ctx.getBytesReceived());

            // Encode request
            long recvAddr = ctx.getRecvAddress() - CAPABILITY_REQUEST_SIZE;
            IlpV4CapabilityRequest.encode(recvAddr, VERSION_1, VERSION_1, 0);

            ctx.process();

            // Send response
            int sendLen = ctx.getSendRemaining();
            ctx.onDataSent(sendLen);
            assertEquals(CAPABILITY_RESPONSE_SIZE, ctx.getBytesSent());
        }
    }

    // ==================== Helper Methods ====================

    private void performHandshake(IlpV4ConnectionContext ctx, int clientFlags) {
        long recvAddr = ctx.getRecvAddress();
        IlpV4CapabilityRequest.encode(recvAddr, VERSION_1, VERSION_1, clientFlags);
        ctx.onDataReceived(CAPABILITY_REQUEST_SIZE);
        ctx.process();
        ctx.onDataSent(ctx.getSendRemaining());
    }

    private byte[] createValidHeader(int tableCount, int payloadLength, boolean gorilla) {
        byte[] header = new byte[HEADER_SIZE];
        header[0] = 'I';
        header[1] = 'L';
        header[2] = 'P';
        header[3] = '4';
        header[4] = VERSION_1;
        header[5] = gorilla ? FLAG_GORILLA : 0;
        header[6] = (byte) (tableCount & 0xFF);
        header[7] = (byte) ((tableCount >> 8) & 0xFF);
        header[8] = (byte) (payloadLength & 0xFF);
        header[9] = (byte) ((payloadLength >> 8) & 0xFF);
        header[10] = (byte) ((payloadLength >> 16) & 0xFF);
        header[11] = (byte) ((payloadLength >> 24) & 0xFF);
        return header;
    }

    private static class DefaultTestConfig implements IlpV4ReceiverConfiguration {
        // Use all defaults
    }
}
