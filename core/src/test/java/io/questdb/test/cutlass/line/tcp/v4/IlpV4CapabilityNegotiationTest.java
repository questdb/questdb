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

import io.questdb.cutlass.line.tcp.v4.IlpV4CapabilityRequest;
import io.questdb.cutlass.line.tcp.v4.IlpV4CapabilityResponse;
import io.questdb.cutlass.line.tcp.v4.IlpV4Negotiator;
import io.questdb.cutlass.line.tcp.v4.IlpV4ParseException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

public class IlpV4CapabilityNegotiationTest {

    // ==================== Capability Request Tests ====================

    @Test
    public void testParseCapabilityRequest() throws IlpV4ParseException {
        byte[] buf = new byte[8];
        IlpV4CapabilityRequest.encode(buf, 0, (byte) 1, (byte) 1, FLAG_LZ4 | FLAG_GORILLA);

        IlpV4CapabilityRequest request = new IlpV4CapabilityRequest();
        request.parse(buf, 0, 8);

        Assert.assertEquals(MAGIC_CAPABILITY_REQUEST, request.getMagic());
        Assert.assertEquals(1, request.getMinVersion());
        Assert.assertEquals(1, request.getMaxVersion());
        Assert.assertTrue(request.supportsLZ4());
        Assert.assertFalse(request.supportsZstd());
        Assert.assertTrue(request.supportsGorilla());
    }

    @Test
    public void testRequestMagicBytes() throws IlpV4ParseException {
        byte[] buf = new byte[8];
        IlpV4CapabilityRequest.encode(buf, 0, (byte) 1, (byte) 1, 0);

        Assert.assertEquals('I', buf[0]);
        Assert.assertEquals('L', buf[1]);
        Assert.assertEquals('P', buf[2]);
        Assert.assertEquals('?', buf[3]);

        IlpV4CapabilityRequest request = new IlpV4CapabilityRequest();
        request.parse(buf, 0, 8);
        Assert.assertEquals(MAGIC_CAPABILITY_REQUEST, request.getMagic());
    }

    @Test
    public void testRequestMinMaxVersion() throws IlpV4ParseException {
        byte[] buf = new byte[8];
        IlpV4CapabilityRequest.encode(buf, 0, (byte) 1, (byte) 5, 0);

        IlpV4CapabilityRequest request = new IlpV4CapabilityRequest();
        request.parse(buf, 0, 8);

        Assert.assertEquals(1, request.getMinVersion());
        Assert.assertEquals(5, request.getMaxVersion());
    }

    @Test
    public void testRequestFlags() throws IlpV4ParseException {
        IlpV4CapabilityRequest request = new IlpV4CapabilityRequest();

        // Test LZ4 flag
        byte[] buf1 = new byte[8];
        IlpV4CapabilityRequest.encode(buf1, 0, (byte) 1, (byte) 1, FLAG_LZ4);
        request.parse(buf1, 0, 8);
        Assert.assertTrue(request.supportsLZ4());
        Assert.assertFalse(request.supportsZstd());
        Assert.assertFalse(request.supportsGorilla());

        // Test Zstd flag
        byte[] buf2 = new byte[8];
        IlpV4CapabilityRequest.encode(buf2, 0, (byte) 1, (byte) 1, FLAG_ZSTD);
        request.parse(buf2, 0, 8);
        Assert.assertFalse(request.supportsLZ4());
        Assert.assertTrue(request.supportsZstd());
        Assert.assertFalse(request.supportsGorilla());

        // Test Gorilla flag
        byte[] buf3 = new byte[8];
        IlpV4CapabilityRequest.encode(buf3, 0, (byte) 1, (byte) 1, FLAG_GORILLA);
        request.parse(buf3, 0, 8);
        Assert.assertFalse(request.supportsLZ4());
        Assert.assertFalse(request.supportsZstd());
        Assert.assertTrue(request.supportsGorilla());
    }

    @Test
    public void testRequestReservedBitsIgnored() throws IlpV4ParseException {
        byte[] buf = new byte[8];
        IlpV4CapabilityRequest.encode(buf, 0, (byte) 1, (byte) 1, 0xFF00); // Reserved bits set

        IlpV4CapabilityRequest request = new IlpV4CapabilityRequest();
        request.parse(buf, 0, 8);

        // Should parse without error
        Assert.assertEquals(0xFF00, request.getFlags());
    }

    @Test
    public void testRequestTooShort() {
        IlpV4CapabilityRequest request = new IlpV4CapabilityRequest();

        for (int len = 0; len < 8; len++) {
            byte[] buf = new byte[len];
            try {
                request.parse(buf, 0, len);
                Assert.fail("Expected exception for length " + len);
            } catch (IlpV4ParseException e) {
                Assert.assertEquals(IlpV4ParseException.ErrorCode.HEADER_TOO_SHORT, e.getErrorCode());
            }
        }
    }

    // ==================== Capability Response Tests ====================

    @Test
    public void testGenerateCapabilityResponse() throws IlpV4ParseException {
        byte[] buf = new byte[8];
        IlpV4CapabilityResponse.encode(buf, 0, (byte) 1, FLAG_LZ4 | FLAG_GORILLA);

        IlpV4CapabilityResponse response = new IlpV4CapabilityResponse();
        response.parse(buf, 0, 8);

        Assert.assertTrue(response.isSuccess());
        Assert.assertFalse(response.isFallback());
        Assert.assertEquals(1, response.getNegotiatedVersion());
        Assert.assertTrue(response.supportsLZ4());
        Assert.assertFalse(response.supportsZstd());
        Assert.assertTrue(response.supportsGorilla());
    }

    @Test
    public void testResponseMagicBytes() throws IlpV4ParseException {
        byte[] buf = new byte[8];
        IlpV4CapabilityResponse.encode(buf, 0, (byte) 1, 0);

        Assert.assertEquals('I', buf[0]);
        Assert.assertEquals('L', buf[1]);
        Assert.assertEquals('P', buf[2]);
        Assert.assertEquals('!', buf[3]);

        IlpV4CapabilityResponse response = new IlpV4CapabilityResponse();
        response.parse(buf, 0, 8);
        Assert.assertEquals(MAGIC_CAPABILITY_RESPONSE, response.getMagic());
    }

    @Test
    public void testFallbackResponse() throws IlpV4ParseException {
        byte[] buf = new byte[8];
        IlpV4CapabilityResponse.encodeFallback(buf, 0);

        Assert.assertEquals('I', buf[0]);
        Assert.assertEquals('L', buf[1]);
        Assert.assertEquals('P', buf[2]);
        Assert.assertEquals('0', buf[3]);

        IlpV4CapabilityResponse response = new IlpV4CapabilityResponse();
        response.parse(buf, 0, 8);

        Assert.assertFalse(response.isSuccess());
        Assert.assertTrue(response.isFallback());
    }

    // ==================== Negotiation Tests ====================

    @Test
    public void testVersionNegotiation_ClientAndServerSame() {
        IlpV4Negotiator negotiator = new IlpV4Negotiator(VERSION_1, VERSION_1, FLAG_LZ4);
        IlpV4Negotiator.NegotiationResult result = negotiator.negotiate(VERSION_1, VERSION_1, FLAG_LZ4);

        Assert.assertTrue(result.success);
        Assert.assertEquals(VERSION_1, result.negotiatedVersion);
    }

    @Test
    public void testVersionNegotiation_ClientHigherThanServer() {
        // Client supports 1-5, server only supports 1
        IlpV4Negotiator negotiator = new IlpV4Negotiator(VERSION_1, VERSION_1, FLAG_LZ4);
        IlpV4Negotiator.NegotiationResult result = negotiator.negotiate((byte) 1, (byte) 5, FLAG_LZ4);

        Assert.assertTrue(result.success);
        Assert.assertEquals(VERSION_1, result.negotiatedVersion); // Negotiates to server's max
    }

    @Test
    public void testVersionNegotiation_ServerHigherThanClient() {
        // Client supports 1, server supports 1-5
        IlpV4Negotiator negotiator = new IlpV4Negotiator(VERSION_1, (byte) 5, FLAG_LZ4);
        IlpV4Negotiator.NegotiationResult result = negotiator.negotiate(VERSION_1, VERSION_1, FLAG_LZ4);

        Assert.assertTrue(result.success);
        Assert.assertEquals(VERSION_1, result.negotiatedVersion); // Negotiates to client's max
    }

    @Test
    public void testVersionNegotiation_NoOverlap() {
        // Client supports 1-2, server supports 5-10
        IlpV4Negotiator negotiator = new IlpV4Negotiator((byte) 5, (byte) 10, FLAG_LZ4);
        IlpV4Negotiator.NegotiationResult result = negotiator.negotiate((byte) 1, (byte) 2, FLAG_LZ4);

        Assert.assertFalse(result.success);
    }

    @Test
    public void testFlagNegotiation_BothSupportLZ4() {
        IlpV4Negotiator negotiator = new IlpV4Negotiator(VERSION_1, VERSION_1, FLAG_LZ4 | FLAG_ZSTD);
        IlpV4Negotiator.NegotiationResult result = negotiator.negotiate(VERSION_1, VERSION_1, FLAG_LZ4);

        Assert.assertTrue(result.success);
        Assert.assertEquals(FLAG_LZ4, result.negotiatedFlags);
    }

    @Test
    public void testFlagNegotiation_OnlyClientSupportsZstd() {
        // Server doesn't support Zstd
        IlpV4Negotiator negotiator = new IlpV4Negotiator(VERSION_1, VERSION_1, FLAG_LZ4);
        IlpV4Negotiator.NegotiationResult result = negotiator.negotiate(VERSION_1, VERSION_1, FLAG_LZ4 | FLAG_ZSTD);

        Assert.assertTrue(result.success);
        Assert.assertEquals(FLAG_LZ4, result.negotiatedFlags); // Only LZ4 is common
    }

    @Test
    public void testFlagNegotiation_IntersectionOfFlags() {
        // Server supports LZ4, Zstd, Gorilla
        IlpV4Negotiator negotiator = new IlpV4Negotiator(VERSION_1, VERSION_1, FLAG_LZ4 | FLAG_ZSTD | FLAG_GORILLA);
        // Client supports LZ4, Gorilla only
        IlpV4Negotiator.NegotiationResult result = negotiator.negotiate(VERSION_1, VERSION_1, FLAG_LZ4 | FLAG_GORILLA);

        Assert.assertTrue(result.success);
        Assert.assertEquals(FLAG_LZ4 | FLAG_GORILLA, result.negotiatedFlags);
    }

    @Test
    public void testFlagNegotiation_NoCommonFlags() {
        // Server supports LZ4 only
        IlpV4Negotiator negotiator = new IlpV4Negotiator(VERSION_1, VERSION_1, FLAG_LZ4);
        // Client supports Zstd only
        IlpV4Negotiator.NegotiationResult result = negotiator.negotiate(VERSION_1, VERSION_1, FLAG_ZSTD);

        // Version negotiation succeeds, but no common flags
        Assert.assertTrue(result.success);
        Assert.assertEquals(0, result.negotiatedFlags);
    }

    // ==================== Write Response Tests ====================

    @Test
    public void testWriteSuccessResponse() throws IlpV4ParseException {
        IlpV4Negotiator.NegotiationResult result = IlpV4Negotiator.NegotiationResult.success(VERSION_1, FLAG_GORILLA);

        byte[] buf = new byte[8];
        IlpV4Negotiator.writeResponse(buf, 0, result);

        IlpV4CapabilityResponse response = new IlpV4CapabilityResponse();
        response.parse(buf, 0, 8);

        Assert.assertTrue(response.isSuccess());
        Assert.assertEquals(VERSION_1, response.getNegotiatedVersion());
        Assert.assertTrue(response.supportsGorilla());
    }

    @Test
    public void testWriteFallbackResponse() throws IlpV4ParseException {
        IlpV4Negotiator.NegotiationResult result = IlpV4Negotiator.NegotiationResult.failure();

        byte[] buf = new byte[8];
        IlpV4Negotiator.writeResponse(buf, 0, result);

        IlpV4CapabilityResponse response = new IlpV4CapabilityResponse();
        response.parse(buf, 0, 8);

        Assert.assertTrue(response.isFallback());
    }

    // ==================== Direct Memory Tests ====================

    @Test
    public void testParseFromDirectMemory() throws IlpV4ParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4CapabilityRequest.encode(addr, (byte) 1, (byte) 3, FLAG_LZ4 | FLAG_ZSTD);

            IlpV4CapabilityRequest request = new IlpV4CapabilityRequest();
            request.parse(addr, 8);

            Assert.assertEquals(1, request.getMinVersion());
            Assert.assertEquals(3, request.getMaxVersion());
            Assert.assertTrue(request.supportsLZ4());
            Assert.assertTrue(request.supportsZstd());
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteResponseToDirectMemory() throws IlpV4ParseException {
        long addr = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            IlpV4Negotiator.NegotiationResult result = IlpV4Negotiator.NegotiationResult.success(VERSION_1, FLAG_GORILLA);
            IlpV4Negotiator.writeResponse(addr, result);

            IlpV4CapabilityResponse response = new IlpV4CapabilityResponse();
            response.parse(addr, 8);

            Assert.assertTrue(response.isSuccess());
            Assert.assertTrue(response.supportsGorilla());
        } finally {
            Unsafe.free(addr, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Round-Trip Tests ====================

    @Test
    public void testFullNegotiationRoundTrip() throws IlpV4ParseException {
        // 1. Client creates request
        byte[] requestBuf = new byte[8];
        IlpV4CapabilityRequest.encode(requestBuf, 0, (byte) 1, (byte) 2, FLAG_LZ4 | FLAG_GORILLA);

        // 2. Server parses request
        IlpV4CapabilityRequest request = new IlpV4CapabilityRequest();
        request.parse(requestBuf, 0, 8);

        // 3. Server negotiates
        IlpV4Negotiator negotiator = new IlpV4Negotiator(VERSION_1, VERSION_1, FLAG_LZ4 | FLAG_ZSTD | FLAG_GORILLA);
        IlpV4Negotiator.NegotiationResult result = negotiator.negotiate(request);

        // 4. Server writes response
        byte[] responseBuf = new byte[8];
        IlpV4Negotiator.writeResponse(responseBuf, 0, result);

        // 5. Client parses response
        IlpV4CapabilityResponse response = new IlpV4CapabilityResponse();
        response.parse(responseBuf, 0, 8);

        // 6. Verify
        Assert.assertTrue(response.isSuccess());
        Assert.assertEquals(VERSION_1, response.getNegotiatedVersion());
        // Common flags: LZ4 and Gorilla (both supported by client and server)
        Assert.assertTrue(response.supportsLZ4());
        Assert.assertTrue(response.supportsGorilla());
        Assert.assertFalse(response.supportsZstd()); // Client didn't request Zstd
    }

    @Test
    public void testToString() throws IlpV4ParseException {
        byte[] buf = new byte[8];
        IlpV4CapabilityRequest.encode(buf, 0, (byte) 1, (byte) 3, FLAG_LZ4 | FLAG_GORILLA);

        IlpV4CapabilityRequest request = new IlpV4CapabilityRequest();
        request.parse(buf, 0, 8);

        String str = request.toString();
        Assert.assertTrue(str.contains("minVersion=1"));
        Assert.assertTrue(str.contains("maxVersion=3"));
        Assert.assertTrue(str.contains("[LZ4]"));
        Assert.assertTrue(str.contains("[Gorilla]"));
    }
}
