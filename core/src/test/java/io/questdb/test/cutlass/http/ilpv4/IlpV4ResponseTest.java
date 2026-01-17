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

package io.questdb.test.cutlass.http.ilpv4;

import io.questdb.cutlass.ilpv4.protocol.*;
import io.questdb.std.ObjList;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for ILP v4 response generation and parsing.
 */
public class IlpV4ResponseTest {

    // ==================== Status Code Tests ====================

    @Test
    public void testStatusCodeName() {
        assertEquals("OK", IlpV4StatusCode.name(IlpV4StatusCode.OK));
        assertEquals("PARTIAL", IlpV4StatusCode.name(IlpV4StatusCode.PARTIAL));
        assertEquals("SCHEMA_REQUIRED", IlpV4StatusCode.name(IlpV4StatusCode.SCHEMA_REQUIRED));
        assertEquals("SCHEMA_MISMATCH", IlpV4StatusCode.name(IlpV4StatusCode.SCHEMA_MISMATCH));
        assertEquals("TABLE_NOT_FOUND", IlpV4StatusCode.name(IlpV4StatusCode.TABLE_NOT_FOUND));
        assertEquals("PARSE_ERROR", IlpV4StatusCode.name(IlpV4StatusCode.PARSE_ERROR));
        assertEquals("INTERNAL_ERROR", IlpV4StatusCode.name(IlpV4StatusCode.INTERNAL_ERROR));
        assertEquals("OVERLOADED", IlpV4StatusCode.name(IlpV4StatusCode.OVERLOADED));
        assertEquals("UNKNOWN(255)", IlpV4StatusCode.name((byte) 0xFF));
    }

    @Test
    public void testStatusCodeIsRetriable() {
        assertFalse(IlpV4StatusCode.isRetriable(IlpV4StatusCode.OK));
        assertFalse(IlpV4StatusCode.isRetriable(IlpV4StatusCode.PARTIAL));
        assertTrue(IlpV4StatusCode.isRetriable(IlpV4StatusCode.SCHEMA_REQUIRED));
        assertFalse(IlpV4StatusCode.isRetriable(IlpV4StatusCode.SCHEMA_MISMATCH));
        assertFalse(IlpV4StatusCode.isRetriable(IlpV4StatusCode.TABLE_NOT_FOUND));
        assertFalse(IlpV4StatusCode.isRetriable(IlpV4StatusCode.PARSE_ERROR));
        assertFalse(IlpV4StatusCode.isRetriable(IlpV4StatusCode.INTERNAL_ERROR));
        assertTrue(IlpV4StatusCode.isRetriable(IlpV4StatusCode.OVERLOADED));
    }

    @Test
    public void testStatusCodeIsSuccess() {
        assertTrue(IlpV4StatusCode.isSuccess(IlpV4StatusCode.OK));
        assertTrue(IlpV4StatusCode.isSuccess(IlpV4StatusCode.PARTIAL));
        assertFalse(IlpV4StatusCode.isSuccess(IlpV4StatusCode.SCHEMA_REQUIRED));
        assertFalse(IlpV4StatusCode.isSuccess(IlpV4StatusCode.INTERNAL_ERROR));
    }

    // ==================== Response Factory Tests ====================

    @Test
    public void testGenerateOkResponse() {
        IlpV4Response response = IlpV4Response.ok();

        assertEquals(IlpV4StatusCode.OK, response.getStatusCode());
        assertTrue(response.isSuccess());
        assertFalse(response.isRetriable());
        assertNull(response.getErrorMessage());
        assertNull(response.getTableErrors());
    }

    @Test
    public void testGenerateSchemaRequiredResponse() {
        IlpV4Response response = IlpV4Response.schemaRequired("trades");

        assertEquals(IlpV4StatusCode.SCHEMA_REQUIRED, response.getStatusCode());
        assertFalse(response.isSuccess());
        assertTrue(response.isRetriable());
        assertTrue(response.getErrorMessage().contains("trades"));
    }

    @Test
    public void testGenerateSchemaMismatchResponse() {
        IlpV4Response response = IlpV4Response.schemaMismatch("trades", "price");

        assertEquals(IlpV4StatusCode.SCHEMA_MISMATCH, response.getStatusCode());
        assertFalse(response.isSuccess());
        assertFalse(response.isRetriable());
        assertTrue(response.getErrorMessage().contains("trades"));
        assertTrue(response.getErrorMessage().contains("price"));
    }

    @Test
    public void testGenerateTableNotFoundResponse() {
        IlpV4Response response = IlpV4Response.tableNotFound("missing_table");

        assertEquals(IlpV4StatusCode.TABLE_NOT_FOUND, response.getStatusCode());
        assertFalse(response.isSuccess());
        assertTrue(response.getErrorMessage().contains("missing_table"));
    }

    @Test
    public void testGenerateParseErrorResponse() {
        IlpV4Response response = IlpV4Response.parseError("invalid varint at offset 42");

        assertEquals(IlpV4StatusCode.PARSE_ERROR, response.getStatusCode());
        assertFalse(response.isSuccess());
        assertEquals("invalid varint at offset 42", response.getErrorMessage());
    }

    @Test
    public void testGenerateInternalErrorResponse() {
        IlpV4Response response = IlpV4Response.internalError("out of memory");

        assertEquals(IlpV4StatusCode.INTERNAL_ERROR, response.getStatusCode());
        assertFalse(response.isSuccess());
        assertEquals("out of memory", response.getErrorMessage());
    }

    @Test
    public void testGenerateOverloadedResponse() {
        IlpV4Response response = IlpV4Response.overloaded();

        assertEquals(IlpV4StatusCode.OVERLOADED, response.getStatusCode());
        assertFalse(response.isSuccess());
        assertTrue(response.isRetriable());
        assertNotNull(response.getErrorMessage());
    }

    @Test
    public void testGeneratePartialResponse() {
        ObjList<IlpV4Response.TableError> errors = new ObjList<>();
        errors.add(new IlpV4Response.TableError(0, IlpV4StatusCode.SCHEMA_MISMATCH, "type mismatch"));
        errors.add(new IlpV4Response.TableError(2, IlpV4StatusCode.TABLE_NOT_FOUND, "table not found"));

        IlpV4Response response = IlpV4Response.partial(errors);

        assertEquals(IlpV4StatusCode.PARTIAL, response.getStatusCode());
        assertTrue(response.isSuccess()); // Partial is considered success
        assertNotNull(response.getTableErrors());
        assertEquals(2, response.getTableErrors().size());
    }

    @Test
    public void testPartialResponseMultipleTables() {
        ObjList<IlpV4Response.TableError> errors = new ObjList<>();
        errors.add(new IlpV4Response.TableError(0, IlpV4StatusCode.SCHEMA_MISMATCH, "error in table 0"));
        errors.add(new IlpV4Response.TableError(3, IlpV4StatusCode.TABLE_NOT_FOUND, "error in table 3"));
        errors.add(new IlpV4Response.TableError(7, IlpV4StatusCode.PARSE_ERROR, "error in table 7"));

        IlpV4Response response = IlpV4Response.partial(errors);

        ObjList<IlpV4Response.TableError> tableErrors = response.getTableErrors();
        assertEquals(3, tableErrors.size());

        assertEquals(0, tableErrors.get(0).getTableIndex());
        assertEquals(IlpV4StatusCode.SCHEMA_MISMATCH, tableErrors.get(0).getErrorCode());

        assertEquals(3, tableErrors.get(1).getTableIndex());
        assertEquals(IlpV4StatusCode.TABLE_NOT_FOUND, tableErrors.get(1).getErrorCode());

        assertEquals(7, tableErrors.get(2).getTableIndex());
        assertEquals(IlpV4StatusCode.PARSE_ERROR, tableErrors.get(2).getErrorCode());
    }

    @Test
    public void testPartialResponseErrorMessages() {
        IlpV4Response.TableError error = new IlpV4Response.TableError(
                5, IlpV4StatusCode.INTERNAL_ERROR, "detailed error message here");

        assertEquals(5, error.getTableIndex());
        assertEquals(IlpV4StatusCode.INTERNAL_ERROR, error.getErrorCode());
        assertEquals("detailed error message here", error.getErrorMessage());

        String str = error.toString();
        assertTrue(str.contains("5"));
        assertTrue(str.contains("INTERNAL_ERROR"));
        assertTrue(str.contains("detailed error message here"));
    }

    // ==================== Encoder Tests ====================

    @Test
    public void testResponseSerializationOk() throws IlpV4ParseException {
        IlpV4Response response = IlpV4Response.ok();
        byte[] encoded = IlpV4ResponseEncoder.encode(response);

        assertEquals(1, encoded.length);
        assertEquals(IlpV4StatusCode.OK, encoded[0]);
    }

    @Test
    public void testResponseSerializationError() throws IlpV4ParseException {
        IlpV4Response response = IlpV4Response.parseError("bad data");
        byte[] encoded = IlpV4ResponseEncoder.encode(response);

        // First byte is status code
        assertEquals(IlpV4StatusCode.PARSE_ERROR, encoded[0]);

        // Should be: status + varint(8) + "bad data"
        assertEquals(1 + 1 + 8, encoded.length);
    }

    @Test
    public void testResponseSerializationPartial() throws IlpV4ParseException {
        ObjList<IlpV4Response.TableError> errors = new ObjList<>();
        errors.add(new IlpV4Response.TableError(0, IlpV4StatusCode.SCHEMA_MISMATCH, "type error"));

        IlpV4Response response = IlpV4Response.partial(errors);
        byte[] encoded = IlpV4ResponseEncoder.encode(response);

        // First byte is PARTIAL
        assertEquals(IlpV4StatusCode.PARTIAL, encoded[0]);

        // Should have: status + errorCount + (tableIndex + errorCode + msgLen + msg)
        assertTrue(encoded.length > 1);
    }

    // ==================== Round-trip Tests ====================

    @Test
    public void testResponseRoundTripOk() throws IlpV4ParseException {
        IlpV4Response original = IlpV4Response.ok();
        byte[] encoded = IlpV4ResponseEncoder.encode(original);
        IlpV4Response decoded = IlpV4ResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(IlpV4StatusCode.OK, decoded.getStatusCode());
        assertTrue(decoded.isSuccess());
    }

    @Test
    public void testResponseRoundTripError() throws IlpV4ParseException {
        IlpV4Response original = IlpV4Response.internalError("something went wrong");
        byte[] encoded = IlpV4ResponseEncoder.encode(original);
        IlpV4Response decoded = IlpV4ResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(IlpV4StatusCode.INTERNAL_ERROR, decoded.getStatusCode());
        assertEquals("something went wrong", decoded.getErrorMessage());
    }

    @Test
    public void testResponseRoundTripPartial() throws IlpV4ParseException {
        ObjList<IlpV4Response.TableError> errors = new ObjList<>();
        errors.add(new IlpV4Response.TableError(1, IlpV4StatusCode.TABLE_NOT_FOUND, "table1 missing"));
        errors.add(new IlpV4Response.TableError(5, IlpV4StatusCode.SCHEMA_MISMATCH, "column x wrong type"));

        IlpV4Response original = IlpV4Response.partial(errors);
        byte[] encoded = IlpV4ResponseEncoder.encode(original);
        IlpV4Response decoded = IlpV4ResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(IlpV4StatusCode.PARTIAL, decoded.getStatusCode());
        assertNotNull(decoded.getTableErrors());
        assertEquals(2, decoded.getTableErrors().size());

        assertEquals(1, decoded.getTableErrors().get(0).getTableIndex());
        assertEquals(IlpV4StatusCode.TABLE_NOT_FOUND, decoded.getTableErrors().get(0).getErrorCode());
        assertEquals("table1 missing", decoded.getTableErrors().get(0).getErrorMessage());

        assertEquals(5, decoded.getTableErrors().get(1).getTableIndex());
        assertEquals(IlpV4StatusCode.SCHEMA_MISMATCH, decoded.getTableErrors().get(1).getErrorCode());
        assertEquals("column x wrong type", decoded.getTableErrors().get(1).getErrorMessage());
    }

    @Test
    public void testResponseRoundTripSchemaRequired() throws IlpV4ParseException {
        IlpV4Response original = IlpV4Response.schemaRequired("my_table");
        byte[] encoded = IlpV4ResponseEncoder.encode(original);
        IlpV4Response decoded = IlpV4ResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(IlpV4StatusCode.SCHEMA_REQUIRED, decoded.getStatusCode());
        assertTrue(decoded.getErrorMessage().contains("my_table"));
    }

    @Test
    public void testResponseRoundTripOverloaded() throws IlpV4ParseException {
        IlpV4Response original = IlpV4Response.overloaded();
        byte[] encoded = IlpV4ResponseEncoder.encode(original);
        IlpV4Response decoded = IlpV4ResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(IlpV4StatusCode.OVERLOADED, decoded.getStatusCode());
        assertTrue(decoded.isRetriable());
    }

    @Test
    public void testResponseRoundTripEmptyPartial() throws IlpV4ParseException {
        ObjList<IlpV4Response.TableError> errors = new ObjList<>();
        IlpV4Response original = IlpV4Response.partial(errors);
        byte[] encoded = IlpV4ResponseEncoder.encode(original);
        IlpV4Response decoded = IlpV4ResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(IlpV4StatusCode.PARTIAL, decoded.getStatusCode());
        assertEquals(0, decoded.getTableErrors().size());
    }

    @Test
    public void testResponseRoundTripUnicodeMessage() throws IlpV4ParseException {
        String unicode = "エラー: テーブルが見つかりません 日本語";
        IlpV4Response original = IlpV4Response.parseError(unicode);
        byte[] encoded = IlpV4ResponseEncoder.encode(original);
        IlpV4Response decoded = IlpV4ResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(IlpV4StatusCode.PARSE_ERROR, decoded.getStatusCode());
        assertEquals(unicode, decoded.getErrorMessage());
    }

    @Test
    public void testResponseRoundTripLargeTableIndex() throws IlpV4ParseException {
        ObjList<IlpV4Response.TableError> errors = new ObjList<>();
        errors.add(new IlpV4Response.TableError(255, IlpV4StatusCode.PARSE_ERROR, "error at table 255"));

        IlpV4Response original = IlpV4Response.partial(errors);
        byte[] encoded = IlpV4ResponseEncoder.encode(original);
        IlpV4Response decoded = IlpV4ResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(255, decoded.getTableErrors().get(0).getTableIndex());
    }

    // ==================== Error Handling Tests ====================

    @Test
    public void testDecodeTruncatedResponse() {
        try {
            IlpV4ResponseEncoder.decode(new byte[0], 0, 0);
            fail("Expected exception");
        } catch (IlpV4ParseException e) {
            assertEquals(IlpV4ParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
        }
    }

    @Test
    public void testDecodeTruncatedErrorMessage() {
        // Create a response with truncated message length
        byte[] truncated = new byte[]{IlpV4StatusCode.PARSE_ERROR, 50}; // says 50 bytes but only 2 bytes total

        try {
            IlpV4ResponseEncoder.decode(truncated, 0, truncated.length);
            fail("Expected exception");
        } catch (IlpV4ParseException e) {
            assertEquals(IlpV4ParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
        }
    }

    // ==================== Size Calculation Tests ====================

    @Test
    public void testCalculateSizeOk() {
        IlpV4Response response = IlpV4Response.ok();
        assertEquals(1, IlpV4ResponseEncoder.calculateSize(response));
    }

    @Test
    public void testCalculateSizeError() {
        IlpV4Response response = IlpV4Response.parseError("test");
        // 1 (status) + 1 (varint len) + 4 (message)
        assertEquals(6, IlpV4ResponseEncoder.calculateSize(response));
    }

    @Test
    public void testCalculateSizePartial() {
        ObjList<IlpV4Response.TableError> errors = new ObjList<>();
        errors.add(new IlpV4Response.TableError(0, IlpV4StatusCode.PARSE_ERROR, "err"));

        IlpV4Response response = IlpV4Response.partial(errors);
        // 1 (status) + 1 (count) + 1 (tableIndex) + 1 (errorCode) + 1 (msgLen) + 3 (msg)
        assertEquals(8, IlpV4ResponseEncoder.calculateSize(response));
    }

    // ==================== toString Tests ====================

    @Test
    public void testResponseToString() {
        IlpV4Response ok = IlpV4Response.ok();
        assertTrue(ok.toString().contains("OK"));

        IlpV4Response error = IlpV4Response.parseError("bad");
        assertTrue(error.toString().contains("PARSE_ERROR"));
        assertTrue(error.toString().contains("bad"));

        ObjList<IlpV4Response.TableError> errors = new ObjList<>();
        errors.add(new IlpV4Response.TableError(0, IlpV4StatusCode.INTERNAL_ERROR, "oops"));
        IlpV4Response partial = IlpV4Response.partial(errors);
        assertTrue(partial.toString().contains("PARTIAL"));
        assertTrue(partial.toString().contains("tableErrors"));
    }
}
