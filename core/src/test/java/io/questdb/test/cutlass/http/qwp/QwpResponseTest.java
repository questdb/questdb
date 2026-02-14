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

package io.questdb.test.cutlass.http.qwp;

import io.questdb.cutlass.qwp.protocol.*;
import io.questdb.std.ObjList;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for ILP v4 response generation and parsing.
 */
public class QwpResponseTest {

    // ==================== Status Code Tests ====================

    @Test
    public void testStatusCodeName() {
        assertEquals("OK", QwpStatusCode.name(QwpStatusCode.OK));
        assertEquals("PARTIAL", QwpStatusCode.name(QwpStatusCode.PARTIAL));
        assertEquals("SCHEMA_REQUIRED", QwpStatusCode.name(QwpStatusCode.SCHEMA_REQUIRED));
        assertEquals("SCHEMA_MISMATCH", QwpStatusCode.name(QwpStatusCode.SCHEMA_MISMATCH));
        assertEquals("TABLE_NOT_FOUND", QwpStatusCode.name(QwpStatusCode.TABLE_NOT_FOUND));
        assertEquals("PARSE_ERROR", QwpStatusCode.name(QwpStatusCode.PARSE_ERROR));
        assertEquals("INTERNAL_ERROR", QwpStatusCode.name(QwpStatusCode.INTERNAL_ERROR));
        assertEquals("OVERLOADED", QwpStatusCode.name(QwpStatusCode.OVERLOADED));
        assertEquals("UNKNOWN(255)", QwpStatusCode.name((byte) 0xFF));
    }

    @Test
    public void testStatusCodeIsRetriable() {
        assertFalse(QwpStatusCode.isRetriable(QwpStatusCode.OK));
        assertFalse(QwpStatusCode.isRetriable(QwpStatusCode.PARTIAL));
        assertTrue(QwpStatusCode.isRetriable(QwpStatusCode.SCHEMA_REQUIRED));
        assertFalse(QwpStatusCode.isRetriable(QwpStatusCode.SCHEMA_MISMATCH));
        assertFalse(QwpStatusCode.isRetriable(QwpStatusCode.TABLE_NOT_FOUND));
        assertFalse(QwpStatusCode.isRetriable(QwpStatusCode.PARSE_ERROR));
        assertFalse(QwpStatusCode.isRetriable(QwpStatusCode.INTERNAL_ERROR));
        assertTrue(QwpStatusCode.isRetriable(QwpStatusCode.OVERLOADED));
    }

    @Test
    public void testStatusCodeIsSuccess() {
        assertTrue(QwpStatusCode.isSuccess(QwpStatusCode.OK));
        assertTrue(QwpStatusCode.isSuccess(QwpStatusCode.PARTIAL));
        assertFalse(QwpStatusCode.isSuccess(QwpStatusCode.SCHEMA_REQUIRED));
        assertFalse(QwpStatusCode.isSuccess(QwpStatusCode.INTERNAL_ERROR));
    }

    // ==================== Response Factory Tests ====================

    @Test
    public void testGenerateOkResponse() {
        QwpResponse response = QwpResponse.ok();

        assertEquals(QwpStatusCode.OK, response.getStatusCode());
        assertTrue(response.isSuccess());
        assertFalse(response.isRetriable());
        assertNull(response.getErrorMessage());
        assertNull(response.getTableErrors());
    }

    @Test
    public void testGenerateSchemaRequiredResponse() {
        QwpResponse response = QwpResponse.schemaRequired("trades");

        assertEquals(QwpStatusCode.SCHEMA_REQUIRED, response.getStatusCode());
        assertFalse(response.isSuccess());
        assertTrue(response.isRetriable());
        assertTrue(response.getErrorMessage().contains("trades"));
    }

    @Test
    public void testGenerateSchemaMismatchResponse() {
        QwpResponse response = QwpResponse.schemaMismatch("trades", "price");

        assertEquals(QwpStatusCode.SCHEMA_MISMATCH, response.getStatusCode());
        assertFalse(response.isSuccess());
        assertFalse(response.isRetriable());
        assertTrue(response.getErrorMessage().contains("trades"));
        assertTrue(response.getErrorMessage().contains("price"));
    }

    @Test
    public void testGenerateTableNotFoundResponse() {
        QwpResponse response = QwpResponse.tableNotFound("missing_table");

        assertEquals(QwpStatusCode.TABLE_NOT_FOUND, response.getStatusCode());
        assertFalse(response.isSuccess());
        assertTrue(response.getErrorMessage().contains("missing_table"));
    }

    @Test
    public void testGenerateParseErrorResponse() {
        QwpResponse response = QwpResponse.parseError("invalid varint at offset 42");

        assertEquals(QwpStatusCode.PARSE_ERROR, response.getStatusCode());
        assertFalse(response.isSuccess());
        assertEquals("invalid varint at offset 42", response.getErrorMessage());
    }

    @Test
    public void testGenerateInternalErrorResponse() {
        QwpResponse response = QwpResponse.internalError("out of memory");

        assertEquals(QwpStatusCode.INTERNAL_ERROR, response.getStatusCode());
        assertFalse(response.isSuccess());
        assertEquals("out of memory", response.getErrorMessage());
    }

    @Test
    public void testGenerateOverloadedResponse() {
        QwpResponse response = QwpResponse.overloaded();

        assertEquals(QwpStatusCode.OVERLOADED, response.getStatusCode());
        assertFalse(response.isSuccess());
        assertTrue(response.isRetriable());
        assertNotNull(response.getErrorMessage());
    }

    @Test
    public void testGeneratePartialResponse() {
        ObjList<QwpResponse.TableError> errors = new ObjList<>();
        errors.add(new QwpResponse.TableError(0, QwpStatusCode.SCHEMA_MISMATCH, "type mismatch"));
        errors.add(new QwpResponse.TableError(2, QwpStatusCode.TABLE_NOT_FOUND, "table not found"));

        QwpResponse response = QwpResponse.partial(errors);

        assertEquals(QwpStatusCode.PARTIAL, response.getStatusCode());
        assertTrue(response.isSuccess()); // Partial is considered success
        assertNotNull(response.getTableErrors());
        assertEquals(2, response.getTableErrors().size());
    }

    @Test
    public void testPartialResponseMultipleTables() {
        ObjList<QwpResponse.TableError> errors = new ObjList<>();
        errors.add(new QwpResponse.TableError(0, QwpStatusCode.SCHEMA_MISMATCH, "error in table 0"));
        errors.add(new QwpResponse.TableError(3, QwpStatusCode.TABLE_NOT_FOUND, "error in table 3"));
        errors.add(new QwpResponse.TableError(7, QwpStatusCode.PARSE_ERROR, "error in table 7"));

        QwpResponse response = QwpResponse.partial(errors);

        ObjList<QwpResponse.TableError> tableErrors = response.getTableErrors();
        assertEquals(3, tableErrors.size());

        assertEquals(0, tableErrors.get(0).getTableIndex());
        assertEquals(QwpStatusCode.SCHEMA_MISMATCH, tableErrors.get(0).getErrorCode());

        assertEquals(3, tableErrors.get(1).getTableIndex());
        assertEquals(QwpStatusCode.TABLE_NOT_FOUND, tableErrors.get(1).getErrorCode());

        assertEquals(7, tableErrors.get(2).getTableIndex());
        assertEquals(QwpStatusCode.PARSE_ERROR, tableErrors.get(2).getErrorCode());
    }

    @Test
    public void testPartialResponseErrorMessages() {
        QwpResponse.TableError error = new QwpResponse.TableError(
                5, QwpStatusCode.INTERNAL_ERROR, "detailed error message here");

        assertEquals(5, error.getTableIndex());
        assertEquals(QwpStatusCode.INTERNAL_ERROR, error.getErrorCode());
        assertEquals("detailed error message here", error.getErrorMessage());

        String str = error.toString();
        assertTrue(str.contains("5"));
        assertTrue(str.contains("INTERNAL_ERROR"));
        assertTrue(str.contains("detailed error message here"));
    }

    // ==================== Encoder Tests ====================

    @Test
    public void testResponseSerializationOk() {
        QwpResponse response = QwpResponse.ok();
        byte[] encoded = QwpResponseEncoder.encode(response);

        assertEquals(1, encoded.length);
        assertEquals(QwpStatusCode.OK, encoded[0]);
    }

    @Test
    public void testResponseSerializationError() {
        QwpResponse response = QwpResponse.parseError("bad data");
        byte[] encoded = QwpResponseEncoder.encode(response);

        // First byte is status code
        assertEquals(QwpStatusCode.PARSE_ERROR, encoded[0]);

        // Should be: status + varint(8) + "bad data"
        assertEquals(1 + 1 + 8, encoded.length);
    }

    @Test
    public void testResponseSerializationPartial() {
        ObjList<QwpResponse.TableError> errors = new ObjList<>();
        errors.add(new QwpResponse.TableError(0, QwpStatusCode.SCHEMA_MISMATCH, "type error"));

        QwpResponse response = QwpResponse.partial(errors);
        byte[] encoded = QwpResponseEncoder.encode(response);

        // First byte is PARTIAL
        assertEquals(QwpStatusCode.PARTIAL, encoded[0]);

        // Should have: status + errorCount + (tableIndex + errorCode + msgLen + msg)
        assertTrue(encoded.length > 1);
    }

    // ==================== Round-trip Tests ====================

    @Test
    public void testResponseRoundTripOk() throws QwpParseException {
        QwpResponse original = QwpResponse.ok();
        byte[] encoded = QwpResponseEncoder.encode(original);
        QwpResponse decoded = QwpResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(QwpStatusCode.OK, decoded.getStatusCode());
        assertTrue(decoded.isSuccess());
    }

    @Test
    public void testResponseRoundTripError() throws QwpParseException {
        QwpResponse original = QwpResponse.internalError("something went wrong");
        byte[] encoded = QwpResponseEncoder.encode(original);
        QwpResponse decoded = QwpResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(QwpStatusCode.INTERNAL_ERROR, decoded.getStatusCode());
        assertEquals("something went wrong", decoded.getErrorMessage());
    }

    @Test
    public void testResponseRoundTripPartial() throws QwpParseException {
        ObjList<QwpResponse.TableError> errors = new ObjList<>();
        errors.add(new QwpResponse.TableError(1, QwpStatusCode.TABLE_NOT_FOUND, "table1 missing"));
        errors.add(new QwpResponse.TableError(5, QwpStatusCode.SCHEMA_MISMATCH, "column x wrong type"));

        QwpResponse original = QwpResponse.partial(errors);
        byte[] encoded = QwpResponseEncoder.encode(original);
        QwpResponse decoded = QwpResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(QwpStatusCode.PARTIAL, decoded.getStatusCode());
        assertNotNull(decoded.getTableErrors());
        assertEquals(2, decoded.getTableErrors().size());

        assertEquals(1, decoded.getTableErrors().get(0).getTableIndex());
        assertEquals(QwpStatusCode.TABLE_NOT_FOUND, decoded.getTableErrors().get(0).getErrorCode());
        assertEquals("table1 missing", decoded.getTableErrors().get(0).getErrorMessage());

        assertEquals(5, decoded.getTableErrors().get(1).getTableIndex());
        assertEquals(QwpStatusCode.SCHEMA_MISMATCH, decoded.getTableErrors().get(1).getErrorCode());
        assertEquals("column x wrong type", decoded.getTableErrors().get(1).getErrorMessage());
    }

    @Test
    public void testResponseRoundTripSchemaRequired() throws QwpParseException {
        QwpResponse original = QwpResponse.schemaRequired("my_table");
        byte[] encoded = QwpResponseEncoder.encode(original);
        QwpResponse decoded = QwpResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(QwpStatusCode.SCHEMA_REQUIRED, decoded.getStatusCode());
        assertTrue(decoded.getErrorMessage().contains("my_table"));
    }

    @Test
    public void testResponseRoundTripOverloaded() throws QwpParseException {
        QwpResponse original = QwpResponse.overloaded();
        byte[] encoded = QwpResponseEncoder.encode(original);
        QwpResponse decoded = QwpResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(QwpStatusCode.OVERLOADED, decoded.getStatusCode());
        assertTrue(decoded.isRetriable());
    }

    @Test
    public void testResponseRoundTripEmptyPartial() throws QwpParseException {
        ObjList<QwpResponse.TableError> errors = new ObjList<>();
        QwpResponse original = QwpResponse.partial(errors);
        byte[] encoded = QwpResponseEncoder.encode(original);
        QwpResponse decoded = QwpResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(QwpStatusCode.PARTIAL, decoded.getStatusCode());
        assertEquals(0, decoded.getTableErrors().size());
    }

    @Test
    public void testResponseRoundTripUnicodeMessage() throws QwpParseException {
        String unicode = "エラー: テーブルが見つかりません 日本語";
        QwpResponse original = QwpResponse.parseError(unicode);
        byte[] encoded = QwpResponseEncoder.encode(original);
        QwpResponse decoded = QwpResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(QwpStatusCode.PARSE_ERROR, decoded.getStatusCode());
        assertEquals(unicode, decoded.getErrorMessage());
    }

    @Test
    public void testResponseRoundTripLargeTableIndex() throws QwpParseException {
        ObjList<QwpResponse.TableError> errors = new ObjList<>();
        errors.add(new QwpResponse.TableError(255, QwpStatusCode.PARSE_ERROR, "error at table 255"));

        QwpResponse original = QwpResponse.partial(errors);
        byte[] encoded = QwpResponseEncoder.encode(original);
        QwpResponse decoded = QwpResponseEncoder.decode(encoded, 0, encoded.length);

        assertEquals(255, decoded.getTableErrors().get(0).getTableIndex());
    }

    // ==================== Error Handling Tests ====================

    @Test
    public void testDecodeTruncatedResponse() {
        try {
            QwpResponseEncoder.decode(new byte[0], 0, 0);
            fail("Expected exception");
        } catch (QwpParseException e) {
            assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
        }
    }

    @Test
    public void testDecodeTruncatedErrorMessage() {
        // Create a response with truncated message length
        byte[] truncated = new byte[]{QwpStatusCode.PARSE_ERROR, 50}; // says 50 bytes but only 2 bytes total

        try {
            QwpResponseEncoder.decode(truncated, 0, truncated.length);
            fail("Expected exception");
        } catch (QwpParseException e) {
            assertEquals(QwpParseException.ErrorCode.INSUFFICIENT_DATA, e.getErrorCode());
        }
    }

    // ==================== Size Calculation Tests ====================

    @Test
    public void testCalculateSizeOk() {
        QwpResponse response = QwpResponse.ok();
        assertEquals(1, QwpResponseEncoder.calculateSize(response));
    }

    @Test
    public void testCalculateSizeError() {
        QwpResponse response = QwpResponse.parseError("test");
        // 1 (status) + 1 (varint len) + 4 (message)
        assertEquals(6, QwpResponseEncoder.calculateSize(response));
    }

    @Test
    public void testCalculateSizePartial() {
        ObjList<QwpResponse.TableError> errors = new ObjList<>();
        errors.add(new QwpResponse.TableError(0, QwpStatusCode.PARSE_ERROR, "err"));

        QwpResponse response = QwpResponse.partial(errors);
        // 1 (status) + 1 (count) + 1 (tableIndex) + 1 (errorCode) + 1 (msgLen) + 3 (msg)
        assertEquals(8, QwpResponseEncoder.calculateSize(response));
    }

    // ==================== toString Tests ====================

    @Test
    public void testResponseToString() {
        QwpResponse ok = QwpResponse.ok();
        assertTrue(ok.toString().contains("OK"));

        QwpResponse error = QwpResponse.parseError("bad");
        assertTrue(error.toString().contains("PARSE_ERROR"));
        assertTrue(error.toString().contains("bad"));

        ObjList<QwpResponse.TableError> errors = new ObjList<>();
        errors.add(new QwpResponse.TableError(0, QwpStatusCode.INTERNAL_ERROR, "oops"));
        QwpResponse partial = QwpResponse.partial(errors);
        assertTrue(partial.toString().contains("PARTIAL"));
        assertTrue(partial.toString().contains("tableErrors"));
    }
}
