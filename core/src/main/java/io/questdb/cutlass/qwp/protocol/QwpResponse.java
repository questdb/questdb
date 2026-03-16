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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a QWP v1 response to be sent to the client.
 * <p>
 * Response format:
 * <pre>
 * ┌─────────────────────────────────────────────────────────────┐
 * │ Status code: uint8                                          │
 * │ [Error payload if status != 0]                              │
 * └─────────────────────────────────────────────────────────────┘
 * </pre>
 * <p>
 * For PARTIAL status, additional payload:
 * <pre>
 * ┌─────────────────────────────────────────────────────────────┐
 * │ Failed table count: varint                                  │
 * │ For each failed table:                                      │
 * │   Table index: varint (0-based index in batch)              │
 * │   Error code: uint8                                         │
 * │   Error message length: varint                              │
 * │   Error message: UTF-8 bytes                                │
 * └─────────────────────────────────────────────────────────────┘
 * </pre>
 */
public class QwpResponse {

    /**
     * Batch accepted successfully.
     */
    public static final byte OK = 0x00;
    /**
     * Server overloaded, the client should retry with backoff.
     */
    public static final byte OVERLOADED = 0x07;
    /**
     * Some rows failed to process. Error payload contains per-table details.
     */
    public static final byte PARTIAL = 0x01;

    private String errorMessage;
    private byte statusCode;
    private ObjList<TableError> tableErrors;

    /**
     * Returns a human-readable name for the status code.
     *
     * @param code status code
     * @return name string
     */
    public static String name(byte code) {
        return switch (code) {
            case OK -> "OK";
            case PARTIAL -> "PARTIAL";
            case OVERLOADED -> "OVERLOADED";
            default -> "UNKNOWN(" + (code & 0xFF) + ")";
        };
    }

    /**
     * Creates an OK response.
     *
     * @return OK response
     */
    public static QwpResponse ok() {
        QwpResponse response = new QwpResponse();
        response.statusCode = OK;
        return response;
    }

    /**
     * Creates an overloaded response.
     *
     * @return overloaded response
     */
    public static QwpResponse overloaded() {
        QwpResponse response = new QwpResponse();
        response.statusCode = OVERLOADED;
        response.errorMessage = "server overloaded, retry later";
        return response;
    }

    /**
     * Creates a partial failure response.
     *
     * @param errors list of table errors
     * @return partial response
     */
    public static QwpResponse partial(ObjList<TableError> errors) {
        QwpResponse response = new QwpResponse();
        response.statusCode = PARTIAL;
        response.tableErrors = errors;
        return response;
    }

    /**
     * Gets the error message (for non-partial errors).
     *
     * @return error message, or null for OK/PARTIAL
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Gets the status code.
     *
     * @return status code
     */
    public byte getStatusCode() {
        return statusCode;
    }

    /**
     * Returns true if this is a success response.
     *
     * @return true if OK or PARTIAL
     */
    public boolean isSuccess() {
        return statusCode == OK || statusCode == PARTIAL;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("QwpResponse{status=").append(name(statusCode));
        if (errorMessage != null) {
            sb.append(", message=\"").append(errorMessage).append("\"");
        }
        if (tableErrors != null && tableErrors.size() > 0) {
            sb.append(", tableErrors=[");
            for (int i = 0; i < tableErrors.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(tableErrors.get(i));
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Represents an error for a specific table in a partial failure.
     */
    public record TableError(int tableIndex, byte errorCode, String errorMessage) {

        @Override
        public @NotNull String toString() {
            return "TableError{index=" + tableIndex +
                    ", code=" + name(errorCode) +
                    ", message=\"" + errorMessage + "\"}";
        }
    }
}
