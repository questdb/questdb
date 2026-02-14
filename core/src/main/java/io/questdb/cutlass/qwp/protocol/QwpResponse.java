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

/**
 * Represents an ILP v4 response to be sent to the client.
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

    private byte statusCode;
    private String errorMessage;
    private ObjList<TableError> tableErrors;

    /**
     * Creates an OK response.
     *
     * @return OK response
     */
    public static QwpResponse ok() {
        QwpResponse response = new QwpResponse();
        response.statusCode = QwpStatusCode.OK;
        return response;
    }

    /**
     * Creates an error response with a message.
     *
     * @param statusCode   error status code
     * @param errorMessage error message
     * @return error response
     */
    public static QwpResponse error(byte statusCode, String errorMessage) {
        QwpResponse response = new QwpResponse();
        response.statusCode = statusCode;
        response.errorMessage = errorMessage;
        return response;
    }

    /**
     * Creates a schema required response.
     *
     * @param tableName table that needs full schema
     * @return schema required response
     */
    public static QwpResponse schemaRequired(String tableName) {
        return error(QwpStatusCode.SCHEMA_REQUIRED, "schema not found for table: " + tableName);
    }

    /**
     * Creates a schema mismatch response.
     *
     * @param tableName  table with mismatched schema
     * @param columnName column with type mismatch
     * @return schema mismatch response
     */
    public static QwpResponse schemaMismatch(String tableName, String columnName) {
        return error(QwpStatusCode.SCHEMA_MISMATCH,
                "column type mismatch [table=" + tableName + ", column=" + columnName + "]");
    }

    /**
     * Creates a table not found response.
     *
     * @param tableName table that wasn't found
     * @return table not found response
     */
    public static QwpResponse tableNotFound(String tableName) {
        return error(QwpStatusCode.TABLE_NOT_FOUND, "table not found: " + tableName);
    }

    /**
     * Creates a parse error response.
     *
     * @param message error details
     * @return parse error response
     */
    public static QwpResponse parseError(String message) {
        return error(QwpStatusCode.PARSE_ERROR, message);
    }

    /**
     * Creates an internal error response.
     *
     * @param message error details
     * @return internal error response
     */
    public static QwpResponse internalError(String message) {
        return error(QwpStatusCode.INTERNAL_ERROR, message);
    }

    /**
     * Creates an overloaded response.
     *
     * @return overloaded response
     */
    public static QwpResponse overloaded() {
        return error(QwpStatusCode.OVERLOADED, "server overloaded, retry later");
    }

    /**
     * Creates a partial failure response.
     *
     * @param errors list of table errors
     * @return partial response
     */
    public static QwpResponse partial(ObjList<TableError> errors) {
        QwpResponse response = new QwpResponse();
        response.statusCode = QwpStatusCode.PARTIAL;
        response.tableErrors = errors;
        return response;
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
     * Gets the error message (for non-partial errors).
     *
     * @return error message, or null for OK/PARTIAL
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Gets the list of table errors (for PARTIAL responses).
     *
     * @return table errors, or null for non-PARTIAL responses
     */
    public ObjList<TableError> getTableErrors() {
        return tableErrors;
    }

    /**
     * Returns true if this is a success response.
     *
     * @return true if OK or PARTIAL
     */
    public boolean isSuccess() {
        return QwpStatusCode.isSuccess(statusCode);
    }

    /**
     * Returns true if this is a retriable error.
     *
     * @return true if retriable
     */
    public boolean isRetriable() {
        return QwpStatusCode.isRetriable(statusCode);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("QwpResponse{status=").append(QwpStatusCode.name(statusCode));
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
    public static class TableError {
        private final int tableIndex;
        private final byte errorCode;
        private final String errorMessage;

        public TableError(int tableIndex, byte errorCode, String errorMessage) {
            this.tableIndex = tableIndex;
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
        }

        public int getTableIndex() {
            return tableIndex;
        }

        public byte getErrorCode() {
            return errorCode;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public String toString() {
            return "TableError{index=" + tableIndex +
                    ", code=" + QwpStatusCode.name(errorCode) +
                    ", message=\"" + errorMessage + "\"}";
        }
    }
}
