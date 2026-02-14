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

/**
 * ILP v4 response status codes.
 * <p>
 * Status codes are sent as the first byte of every response to indicate
 * the result of batch processing.
 */
public final class QwpStatusCode {

    /**
     * Batch accepted successfully.
     */
    public static final byte OK = 0x00;

    /**
     * Some rows failed processing. Error payload contains per-table details.
     */
    public static final byte PARTIAL = 0x01;

    /**
     * Schema hash not recognized. Client should resend with full schema.
     */
    public static final byte SCHEMA_REQUIRED = 0x02;

    /**
     * Column type incompatible with existing table schema.
     */
    public static final byte SCHEMA_MISMATCH = 0x03;

    /**
     * Table doesn't exist and auto-create is disabled.
     */
    public static final byte TABLE_NOT_FOUND = 0x04;

    /**
     * Malformed message (parsing error).
     */
    public static final byte PARSE_ERROR = 0x05;

    /**
     * Server error.
     */
    public static final byte INTERNAL_ERROR = 0x06;

    /**
     * Server overloaded, client should retry with backoff.
     */
    public static final byte OVERLOADED = 0x07;

    private QwpStatusCode() {
        // Prevent instantiation
    }

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
            case SCHEMA_REQUIRED -> "SCHEMA_REQUIRED";
            case SCHEMA_MISMATCH -> "SCHEMA_MISMATCH";
            case TABLE_NOT_FOUND -> "TABLE_NOT_FOUND";
            case PARSE_ERROR -> "PARSE_ERROR";
            case INTERNAL_ERROR -> "INTERNAL_ERROR";
            case OVERLOADED -> "OVERLOADED";
            default -> "UNKNOWN(" + (code & 0xFF) + ")";
        };
    }

    /**
     * Returns true if the status indicates a retriable error.
     *
     * @param code status code
     * @return true if retriable
     */
    public static boolean isRetriable(byte code) {
        return code == SCHEMA_REQUIRED || code == OVERLOADED;
    }

    /**
     * Returns true if the status indicates success (full or partial).
     *
     * @param code status code
     * @return true if success
     */
    public static boolean isSuccess(byte code) {
        return code == OK || code == PARTIAL;
    }
}
