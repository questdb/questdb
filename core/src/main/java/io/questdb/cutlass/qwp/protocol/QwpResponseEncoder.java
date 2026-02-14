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
import io.questdb.std.Unsafe;

import java.nio.charset.StandardCharsets;

/**
 * Encodes ILP v4 responses to binary format.
 * <p>
 * Response format for OK:
 * <pre>
 * ┌──────────────┐
 * │ Status: 0x00 │
 * └──────────────┘
 * </pre>
 * <p>
 * Response format for errors (non-partial):
 * <pre>
 * ┌─────────────────────────────────────────┐
 * │ Status code: uint8                      │
 * │ Error message length: varint            │
 * │ Error message: UTF-8 bytes              │
 * └─────────────────────────────────────────┘
 * </pre>
 * <p>
 * Response format for PARTIAL:
 * <pre>
 * ┌─────────────────────────────────────────┐
 * │ Status: 0x01                            │
 * │ Failed table count: varint              │
 * │ For each failed table:                  │
 * │   Table index: varint                   │
 * │   Error code: uint8                     │
 * │   Error message length: varint          │
 * │   Error message: UTF-8 bytes            │
 * └─────────────────────────────────────────┘
 * </pre>
 */
public class QwpResponseEncoder {

    /**
     * Encodes a response to the given memory address.
     *
     * @param response response to encode
     * @param address  destination address
     * @param maxLen   maximum bytes available
     * @return number of bytes written
     * @throws IllegalArgumentException if buffer too small
     */
    public static int encode(QwpResponse response, long address, int maxLen) {
        int offset = 0;

        // Write status code
        if (offset >= maxLen) {
            throw new IllegalArgumentException("buffer too small for status code");
        }
        Unsafe.getUnsafe().putByte(address + offset, response.getStatusCode());
        offset++;

        byte statusCode = response.getStatusCode();

        if (statusCode == QwpStatusCode.OK) {
            // OK response is just the status byte
            return offset;
        }

        if (statusCode == QwpStatusCode.PARTIAL) {
            // Partial response has table errors
            ObjList<QwpResponse.TableError> errors = response.getTableErrors();
            int errorCount = errors != null ? errors.size() : 0;

            // Write error count
            offset += encodeVarint(errorCount, address + offset, maxLen - offset);

            // Write each table error
            for (int i = 0; i < errorCount; i++) {
                QwpResponse.TableError error = errors.get(i);

                // Table index
                offset += encodeVarint(error.getTableIndex(), address + offset, maxLen - offset);

                // Error code
                if (offset >= maxLen) {
                    throw new IllegalArgumentException("buffer too small for error code");
                }
                Unsafe.getUnsafe().putByte(address + offset, error.getErrorCode());
                offset++;

                // Error message
                offset += encodeString(error.getErrorMessage(), address + offset, maxLen - offset);
            }
        } else {
            // Other error responses have an error message
            String errorMessage = response.getErrorMessage();
            offset += encodeString(errorMessage, address + offset, maxLen - offset);
        }

        return offset;
    }

    /**
     * Encodes a response to a byte array.
     *
     * @param response response to encode
     * @return encoded bytes
     */
    public static byte[] encode(QwpResponse response) {
        // Calculate required size
        int size = calculateSize(response);
        byte[] buf = new byte[size];

        // Encode to native memory then copy
        long address = Unsafe.malloc(size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        try {
            int written = encode(response, address, size);
            for (int i = 0; i < written; i++) {
                buf[i] = Unsafe.getUnsafe().getByte(address + i);
            }
            return buf;
        } finally {
            Unsafe.free(address, size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Calculates the encoded size of a response.
     *
     * @param response response to measure
     * @return size in bytes
     */
    public static int calculateSize(QwpResponse response) {
        int size = 1; // status code

        byte statusCode = response.getStatusCode();

        if (statusCode == QwpStatusCode.OK) {
            return size;
        }

        if (statusCode == QwpStatusCode.PARTIAL) {
            ObjList<QwpResponse.TableError> errors = response.getTableErrors();
            int errorCount = errors != null ? errors.size() : 0;

            size += varintSize(errorCount);

            for (int i = 0; i < errorCount; i++) {
                QwpResponse.TableError error = errors.get(i);
                size += varintSize(error.getTableIndex());
                size += 1; // error code
                size += stringSize(error.getErrorMessage());
            }
        } else {
            size += stringSize(response.getErrorMessage());
        }

        return size;
    }

    /**
     * Decodes a response from a byte array.
     *
     * @param buf    buffer containing response
     * @param offset starting offset
     * @param length available bytes
     * @return decoded response
     * @throws QwpParseException if parsing fails
     */
    public static QwpResponse decode(byte[] buf, int offset, int length) throws QwpParseException {
        if (length < 1) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "response too short"
            );
        }

        byte statusCode = buf[offset];
        int pos = offset + 1;

        if (statusCode == QwpStatusCode.OK) {
            return QwpResponse.ok();
        }

        if (statusCode == QwpStatusCode.PARTIAL) {
            // Decode table errors
            int[] varintResult = decodeVarint(buf, pos, offset + length);
            int errorCount = varintResult[0];
            pos = varintResult[1];

            ObjList<QwpResponse.TableError> errors = new ObjList<>(errorCount);
            for (int i = 0; i < errorCount; i++) {
                // Table index
                varintResult = decodeVarint(buf, pos, offset + length);
                int tableIndex = varintResult[0];
                pos = varintResult[1];

                // Error code
                if (pos >= offset + length) {
                    throw QwpParseException.create(
                            QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                            "unexpected end of response reading error code"
                    );
                }
                byte errorCode = buf[pos++];

                // Error message
                String[] stringResult = decodeString(buf, pos, offset + length);
                String errorMessage = stringResult[0];
                pos = Integer.parseInt(stringResult[1]);

                errors.add(new QwpResponse.TableError(tableIndex, errorCode, errorMessage));
            }

            return QwpResponse.partial(errors);
        } else {
            // Decode error message
            String[] stringResult = decodeString(buf, pos, offset + length);
            String errorMessage = stringResult[0];

            return QwpResponse.error(statusCode, errorMessage);
        }
    }

    /**
     * Decodes a response from direct memory.
     *
     * @param address starting address
     * @param length  available bytes
     * @return decoded response
     * @throws QwpParseException if parsing fails
     */
    public static QwpResponse decode(long address, int length) throws QwpParseException {
        byte[] buf = new byte[length];
        for (int i = 0; i < length; i++) {
            buf[i] = Unsafe.getUnsafe().getByte(address + i);
        }
        return decode(buf, 0, length);
    }

    // Helper methods

    private static int encodeVarint(long value, long address, int maxLen) {
        int offset = 0;
        while (value > 0x7F) {
            if (offset >= maxLen) {
                throw new IllegalArgumentException("buffer too small for varint");
            }
            Unsafe.getUnsafe().putByte(address + offset, (byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
            offset++;
        }
        if (offset >= maxLen) {
            throw new IllegalArgumentException("buffer too small for varint");
        }
        Unsafe.getUnsafe().putByte(address + offset, (byte) value);
        return offset + 1;
    }

    private static int varintSize(long value) {
        int size = 1;
        while (value > 0x7F) {
            value >>>= 7;
            size++;
        }
        return size;
    }

    private static int encodeString(String str, long address, int maxLen) {
        if (str == null) {
            return encodeVarint(0, address, maxLen);
        }

        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        int offset = encodeVarint(bytes.length, address, maxLen);

        if (offset + bytes.length > maxLen) {
            throw new IllegalArgumentException("buffer too small for string");
        }

        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(address + offset + i, bytes[i]);
        }

        return offset + bytes.length;
    }

    private static int stringSize(String str) {
        if (str == null) {
            return 1; // varint(0)
        }
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        return varintSize(bytes.length) + bytes.length;
    }

    private static int[] decodeVarint(byte[] buf, int offset, int limit) throws QwpParseException {
        long result = 0;
        int shift = 0;
        int pos = offset;

        while (pos < limit) {
            byte b = buf[pos++];
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return new int[]{(int) result, pos};
            }
            shift += 7;
            if (shift >= 64) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.VARINT_OVERFLOW,
                        "varint too long"
                );
            }
        }

        throw QwpParseException.create(
                QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                "incomplete varint"
        );
    }

    private static String[] decodeString(byte[] buf, int offset, int limit) throws QwpParseException {
        int[] varintResult = decodeVarint(buf, offset, limit);
        int strLen = varintResult[0];
        int pos = varintResult[1];

        if (pos + strLen > limit) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "string extends beyond buffer"
            );
        }

        String str = new String(buf, pos, strLen, StandardCharsets.UTF_8);
        return new String[]{str, String.valueOf(pos + strLen)};
    }
}
