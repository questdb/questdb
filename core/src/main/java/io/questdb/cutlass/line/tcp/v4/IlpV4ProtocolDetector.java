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

package io.questdb.cutlass.line.tcp.v4;

import io.questdb.std.Unsafe;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * Detects which protocol variant is being used based on the first bytes.
 * <p>
 * Protocol detection works as follows:
 * <ul>
 *   <li>"ILP?" - ILP v4 capability request (handshake)</li>
 *   <li>"ILP4" - ILP v4 direct message (no handshake)</li>
 *   <li>ASCII letter - Text protocol (table name starts with letter)</li>
 *   <li>Other - Unknown/invalid</li>
 * </ul>
 * <p>
 * Only 4 bytes are needed to detect the protocol.
 */
public final class IlpV4ProtocolDetector {

    /**
     * Detection result types.
     */
    public enum DetectionResult {
        /**
         * ILP v4 capability request ("ILP?" magic).
         */
        V4_HANDSHAKE,

        /**
         * ILP v4 direct message ("ILP4" magic).
         */
        V4_DIRECT,

        /**
         * Text protocol (starts with ASCII letter).
         */
        TEXT_PROTOCOL,

        /**
         * Need more bytes to determine protocol.
         */
        NEED_MORE_DATA,

        /**
         * Unknown/invalid protocol.
         */
        UNKNOWN
    }

    /**
     * Minimum bytes needed for protocol detection.
     */
    public static final int MIN_BYTES_FOR_DETECTION = 4;

    private IlpV4ProtocolDetector() {
        // utility class
    }

    /**
     * Detects the protocol from direct memory.
     *
     * @param address the memory address
     * @param length  available bytes
     * @return detection result
     */
    public static DetectionResult detect(long address, int length) {
        if (length < MIN_BYTES_FOR_DETECTION) {
            if (length == 0) {
                return DetectionResult.NEED_MORE_DATA;
            }
            // Check if first byte could be text protocol
            byte firstByte = Unsafe.getUnsafe().getByte(address);
            if (isAsciiLetter(firstByte)) {
                // Could be text protocol, but wait for more bytes to be sure
                // Actually, if it starts with a letter, it's definitely not ILP v4
                // which always starts with 'I' for "ILP"
                if (firstByte != 'I') {
                    return DetectionResult.TEXT_PROTOCOL;
                }
            }
            return DetectionResult.NEED_MORE_DATA;
        }

        int magic = Unsafe.getUnsafe().getInt(address);
        return detectFromMagic(magic, Unsafe.getUnsafe().getByte(address));
    }

    /**
     * Detects the protocol from a byte array.
     *
     * @param buf    the buffer
     * @param offset starting offset
     * @param length available bytes
     * @return detection result
     */
    public static DetectionResult detect(byte[] buf, int offset, int length) {
        if (length < MIN_BYTES_FOR_DETECTION) {
            if (length == 0) {
                return DetectionResult.NEED_MORE_DATA;
            }
            byte firstByte = buf[offset];
            if (isAsciiLetter(firstByte)) {
                if (firstByte != 'I') {
                    return DetectionResult.TEXT_PROTOCOL;
                }
            }
            return DetectionResult.NEED_MORE_DATA;
        }

        int magic = (buf[offset] & 0xFF) |
                ((buf[offset + 1] & 0xFF) << 8) |
                ((buf[offset + 2] & 0xFF) << 16) |
                ((buf[offset + 3] & 0xFF) << 24);

        return detectFromMagic(magic, buf[offset]);
    }

    /**
     * Detects protocol from the magic integer.
     *
     * @param magic     the first 4 bytes as little-endian int
     * @param firstByte the first byte (for text protocol detection)
     * @return detection result
     */
    private static DetectionResult detectFromMagic(int magic, byte firstByte) {
        if (magic == MAGIC_CAPABILITY_REQUEST) {
            return DetectionResult.V4_HANDSHAKE;
        }

        if (magic == MAGIC_MESSAGE) {
            return DetectionResult.V4_DIRECT;
        }

        // Check for text protocol (starts with ASCII letter, which is table name)
        if (isAsciiLetter(firstByte)) {
            return DetectionResult.TEXT_PROTOCOL;
        }

        return DetectionResult.UNKNOWN;
    }

    /**
     * Checks if a byte is an ASCII letter (a-z, A-Z).
     *
     * @param b the byte to check
     * @return true if ASCII letter
     */
    private static boolean isAsciiLetter(byte b) {
        return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z');
    }

    /**
     * Returns true if the detection result indicates an ILP v4 protocol.
     *
     * @param result the detection result
     * @return true if v4 protocol
     */
    public static boolean isV4Protocol(DetectionResult result) {
        return result == DetectionResult.V4_HANDSHAKE || result == DetectionResult.V4_DIRECT;
    }

    /**
     * Peeks at the magic integer without full detection.
     *
     * @param address memory address
     * @return magic integer (only valid if at least 4 bytes available)
     */
    public static int peekMagic(long address) {
        return Unsafe.getUnsafe().getInt(address);
    }

    /**
     * Peeks at the magic integer from a byte array.
     *
     * @param buf    buffer
     * @param offset starting offset
     * @return magic integer (only valid if at least 4 bytes available)
     */
    public static int peekMagic(byte[] buf, int offset) {
        return (buf[offset] & 0xFF) |
                ((buf[offset + 1] & 0xFF) << 8) |
                ((buf[offset + 2] & 0xFF) << 16) |
                ((buf[offset + 3] & 0xFF) << 24);
    }
}
