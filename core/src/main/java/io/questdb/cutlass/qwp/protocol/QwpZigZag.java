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
 * ZigZag encoding/decoding for signed integers.
 * <p>
 * ZigZag encoding maps signed integers to unsigned integers so that
 * numbers with small absolute value have small encoded values.
 * <p>
 * The encoding works as follows:
 * <pre>
 *  0 ->  0
 * -1 ->  1
 *  1 ->  2
 * -2 ->  3
 *  2 ->  4
 * ...
 * </pre>
 * <p>
 * Formula:
 * <pre>
 * encode(n) = (n << 1) ^ (n >> 63)  // for 64-bit
 * decode(n) = (n >>> 1) ^ -(n & 1)
 * </pre>
 * <p>
 * This is useful when combined with varint encoding because small
 * negative numbers like -1 become small positive numbers (1), which
 * encode efficiently as varints.
 */
public final class QwpZigZag {

    private QwpZigZag() {
        // utility class
    }

    /**
     * Encodes a signed 64-bit integer using ZigZag encoding.
     *
     * @param value the signed value to encode
     * @return the ZigZag encoded value (unsigned interpretation)
     */
    public static long encode(long value) {
        return (value << 1) ^ (value >> 63);
    }

    /**
     * Decodes a ZigZag encoded 64-bit integer.
     *
     * @param value the ZigZag encoded value
     * @return the original signed value
     */
    public static long decode(long value) {
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Encodes a signed 32-bit integer using ZigZag encoding.
     *
     * @param value the signed value to encode
     * @return the ZigZag encoded value (unsigned interpretation)
     */
    public static int encode(int value) {
        return (value << 1) ^ (value >> 31);
    }

    /**
     * Decodes a ZigZag encoded 32-bit integer.
     *
     * @param value the ZigZag encoded value
     * @return the original signed value
     */
    public static int decode(int value) {
        return (value >>> 1) ^ -(value & 1);
    }
}
