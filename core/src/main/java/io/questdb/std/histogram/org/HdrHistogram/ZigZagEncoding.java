/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

// Written by Gil Tene of Azul Systems, and released to the public domain,
// as explained at http://creativecommons.org/publicdomain/zero/1.0/
//
// @author Gil Tene

package io.questdb.std.histogram.org.HdrHistogram;

import java.nio.ByteBuffer;

/**
 * This class provides encoding and decoding methods for writing and reading
 * ZigZag-encoded LEB128-64b9B-variant (Little Endian Base 128) values to/from a
 * {@link ByteBuffer}. LEB128's variable length encoding provides for using a
 * smaller nuber of bytes for smaller values, and the use of ZigZag encoding
 * allows small (closer to zero) negative values to use fewer bytes. Details
 * on both LEB128 and ZigZag can be readily found elsewhere.
 * <p>
 * The LEB128-64b9B-variant encoding used here diverges from the "original"
 * LEB128 as it extends to 64 bit values: In the original LEB128, a 64 bit
 * value can take up to 10 bytes in the stream, where this variant's encoding
 * of a 64 bit values will max out at 9 bytes.
 * <p>
 * As such, this encoder/decoder should NOT be used for encoding or decoding
 * "standard" LEB128 formats (e.g. Google Protocol Buffers).
 */
class ZigZagEncoding {

    /**
     * Read an LEB128-64b9B ZigZag encoded int value from the given buffer
     *
     * @param buffer the buffer to read from
     * @return the value read from the buffer
     */
    static int getInt(ByteBuffer buffer) {
        int v = buffer.get();
        int value = v & 0x7F;
        if ((v & 0x80) != 0) {
            v = buffer.get();
            value |= (v & 0x7F) << 7;
            if ((v & 0x80) != 0) {
                v = buffer.get();
                value |= (v & 0x7F) << 14;
                if ((v & 0x80) != 0) {
                    v = buffer.get();
                    value |= (v & 0x7F) << 21;
                    if ((v & 0x80) != 0) {
                        v = buffer.get();
                        value |= (v & 0x7F) << 28;
                    }
                }
            }
        }
        value = (value >>> 1) ^ (-(value & 1));
        return value;
    }

    /**
     * Read an LEB128-64b9B ZigZag encoded long value from the given buffer
     *
     * @param buffer the buffer to read from
     * @return the value read from the buffer
     */
    static long getLong(ByteBuffer buffer) {
        long v = buffer.get();
        long value = v & 0x7F;
        if ((v & 0x80) != 0) {
            v = buffer.get();
            value |= (v & 0x7F) << 7;
            if ((v & 0x80) != 0) {
                v = buffer.get();
                value |= (v & 0x7F) << 14;
                if ((v & 0x80) != 0) {
                    v = buffer.get();
                    value |= (v & 0x7F) << 21;
                    if ((v & 0x80) != 0) {
                        v = buffer.get();
                        value |= (v & 0x7F) << 28;
                        if ((v & 0x80) != 0) {
                            v = buffer.get();
                            value |= (v & 0x7F) << 35;
                            if ((v & 0x80) != 0) {
                                v = buffer.get();
                                value |= (v & 0x7F) << 42;
                                if ((v & 0x80) != 0) {
                                    v = buffer.get();
                                    value |= (v & 0x7F) << 49;
                                    if ((v & 0x80) != 0) {
                                        v = buffer.get();
                                        value |= v << 56;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        value = (value >>> 1) ^ (-(value & 1));
        return value;
    }

    /**
     * Writes an int value to the given buffer in LEB128-64b9B ZigZag encoded format
     *
     * @param buffer the buffer to write to
     * @param value  the value to write to the buffer
     */
    static void putInt(ByteBuffer buffer, int value) {
        value = (value << 1) ^ (value >> 31);
        if (value >>> 7 == 0) {
            buffer.put((byte) value);
        } else {
            buffer.put((byte) ((value & 0x7F) | 0x80));
            if (value >>> 14 == 0) {
                buffer.put((byte) (value >>> 7));
            } else {
                buffer.put((byte) (value >>> 7 | 0x80));
                if (value >>> 21 == 0) {
                    buffer.put((byte) (value >>> 14));
                } else {
                    buffer.put((byte) (value >>> 14 | 0x80));
                    if (value >>> 28 == 0) {
                        buffer.put((byte) (value >>> 21));
                    } else {
                        buffer.put((byte) (value >>> 21 | 0x80));
                        buffer.put((byte) (value >>> 28));
                    }
                }
            }
        }
    }

    /**
     * Writes a long value to the given buffer in LEB128 ZigZag encoded format
     *
     * @param buffer the buffer to write to
     * @param value  the value to write to the buffer
     */
    static void putLong(ByteBuffer buffer, long value) {
        value = (value << 1) ^ (value >> 63);
        if (value >>> 7 == 0) {
            buffer.put((byte) value);
        } else {
            buffer.put((byte) ((value & 0x7F) | 0x80));
            if (value >>> 14 == 0) {
                buffer.put((byte) (value >>> 7));
            } else {
                buffer.put((byte) (value >>> 7 | 0x80));
                if (value >>> 21 == 0) {
                    buffer.put((byte) (value >>> 14));
                } else {
                    buffer.put((byte) (value >>> 14 | 0x80));
                    if (value >>> 28 == 0) {
                        buffer.put((byte) (value >>> 21));
                    } else {
                        buffer.put((byte) (value >>> 21 | 0x80));
                        if (value >>> 35 == 0) {
                            buffer.put((byte) (value >>> 28));
                        } else {
                            buffer.put((byte) (value >>> 28 | 0x80));
                            if (value >>> 42 == 0) {
                                buffer.put((byte) (value >>> 35));
                            } else {
                                buffer.put((byte) (value >>> 35 | 0x80));
                                if (value >>> 49 == 0) {
                                    buffer.put((byte) (value >>> 42));
                                } else {
                                    buffer.put((byte) (value >>> 42 | 0x80));
                                    if (value >>> 56 == 0) {
                                        buffer.put((byte) (value >>> 49));
                                    } else {
                                        buffer.put((byte) (value >>> 49 | 0x80));
                                        buffer.put((byte) (value >>> 56));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
