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
 * Gorilla delta-of-delta decoder for timestamps in QWP v1 format.
 * <p>
 * Gorilla encoding uses delta-of-delta compression where:
 * <pre>
 * D = (t[n] - t[n-1]) - (t[n-1] - t[n-2])
 *
 * if D == 0:              write '0'              (1 bit)
 * elif D in [-64, 63]:    write '10' + 7-bit     (9 bits)
 * elif D in [-256, 255]:  write '110' + 9-bit    (12 bits)
 * elif D in [-2048, 2047]: write '1110' + 12-bit (16 bits)
 * else:                   write '1111' + 32-bit  (36 bits)
 * </pre>
 * <p>
 * The decoder reads bit-packed delta-of-delta values and reconstructs
 * the original timestamp sequence.
 */
public class QwpGorillaDecoder {

    private static final int BUCKET_12BIT_MAX = 2047;
    private static final int BUCKET_12BIT_MIN = -2048;
    private static final int BUCKET_7BIT_MAX = 63;
    // Bucket boundaries (two's complement signed ranges)
    private static final int BUCKET_7BIT_MIN = -64;
    private static final int BUCKET_9BIT_MAX = 255;
    private static final int BUCKET_9BIT_MIN = -256;
    private final QwpBitReader bitReader;
    private int decodeCount;
    private long prevDelta;
    // State for decoding
    private long prevTimestamp;

    /**
     * Creates a new Gorilla decoder.
     */
    public QwpGorillaDecoder() {
        this.bitReader = new QwpBitReader();
    }

    /**
     * Returns the number of bits required to encode a delta-of-delta value.
     *
     * @param deltaOfDelta the delta-of-delta value
     * @return bits required
     */
    public static int getBitsRequired(long deltaOfDelta) {
        int bucket = getBucket(deltaOfDelta);
        return switch (bucket) {
            case 0 -> 1;
            case 1 -> 9;
            case 2 -> 12;
            case 3 -> 16;
            default -> 36;
        };
    }

    /**
     * Determines which bucket a delta-of-delta value falls into.
     *
     * @param deltaOfDelta the delta-of-delta value
     * @return bucket number (0 = 1-bit, 1 = 9-bit, 2 = 12-bit, 3 = 16-bit, 4 = 36-bit)
     */
    public static int getBucket(long deltaOfDelta) {
        if (deltaOfDelta == 0) {
            return 0; // 1-bit
        } else if (deltaOfDelta >= BUCKET_7BIT_MIN && deltaOfDelta <= BUCKET_7BIT_MAX) {
            return 1; // 9-bit (2 prefix + 7 value)
        } else if (deltaOfDelta >= BUCKET_9BIT_MIN && deltaOfDelta <= BUCKET_9BIT_MAX) {
            return 2; // 12-bit (3 prefix + 9 value)
        } else if (deltaOfDelta >= BUCKET_12BIT_MIN && deltaOfDelta <= BUCKET_12BIT_MAX) {
            return 3; // 16-bit (4 prefix + 12 value)
        } else {
            return 4; // 36-bit (4 prefix + 32 value)
        }
    }

    /**
     * Decodes the next timestamp from the bit stream.
     * <p>
     * The encoding format is:
     * <ul>
     *   <li>'0' = delta-of-delta is 0 (1 bit)</li>
     *   <li>'10' + 7-bit signed = delta-of-delta in [-64, 63] (9 bits)</li>
     *   <li>'110' + 9-bit signed = delta-of-delta in [-256, 255] (12 bits)</li>
     *   <li>'1110' + 12-bit signed = delta-of-delta in [-2048, 2047] (16 bits)</li>
     *   <li>'1111' + 32-bit signed = any other delta-of-delta (36 bits)</li>
     * </ul>
     *
     * @return the decoded timestamp
     * @throws QwpParseException if decoding fails
     */
    public long decodeNext() throws QwpParseException {
        long deltaOfDelta = decodeDoD();
        long delta = prevDelta + deltaOfDelta;
        long timestamp = prevTimestamp + delta;

        prevDelta = delta;
        prevTimestamp = timestamp;
        decodeCount++;

        return timestamp;
    }

    /**
     * Returns the number of values decoded since the last {@link #reset}.
     *
     * @return decode count
     */
    public int getDecodeCount() {
        return decodeCount;
    }

    /**
     * Returns the number of bits remaining.
     *
     * @return available bits
     */
    public long getAvailableBits() {
        return bitReader.getAvailableBits();
    }

    /**
     * Returns the current bit position (bits read since reset).
     *
     * @return bits read
     */
    public long getBitPosition() {
        return bitReader.getBitPosition();
    }

    /**
     * Resets the decoder with the first two timestamps.
     * <p>
     * The first two timestamps are always stored uncompressed and are used
     * to establish the initial delta for subsequent compression.
     *
     * @param firstTimestamp  the first timestamp in the sequence
     * @param secondTimestamp the second timestamp in the sequence
     */
    public void reset(long firstTimestamp, long secondTimestamp) {
        this.prevTimestamp = secondTimestamp;
        this.prevDelta = secondTimestamp - firstTimestamp;
    }

    /**
     * Resets the bit reader for reading encoded delta-of-deltas.
     *
     * @param address the address of the encoded data
     * @param length  the length of the encoded data in bytes
     */
    public void resetReader(long address, long length) {
        bitReader.reset(address, length);
    }

    /**
     * Decodes a delta-of-delta value from the bit stream.
     *
     * @return the delta-of-delta value
     * @throws QwpParseException if not enough bits available
     */
    private long decodeDoD() throws QwpParseException {
        int bit = bitReader.readBit();

        if (bit == 0) {
            // '0' = DoD is 0
            return 0;
        }

        // bit == 1, check next bit
        bit = bitReader.readBit();
        if (bit == 0) {
            // '10' = 7-bit signed value
            return bitReader.readSigned(7);
        }

        // '11', check next bit
        bit = bitReader.readBit();
        if (bit == 0) {
            // '110' = 9-bit signed value
            return bitReader.readSigned(9);
        }

        // '111', check next bit
        bit = bitReader.readBit();
        if (bit == 0) {
            // '1110' = 12-bit signed value
            return bitReader.readSigned(12);
        }

        // '1111' = 32-bit signed value
        return bitReader.readSigned(32);
    }
}
