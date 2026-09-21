/*+*****************************************************************************
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

package io.questdb.cairo.lv;

import org.jetbrains.annotations.NotNull;

/**
 * Hands one partition's persisted ring back to the function that owns it. The
 * rows arrive in the same designated-timestamp order the seal streamed them in,
 * spliced back together from however many chunk pages the root shares with its
 * neighbours - the function sees one ring and never the chunk boundaries.
 *
 * @see LiveViewCheckpointRingStateSink
 */
public interface LiveViewCheckpointRingStateSource {

    /**
     * Replays every live ring row in designated-timestamp order. A malformed
     * chunk page invalidates the root here rather than at open time: the root's
     * metadata is validated eagerly, its payload lazily.
     * <p>
     * Reads a one-word ring. A function always replays the width it sealed under,
     * so calling the wrong overload is a wiring error and fails fast.
     */
    void forEachRow(@NotNull RowConsumer consumer);

    /**
     * Replays a two-word (128-bit decimal) ring. See {@link #forEachRow(RowConsumer)}.
     */
    void forEachRow(@NotNull Decimal128RowConsumer consumer);

    /**
     * Replays a four-word (256-bit decimal) ring. See {@link #forEachRow(RowConsumer)}.
     */
    void forEachRow(@NotNull Decimal256RowConsumer consumer);

    /**
     * Replays a valueless ring, whose rows are designated timestamps alone - the shape
     * {@code count} keeps. See {@link #forEachRow(RowConsumer)}.
     */
    void forEachTimestamp(@NotNull TimestampConsumer consumer);

    /**
     * @return the number of ring rows the stored scalar covers
     */
    long getFrameSize();

    /**
     * @return the number of live ring rows {@link #forEachRow} will replay
     */
    long getRowCount();

    /**
     * @return the first word of the stored scalar continuation state, equivalent to
     * {@code getScalarWord(0)}. The function reinterprets the bits: a DOUBLE ring
     * reads them as IEEE-754, every other ring as a raw payload
     */
    long getScalarBits();

    /**
     * @return one word of the exact stored scalar continuation state, by the raw bits
     * the seal captured (the running aggregate for avg/sum, the emitted frame value for
     * first_value/last_value/nth_value). Word 0 is the most significant word of a
     * multi-word scalar, so a 256-bit decimal accumulator arrives as
     * {@code (hh, hl, lh, ll)}
     */
    long getScalarWord(int index);

    /**
     * @return the words the stored scalar continuation state occupies: 1, 2 or 4
     */
    int getScalarWordCount();

    @FunctionalInterface
    interface Decimal128RowConsumer {
        /**
         * Receives one ring row's 128-bit decimal value, most significant word first.
         */
        void accept(long timestamp, long hi, long lo);
    }

    @FunctionalInterface
    interface Decimal256RowConsumer {
        /**
         * Receives one ring row's 256-bit decimal value, most significant word first.
         */
        void accept(long timestamp, long hh, long hl, long lh, long ll);
    }

    @FunctionalInterface
    interface RowConsumer {
        /**
         * Receives one ring row's raw 64-bit value bits - IEEE-754 bits for a DOUBLE
         * ring, the raw payload for a LONG/DATE/TIMESTAMP or narrow DECIMAL ring.
         */
        void accept(long timestamp, long valueBits);
    }

    @FunctionalInterface
    interface TimestampConsumer {
        /**
         * Receives one valueless ring row, which is its designated timestamp.
         */
        void accept(long timestamp);
    }
}
