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

/**
 * Receives one partition's bounded-frame ring while a checkpoint seals it.
 * <p>
 * The function streams its complete live ring in designated-timestamp order and
 * says nothing about which rows are new. The seal decides that: rows at or below
 * the previous boundary are already encoded in the pages that boundary's root
 * published, so it reuses those pages by reference and encodes only the rows
 * above it. That is what makes a seal cost the rows the batch added rather than
 * the whole frame.
 *
 * @see LiveViewCheckpointRingStateSource
 */
public interface LiveViewCheckpointRingStateSink {

    /**
     * Records the exact scalar continuation state, restored verbatim rather than
     * recomputed so it cannot drift from the one the runtime carried: the running
     * aggregate for {@code avg}/{@code sum}, or the frame value {@code first_value}/
     * {@code last_value}/{@code nth_value} emits. Called once per partition, before
     * the first {@link #putRow}. May be non-finite - a base first/last/nth value
     * over a NULL row is NaN.
     *
     * @param scalar    the function's scalar continuation state, by raw bits
     * @param frameSize the number of rows the scalar covers. It is the function's
     *                  own cardinality, not a ring index: a frame whose high bound
     *                  trails the current row covers a prefix of the ring, and one
     *                  whose low bound is unbounded covers rows the ring has already
     *                  expired
     */
    void putScalarState(double scalar, long frameSize);

    /**
     * Appends one live ring row. Timestamps must not decrease across a
     * partition's stream.
     */
    void putRow(long timestamp, double value);
}
