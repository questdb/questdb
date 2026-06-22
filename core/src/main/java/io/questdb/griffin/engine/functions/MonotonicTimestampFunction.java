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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.sql.Function;
import io.questdb.std.Interval;

/**
 * Implemented by functions that are monotonic in their single designated-timestamp
 * argument, so that a predicate {@code g(ts) OP const} can be turned into a bound
 * on {@code ts} for interval-based partition pruning.
 * <p>
 * The bound is derived by walking the chain of these functions from the outermost
 * wrapper inward to the designated-timestamp column, inverting the queried value
 * range one layer at a time via {@link #invertTimestampInterval(Interval)}.
 */
public interface MonotonicTimestampFunction {

    /**
     * Cannot bound the argument; the predicate must stay a row filter.
     */
    int NONE = 0;
    /**
     * The inverted interval soundly contains the preimage but may include extra
     * timestamps; the predicate must be kept as a residual filter.
     */
    int SUPERSET = 1;
    /**
     * The inverted interval equals the preimage exactly; the predicate may be
     * dropped from the row filter.
     */
    int EXACT = 2;

    /**
     * Returns the argument that carries the timestamp. Every other argument must
     * be constant or runtime-constant.
     */
    Function getTimestampArg();

    /**
     * Inverts a value range of this function's result into a range on its timestamp
     * argument.
     * <p>
     * On entry {@code io} holds the closed interval {@code [lo, hi]} that this
     * function's result must fall in, expressed in this function's output domain.
     * On exit {@code io} holds the interval the timestamp argument must fall in.
     * {@link io.questdb.std.Numbers#LONG_NULL} is the open lower bound and
     * {@link Long#MAX_VALUE} the open upper bound; an empty result is encoded as
     * {@code lo > hi}.
     * <p>
     * The returned grade ({@link #EXACT}, {@link #SUPERSET} or {@link #NONE}) is
     * determined by this function's own constant arguments and does not depend on
     * the bound values, so it can be probed with an unbounded interval.
     */
    int invertTimestampInterval(Interval io);
}
