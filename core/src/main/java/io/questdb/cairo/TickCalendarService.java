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

package io.questdb.cairo;

import io.questdb.std.LongList;
import org.jetbrains.annotations.Nullable;

/**
 * Service providing exchange trading schedules for use in TICK interval expressions.
 * <p>
 * The schedule is returned as a {@link LongList} of [lo, hi] timestamp pairs representing
 * trading hours. This can be intersected with query intervals using
 * {@link io.questdb.griffin.model.IntervalUtils#intersectInPlace}.
 */
public interface TickCalendarService {

    /**
     * Returns the trading schedule for the specified exchange.
     * <p>
     * The returned {@link LongList} contains pairs of timestamps [lo, hi] representing
     * trading sessions. Timestamps are in microseconds since epoch (UTC).
     * <p>
     * The schedule covers all known trading days (past and future) for the exchange.
     * Weekends and holidays are excluded.
     *
     * @param exchange the exchange identifier (e.g., "NYSE", "LSE"), case-insensitive
     * @return the trading schedule as interval pairs, or null if exchange is not found
     */
    @Nullable
    LongList getSchedule(CharSequence exchange);
}
