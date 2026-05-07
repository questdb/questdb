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

package io.questdb.mp.continuation;

import java.util.concurrent.Delayed;

/**
 * Entry registered into a {@link TimerShards} shard. Implementations supply their own
 * deadline via {@link Delayed#getDelay} and route their CAS state machine through
 * {@link #expire()} (deadline pop) and {@link #shutdown()} (engine shutdown drain).
 *
 * <p>Both {@link #expire()} and {@link #shutdown()} must be no-ops if another path has
 * already moved the entry to a terminal state, since a timer shard can race with the
 * data-arrived wakeup path that fires the same entry.
 *
 * <p>Identity-based equality (default {@link Object#equals}) is used: {@link java.util.concurrent.DelayQueue}
 * orders entries by {@link Delayed#compareTo}, never by {@code equals}.
 */
public interface DelayedFireable extends Delayed {
    /**
     * Called by the shard timer thread when this entry's deadline pops. Implementations
     * must route this through their CAS state machine: if another path has already
     * fired or cancelled this entry, this is a no-op.
     */
    void expire();

    /**
     * Called during shard drain on engine shutdown, regardless of deadline. Subject to
     * the same CAS no-op rule as {@link #expire()}: another path may have already
     * moved the entry to a terminal state.
     */
    void shutdown();
}