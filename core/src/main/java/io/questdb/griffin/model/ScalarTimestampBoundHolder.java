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

package io.questdb.griffin.model;

import io.questdb.std.Numbers;

/**
 * Shared, execution-scoped carrier for the single value of a scalar sub-query designated-timestamp
 * bound (e.g. {@code ts >= (SELECT max(lo) FROM b)}).
 * <p>
 * A monotonic-timestamp predicate whose bound is a scalar sub-query is used in two places at once:
 * the interval-pruning inverter (kept in {@link RuntimeIntervalModel}) and the retained residual row
 * filter. Historically each side compiled and opened the sub-query independently, so a commit landing
 * on the sub-query's table between the two opens could make the pruning bound stricter than the
 * residual and silently drop qualifying rows.
 * <p>
 * With this holder the sub-query is evaluated exactly once per outer execution: the pruning bound
 * (the owner) publishes its computed value here in {@code init()}, which runs at partition-frame open
 * before any row is filtered, and every residual reader - including per-worker filter clones, which
 * share this same holder by reference - reads that one frozen value. Publishing is a single write on
 * the frame-open path that happens-before the reads dispatched to worker threads; {@code volatile}
 * makes the frozen value visible to those readers.
 */
public class ScalarTimestampBoundHolder {
    private final boolean nonDeterministic;
    private final int timestampType;
    private volatile boolean published;
    private volatile long value = Numbers.LONG_NULL;

    public ScalarTimestampBoundHolder(int timestampType, boolean nonDeterministic) {
        this.timestampType = timestampType;
        this.nonDeterministic = nonDeterministic;
    }

    public int getTimestampType() {
        return timestampType;
    }

    /**
     * Mirrors the wrapped sub-query factory's determinism so the residual filter that reads this
     * holder keeps the exact framing/optimization behaviour the direct sub-query bound had.
     */
    public boolean isNonDeterministic() {
        return nonDeterministic;
    }

    public boolean isPublished() {
        return published;
    }

    /**
     * Called by the owning pruning bound in {@code init()} (once per outer execution). Overwrites the
     * previous execution's value so a reused/cached factory re-freezes on every run.
     */
    public void publish(long value) {
        this.value = value;
        this.published = true;
    }

    public long value() {
        return value;
    }
}
