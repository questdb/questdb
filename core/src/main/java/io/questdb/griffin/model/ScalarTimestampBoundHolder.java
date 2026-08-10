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
 * share this same holder by reference - snapshots that one frozen value in its own {@code init()}.
 * Both the publish and every snapshot run on the frame-open thread, before the async filter dispatches
 * to workers, so no row loop touches this holder; {@code volatile} is retained as a cheap guard for any
 * future reader that is not init()-scoped.
 * <p>
 * The {@code published} flag is a per-execution tripwire, not a one-shot latch: the owner disarms it
 * via {@link #reset()} at the top of its {@code init()} and re-arms it in {@link #publish(long)} once
 * the fresh value is in place. Without the disarm the flag would stay {@code true} forever after the
 * first execution, so a reused/cached factory that stopped publishing would silently serve the
 * previous execution's bound instead of tripping the assertion in the residual reader.
 */
public class ScalarTimestampBoundHolder {
    private final int timestampType;
    // Per-execution, not write-once: reset() disarms it at the top of the owner's init() so the
    // residual-side assertion stays live on executions 2..N of a reused/cached factory.
    private volatile boolean published;
    private volatile long value = Numbers.LONG_NULL;

    public ScalarTimestampBoundHolder(int timestampType) {
        this.timestampType = timestampType;
    }

    public int getTimestampType() {
        return timestampType;
    }

    // Deliberately carries no determinism flag: the only prospective reader is the residual-side
    // ScalarSubQueryBoundRefFunction, and forwarding the sub-query factory's fail-safe
    // RecordCursorFactory#isNonDeterministic() hint into the fail-open Function legality flag makes
    // the materialized-view guard reject valid DDL (see the polarity note in
    // ScalarSubQueryBoundRefFunction).

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

    /**
     * Called by the owning pruning bound at the top of {@code init()}, before the sub-query is
     * re-opened. Runs on the frame-open path (single-threaded, and after {@code frameSequence.await()}
     * has drained any worker reads from the previous open), so it cannot race a residual reader.
     * <p>
     * Deliberately does not clear {@code value}: leaving the stale value in place keeps a missed
     * publish observable as an assertion failure rather than as a silent {@code NULL} bound on a
     * build with assertions disabled.
     */
    public void reset() {
        this.published = false;
    }

    public long value() {
        return value;
    }
}
