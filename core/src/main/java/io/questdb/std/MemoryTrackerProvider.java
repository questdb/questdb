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

package io.questdb.std;

import io.questdb.cairo.SecurityContext;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Per-engine source of {@link MemoryTracker} instances. The OSS default returns
 * a per-workload tracker from a pool; an enterprise build can return a tracker
 * shared across all workloads of the same principal.
 * <p>
 * The provider is constructed once per {@code CairoEngine} and its
 * {@link #close()} is invoked from {@code CairoEngine.close()} to drain the
 * pool and release every retained native block.
 */
public interface MemoryTrackerProvider extends Closeable, Mutable {

    /**
     * Returns a tracker bound to a single workload invocation. The caller owns
     * the returned tracker and must {@link MemoryTracker#close() close} it
     * exactly once when the workload ends.
     *
     * @param securityContext the principal driving the workload; consumed by
     *                        the enterprise provider to look up a per-principal
     *                        quota node, ignored by the OSS provider.
     * @param queryId         identifier carried in the error message on a limit
     *                        breach. For {@code QUERY} it is the query registry
     *                        id (matches {@code query_activity.query_id}); for
     *                        {@code MAT_VIEW_REFRESH} and {@code WAL_APPLY} it is
     *                        the target table id.
     * @param workload        the workload class; selects which configured
     *                        limit applies.
     */
    @NotNull
    MemoryTracker acquire(@NotNull SecurityContext securityContext, long queryId, @NotNull MemoryTrackerWorkload workload);

    /**
     * Drains any cached state without closing the provider. Used by
     * {@code CairoEngine.clear()} so the leak-checker in test infrastructure
     * does not flag the provider's retained native blocks as a residual after
     * a test that exercised a workload boundary. The default implementation is
     * a no-op so non-pooling providers (e.g., the enterprise shared-tracker
     * provider, whose lifetimes follow refcount drops) need no change.
     */
    default void clear() {
    }

    @Override
    void close();
}
