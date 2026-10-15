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

package io.questdb.metrics;

import io.questdb.std.str.BorrowableUtf8Sink;

/**
 * Anything that can be scraped for Prometheus metrics.
 */
public interface Target {

    /**
     * Visits this target's metrics without serializing them to Prometheus text. Targets that only
     * perform scrape-time bookkeeping may keep the default no-op implementation.
     * <p>
     * Concurrency contract: an implementation must be safe against concurrent invocation. A target
     * that populates a shared/instance buffer (e.g. a reused native snapshot buffer) must serialize
     * those writes itself -- for example by declaring the method {@code synchronized}, as
     * {@code ColdStorageMetrics}, {@code WalUploader} and {@code WalDownloader} do -- because
     * {@code MetricsRegistryImpl.snapshot} imposes no ordering across targets or callers.
     */
    default void snapshot(MetricSnapshotVisitor visitor) {
    }

    // We need a sink that we can borrow from and append to in native code.
    void scrapeIntoPrometheus(BorrowableUtf8Sink sink);
}
