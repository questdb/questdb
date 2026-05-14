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

package io.questdb.cairo.wal;

import io.questdb.cairo.TableToken;

/**
 * SPI for components that pin specific WAL segments against deletion. Consulted
 * by {@link WalPurgeJob} on every segment-delete decision: a segment is
 * preserved if any registered predicate returns true.
 * <p>
 * The default implementation returns {@code false} for everything, which
 * preserves the pre-SPI purge behaviour. Components that need to hold a
 * segment back register their own implementation via
 * {@link io.questdb.cairo.CairoEngine#setSegmentPurgeBlocker}.
 * <p>
 * Threading: implementations must be thread-safe. The purge job runs on its
 * own scheduled thread; predicate registrations happen during server startup
 * and component lifecycle. Implementations are queried under a per-WAL
 * exclusive lock so the answer is observed at the moment the purge decision
 * is made; a "true" answer means the purge job skips this segment for the
 * current cycle and re-checks on the next cycle.
 */
public interface SegmentPurgeBlocker {

    /**
     * No-op blocker. Always returns {@code false}; the purge job behaves as
     * if no blockers are registered.
     */
    SegmentPurgeBlocker DEFAULT = (token, walId, segmentId) -> false;

    /**
     * @param tableToken the table whose WAL contains the segment under
     *                   consideration for purge
     * @param walId      the WAL directory id
     * @param segmentId  the segment id within the WAL directory
     * @return {@code true} to block deletion of this segment for the current
     * purge cycle; {@code false} to allow deletion to proceed (subject to
     * other purge-side checks such as apply progress and writer lock)
     */
    boolean isSegmentInUse(TableToken tableToken, int walId, int segmentId);
}
