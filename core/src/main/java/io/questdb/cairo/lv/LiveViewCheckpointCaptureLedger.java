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
 * What one publication's freeze actually walked, split by the root kind that walked it.
 * <p>
 * Nothing in the published artifacts separates an incremental capture from a complete
 * one: both leave a root naming the whole live key domain, because the incremental one
 * keeps every key it did not touch from its predecessor. Timing does not separate them
 * either - a complete walk of a small domain beats an incremental walk that had to map an
 * older segment to compare against. So the structural claim that a seal costs the keys the
 * batch changed rather than the keys the view holds needs a ledger of its own, and this is
 * it.
 * <p>
 * The window root and the function roots are counted apart because they are the two halves
 * the layout-removal change moves state between: one window root replaces the anchor root
 * and every durable projection's root at once, and what stays on a root of its own is a
 * residual function or a component the inline leaf budget left out. A reading that pooled
 * them could not tell a seal that fused its state from one that did not.
 * <p>
 * {@code keysVisited} counts <b>walks</b> rather than roots on the function side: one seal
 * shares a single walk of the group's map across every runtime-only member that agrees on
 * the incremental disposition, so a wide SELECT list adds roots and images without adding
 * walks. {@code captures} counts roots either way.
 * <p>
 * A mutable accumulator, owned by the freeze scratch the operation is bound to rather than
 * by {@link LiveViewCheckpointTimelineStoreWriter} itself: a suspended repair holds its own
 * scratch across refresh turns, and a writer-level ledger would be cleared out from under it
 * by any cadence seal that worker ran in between. Binding the scratch clears it, each freeze
 * charges into it, and the refresh worker copies it onto the view's {@link LiveViewInstance}
 * once the publication is durable. Not thread-safe, and valid only until its scratch is
 * bound again.
 */
public final class LiveViewCheckpointCaptureLedger {

    private long functionCaptures;
    private long functionIncrementalCaptures;
    private long functionKeysImaged;
    private long functionKeysVisited;
    private long windowCaptures;
    private long windowIncrementalCaptures;
    private long windowKeysImaged;
    private long windowKeysRemoved;
    private long windowKeysVisited;

    /**
     * Charges one function root's capture.
     *
     * @param keysVisited rows the walk that produced this root read, or 0 when the root
     *                    shares a walk another member already charged
     * @param keysImaged  keys this root published an image for, which excludes the keys a
     *                    partial key domain left to the predecessor's entries
     */
    public void addFunctionCapture(boolean isIncremental, long keysVisited, long keysImaged) {
        functionCaptures++;
        if (isIncremental) {
            functionIncrementalCaptures++;
        }
        functionKeysVisited += keysVisited;
        functionKeysImaged += keysImaged;
    }

    /**
     * Charges one window root's capture.
     *
     * @param keysVisited rows the walk read - the dirty map's for an incremental capture,
     *                    the whole anchor map's for a complete one
     * @param keysImaged  keys this root published an entry for
     * @param keysRemoved keys the frontier sweep dropped, which an incremental capture has
     *                    to name because the root it builds on still holds their entries
     */
    public void addWindowCapture(boolean isIncremental, long keysVisited, long keysImaged, long keysRemoved) {
        windowCaptures++;
        if (isIncremental) {
            windowIncrementalCaptures++;
        }
        windowKeysVisited += keysVisited;
        windowKeysImaged += keysImaged;
        windowKeysRemoved += keysRemoved;
    }

    public void clear() {
        windowCaptures = 0;
        windowIncrementalCaptures = 0;
        windowKeysVisited = 0;
        windowKeysImaged = 0;
        windowKeysRemoved = 0;
        functionCaptures = 0;
        functionIncrementalCaptures = 0;
        functionKeysVisited = 0;
        functionKeysImaged = 0;
    }

    public long getFunctionCaptures() {
        return functionCaptures;
    }

    public long getFunctionIncrementalCaptures() {
        return functionIncrementalCaptures;
    }

    public long getFunctionKeysImaged() {
        return functionKeysImaged;
    }

    public long getFunctionKeysVisited() {
        return functionKeysVisited;
    }

    public long getWindowCaptures() {
        return windowCaptures;
    }

    public long getWindowIncrementalCaptures() {
        return windowIncrementalCaptures;
    }

    public long getWindowKeysImaged() {
        return windowKeysImaged;
    }

    public long getWindowKeysRemoved() {
        return windowKeysRemoved;
    }

    public long getWindowKeysVisited() {
        return windowKeysVisited;
    }
}
