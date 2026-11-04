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
 * Freeze work by root kind. Captures count roots; keysVisited counts map walks, which
 * runtime-only functions may share. keysImaged and elisionProbes remain separate:
 * imaging a changed key need not probe its predecessor.
 * <p>
 * The operation's scratch owns this mutable ledger, so a parked repair keeps its
 * counts across unrelated seals. Binding clears it; each freeze adds work, and a
 * durable publication records it on the instance. Not thread-safe.
 */
public final class LiveViewCheckpointCaptureLedger {

    private long functionCaptures;
    private long functionIncrementalCaptures;
    private long functionKeysImaged;
    private long functionKeysVisited;
    private long windowCaptures;
    private long windowElisionProbes;
    private long windowIncrementalCaptures;
    private long windowKeysImaged;
    private long windowKeysRemoved;
    private long windowKeysVisited;

    /**
     * Adds every reading of {@code other} to this ledger's, which is how a view keeps its
     * lifetime totals.
     */
    public void add(@NotNull LiveViewCheckpointCaptureLedger other) {
        functionCaptures += other.functionCaptures;
        functionIncrementalCaptures += other.functionIncrementalCaptures;
        functionKeysImaged += other.functionKeysImaged;
        functionKeysVisited += other.functionKeysVisited;
        windowCaptures += other.windowCaptures;
        windowElisionProbes += other.windowElisionProbes;
        windowIncrementalCaptures += other.windowIncrementalCaptures;
        windowKeysImaged += other.windowKeysImaged;
        windowKeysRemoved += other.windowKeysRemoved;
        windowKeysVisited += other.windowKeysVisited;
    }

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
     * @param keysVisited   rows the walk read - the dirty map's for an incremental capture,
     *                      the whole anchor map's for a complete one
     * @param keysImaged    keys this root published an entry for
     * @param keysRemoved   keys the frontier sweep dropped, which an incremental capture has
     *                      to name because the root it builds on still holds their entries
     * @param elisionProbes predecessor entries this capture looked up to decide whether it
     *                      could leave the predecessor's entry standing. It is the reading
     *                      the unchanged-entry elision is paid for in: keys the freeze
     *                      already knew it could not elide are skipped, so this sits at
     *                      zero for a capture whose every imaged key crossed an anchor
     *                      boundary and at the imaged count for one whose anchors held
     */
    public void addWindowCapture(
            boolean isIncremental,
            long keysVisited,
            long keysImaged,
            long keysRemoved,
            long elisionProbes
    ) {
        windowCaptures++;
        if (isIncremental) {
            windowIncrementalCaptures++;
        }
        windowKeysVisited += keysVisited;
        windowKeysImaged += keysImaged;
        windowKeysRemoved += keysRemoved;
        windowElisionProbes += elisionProbes;
    }

    public void clear() {
        windowCaptures = 0;
        windowIncrementalCaptures = 0;
        windowKeysVisited = 0;
        windowKeysImaged = 0;
        windowKeysRemoved = 0;
        windowElisionProbes = 0;
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

    public long getWindowElisionProbes() {
        return windowElisionProbes;
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
