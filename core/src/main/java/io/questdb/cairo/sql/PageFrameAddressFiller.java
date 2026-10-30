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

package io.questdb.cairo.sql;

/**
 * Computes the column addresses of a native page frame whose frame cursor handed it out without
 * them (see {@link PageFrameCursor#deferFrameAddresses()}), into the frame's entry of a
 * {@link PageFrameAddressCache}. The cache calls it the first time any thread navigates to the
 * frame, see {@link PageFrameAddressCache#ensureFrameReady(int)}, so the per-frame column work runs
 * on the threads that process the frames rather than on the query's thread that enumerates them.
 * <p>
 * It must be safe to call from any thread, concurrently for different frames, while the frame
 * cursor's partitions stay open: implementations read only state that the enumeration left fixed.
 */
public interface PageFrameAddressFiller {

    /**
     * Writes, through {@link PageFrameAddressCache#setFrameColumnAddresses}, the addresses and sizes of
     * every column of the native frame of rows {@code [partitionLo, partitionHi)} of partition
     * {@code partitionIndex}, at cache frame {@code frameIndex}: the values the frame would have
     * reported had its cursor computed them eagerly.
     */
    void fillFrameAddresses(int partitionIndex, long partitionLo, long partitionHi, PageFrameAddressCache cache, int frameIndex);
}
