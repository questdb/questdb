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

import io.questdb.cairo.CairoException;
import io.questdb.std.Unsafe;

/** Stable Java view of the fixed ABI header in an opaque Rust partition-state handle. */
public final class PartitionFrameState {
    private static final int HEADER_HAS_CUSTOM_FRAMES = 3;
    private static final int HEADER_LOGICAL_ROWS = 2;
    private static final int HEADER_SUBFRAME_SIZE = 4;
    private static final int HEADER_SUMMARIES_ADDR = 0;
    private static final int HEADER_WINDOW_COUNT = 1;
    public static final int SUMMARY_BASE_ROWS = 0;
    public static final int SUMMARY_FLAGS = 3;
    public static final int SUMMARY_LOGICAL_ROWS = 2;
    public static final int SUMMARY_LONGS = 4;
    public static final long WINDOW_HAS_DELTA = 1;

    private PartitionFrameState() {
    }

    public static long getBaseRowCount(long state, int window) {
        return summaryValue(state, window, SUMMARY_BASE_ROWS);
    }

    public static long getLogicalPartitionRowCount(long state) {
        return headerValue(state, HEADER_LOGICAL_ROWS);
    }

    public static long getLogicalRowCount(long state, int window) {
        return summaryValue(state, window, SUMMARY_LOGICAL_ROWS);
    }

    public static long getSubframeSize(long state) {
        return headerValue(state, HEADER_SUBFRAME_SIZE);
    }

    public static int getWindowCount(long state) {
        return Math.toIntExact(headerValue(state, HEADER_WINDOW_COUNT));
    }

    public static boolean hasCustomFrames(long state) {
        return headerValue(state, HEADER_HAS_CUSTOM_FRAMES) != 0;
    }

    public static boolean requiresMaterialization(long state, int window) {
        return (summaryValue(state, window, SUMMARY_FLAGS) & WINDOW_HAS_DELTA) != 0;
    }

    private static long headerValue(long state, int cell) {
        if (state == 0) {
            throw CairoException.critical(0).put("partition frame state pointer is null");
        }
        return Unsafe.getLong(state + (long) cell * Long.BYTES);
    }

    private static long summaryValue(long state, int window, int cell) {
        final long summariesAddr = headerValue(state, HEADER_SUMMARIES_ADDR);
        final int windowCount = getWindowCount(state);
        if (summariesAddr == 0 || window < 0 || window >= windowCount) {
            throw CairoException.critical(0)
                    .put("invalid partition frame state summary index [window=").put(window)
                    .put(", windowCount=").put(windowCount).put(']');
        }
        return Unsafe.getLong(
                summariesAddr + ((long) window * SUMMARY_LONGS + cell) * Long.BYTES
        );
    }
}
