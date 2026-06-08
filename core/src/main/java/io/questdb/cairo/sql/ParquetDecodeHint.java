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
 * Hint declared by a random-access factory describing how it walks the base
 * cursor's rowIds. The Parquet decode-buffer pool scales its byte budget by
 * the pattern's factor: monotonic walks visit each frame at most once, so a
 * smaller working set fits; scattered walks may revisit frames in any order
 * and warrant the full configured budget.
 */
public enum ParquetDecodeHint {
    MONOTONIC(2, 4),
    SCATTERED(0, 256);

    final int maxCachedBuffers;
    private final int rightShift;

    ParquetDecodeHint(int rightShift, int maxCachedBuffers) {
        this.rightShift = rightShift;
        this.maxCachedBuffers = maxCachedBuffers;
    }

    public long applyTo(long configuredBytes) {
        return configuredBytes >>> rightShift;
    }
}
