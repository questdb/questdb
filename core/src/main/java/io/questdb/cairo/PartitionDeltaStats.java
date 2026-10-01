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

package io.questdb.cairo;

/** Logical row count and Delta-only timestamp bounds at one partition snapshot. */
public final class PartitionDeltaStats {
    private long maxTimestamp;
    private long minTimestamp;
    private long rowCount;

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getRowCount() {
        return rowCount;
    }

    /** Empty Delta bounds are {@code (Long.MAX_VALUE, Long.MIN_VALUE)}. */
    public void of(long rowCount, long minTimestamp, long maxTimestamp) {
        this.rowCount = rowCount;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }
}
