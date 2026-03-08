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

package io.questdb.cairo.sql;

import io.questdb.std.Mutable;
import io.questdb.std.Rows;

public final class TimeFrame implements Mutable {
    private long estimateTimestampHi;
    private long estimateTimestampLo;
    private int frameIndex;
    private long rowHi;
    private long rowLo;
    private long timestampHi;
    private long timestampLo;

    @Override
    public void clear() {
        frameIndex = -1;
        estimateTimestampLo = Long.MIN_VALUE;
        estimateTimestampHi = Long.MIN_VALUE;
        timestampLo = Long.MIN_VALUE;
        timestampHi = Long.MIN_VALUE;
        rowLo = -1;
        rowHi = -1;
    }

    /**
     * @return numeric index of the current time frame.
     */
    public int getFrameIndex() {
        return frameIndex;
    }

    /**
     * Make sure to call {@link TimeFrameCursor#open()} prior to this method.
     * <p>
     * Note: it's allowed to jump with the {@link TimeFrameCursor} record
     * to any row id in the [rowLo, rowHi) range. {@link Rows#toRowID(int, long)}
     * method should be used to obtain final row id.
     *
     * @return upper boundary for row number within the frame, exclusive
     */
    public long getRowHi() {
        return rowHi;
    }

    /**
     * Make sure to call {@link TimeFrameCursor#open()} prior to this method.
     * <p>
     * Note: it's allowed to jump with the {@link TimeFrameCursor} record
     * to any row id in the [rowLo, rowHi) range. {@link Rows#toRowID(int, long)}
     * method should be used to obtain final row id.
     *
     * @return lower boundary for row number within the frame, inclusive
     */
    public long getRowLo() {
        return rowLo;
    }

    /**
     * Unlike {@link #getTimestampHi()} this value is not precise, i.e. it may
     * be larger than the actual boundary. Yet, it doesn't require the frame
     * to be open.
     * <p>
     * Note that estimated timestamp intervals of subsequent time frames may
     * intersect.
     *
     * @return upper range boundary of timestamps in the time frame, exclusive
     */
    public long getTimestampEstimateHi() {
        return estimateTimestampHi;
    }

    /**
     * Unlike {@link #getTimestampLo()} this value is not precise, i.e. it may
     * be smaller than the actual boundary. Yet, it doesn't require the frame
     * to be open.
     * <p>
     * Note that estimated timestamp intervals of subsequent time frames may
     * intersect.
     *
     * @return lower range boundary of timestamps in the time frame, inclusive
     */
    public long getTimestampEstimateLo() {
        return estimateTimestampLo;
    }

    /**
     * Make sure to call {@link TimeFrameCursor#open()} prior to this method.
     *
     * @return upper boundary of timestamps present in the time frame, exclusive
     */
    public long getTimestampHi() {
        return timestampHi;
    }

    /**
     * Make sure to call {@link TimeFrameCursor#open()} prior to this method.
     *
     * @return lower boundary of timestamps present in the time frame, inclusive
     */
    public long getTimestampLo() {
        return timestampLo;
    }

    /**
     * @return whether the frame was previously open
     */
    public boolean isOpen() {
        return rowLo != -1 && rowHi != -1;
    }

    public void ofEstimate(int frameIndex, long estimateTimestampLo, long estimateTimestampHi) {
        this.frameIndex = frameIndex;
        this.estimateTimestampLo = estimateTimestampLo;
        this.estimateTimestampHi = estimateTimestampHi;
        timestampLo = Long.MIN_VALUE;
        timestampHi = Long.MIN_VALUE;
        rowLo = -1;
        rowHi = -1;
    }

    public void ofOpen(
            long timestampLo,
            long timestampHi,
            long rowLo,
            long rowHi
    ) {
        this.timestampLo = timestampLo;
        this.timestampHi = timestampHi;
        this.rowLo = rowLo;
        this.rowHi = rowHi;
    }
}
