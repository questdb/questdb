/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.Rows;

public interface TimeFrame {

    /**
     * @return numeric index of the current time frame.
     */
    int getFrameIndex();

    /**
     * Make sure to call {@link TimeFrameRecordCursor#open()} prior to this method.
     * <p>
     * Note: it's allowed to jump with the {@link TimeFrameRecordCursor} record
     * to any row id in the [rowLo, rowHi) range. {@link Rows#toRowID(int, long)}
     * method should be used to obtain final row id.
     *
     * @return upper boundary for row number within the frame, exclusive
     */
    long getRowHi();

    /**
     * Make sure to call {@link TimeFrameRecordCursor#open()} prior to this method.
     * <p>
     * Note: it's allowed to jump with the {@link TimeFrameRecordCursor} record
     * to any row id in the [rowLo, rowHi) range. {@link Rows#toRowID(int, long)}
     * method should be used to obtain final row id.
     *
     * @return lower boundary for row number within the frame, inclusive
     */
    long getRowLo();

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
    long getTimestampEstimateHi();

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
    long getTimestampEstimateLo();

    /**
     * Make sure to call {@link TimeFrameRecordCursor#open()} prior to this method.
     *
     * @return upper boundary of timestamps present in the time frame, exclusive
     */
    long getTimestampHi();

    /**
     * Make sure to call {@link TimeFrameRecordCursor#open()} prior to this method.
     *
     * @return lower boundary of timestamps present in the time frame, inclusive
     */
    long getTimestampLo();

    /**
     * @return whether the frame was previously open
     */
    boolean isOpen();
}
