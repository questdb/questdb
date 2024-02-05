/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
     * @return numeric index of the current partition
     */
    int getPartitionIndex();

    /**
     * @return timestamp of the current partition
     */
    long getPartitionTimestamp();

    /**
     * Make sure to call {@link TimeFrameRecordCursor#open()} prior to this method.
     * <p>
     * Note: it's allowed to jump with the {@link TimeFrameRecordCursor} record
     * to any row id in the [rowLo, rowHi) range. {@link Rows#toRowID(int, long)}
     * method should be used to obtain final row id.
     *
     * @return upper boundary for the last row of a data frame, i.e. last row + 1
     */
    long getRowHi();

    /**
     * Make sure to call {@link TimeFrameRecordCursor#open()} prior to this method.
     * <p>
     * Note: it's allowed to jump with the {@link TimeFrameRecordCursor} record
     * to any row id in the [rowLo, rowHi) range. {@link Rows#toRowID(int, long)}
     * method should be used to obtain final row id.
     *
     * @return first row of a data frame
     */
    long getRowLo();

    /**
     * @return whether the partition was previously open
     */
    boolean isOpen();
}
