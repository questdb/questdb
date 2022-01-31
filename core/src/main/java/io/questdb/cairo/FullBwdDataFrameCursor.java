/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.sql.DataFrame;
import org.jetbrains.annotations.Nullable;

public class FullBwdDataFrameCursor extends AbstractFullDataFrameCursor {

    @Override
    public DataFrame next() {
        while (this.partitionIndex > -1) {
            final long hi = reader.openPartition(partitionIndex);
            if (hi < 1) {
                // this partition is missing, skip
                partitionIndex--;
            } else {
                frame.partitionIndex = partitionIndex;
                frame.rowHi = hi;
                partitionIndex--;
                return frame;

            }
        }
        return null;
    }

    @Override
    public void toTop() {
        this.partitionIndex = this.partitionHi - 1;
    }

    @Override
    public @Nullable DataFrame skipTo(long rowCount) {
        int partitionCount = getTableReader().getPartitionCount();

        if (partitionCount < 1) {
            return null;
        }

        long position = rowCount;
        long partitionRows = 0;
        int partitionIndex = partitionCount - 1;

        for (; partitionIndex > -1; partitionIndex--) {
            partitionRows = getTableReader().openPartition(partitionIndex);
            if (partitionRows < 0) {
                continue;
            }
            if (partitionRows > position) {
                break;
            }
            if (partitionIndex == 0) {
                position = -1L;
                break;
            } else {
                position -= partitionRows;
            }
        }

        frame.partitionIndex = partitionIndex;
        frame.rowHi = position > -1L ? partitionRows - position : position;
        frame.rowLo = 0;
        this.partitionIndex = partitionIndex - 1;

        return frame;
    }

    public boolean supportsRandomAccess() {
        return true;
    }
}
