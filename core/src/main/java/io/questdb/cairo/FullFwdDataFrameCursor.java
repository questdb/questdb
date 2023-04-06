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

package io.questdb.cairo;

import io.questdb.cairo.sql.DataFrame;
import org.jetbrains.annotations.Nullable;

public class FullFwdDataFrameCursor extends AbstractFullDataFrameCursor {
    private int skipToPartitionIndex = -1;
    private long skipToPosition = -1;

    @Override
    public @Nullable DataFrame next() {
        while (partitionIndex < partitionHi) {
            final long hi = getTableReader().openPartition(partitionIndex);
            if (hi < 1) {
                // this partition is missing, skip
                partitionIndex++;
            } else {
                frame.partitionIndex = partitionIndex;
                frame.rowLo = 0;
                frame.rowHi = hi;
                partitionIndex++;
                return frame;
            }
        }
        return null;
    }

    @Override
    public @Nullable DataFrame skipTo(long rowCount) {
        int partitionCount = getTableReader().getPartitionCount();

        if (partitionCount < 1) {
            return null;
        }

        if (skipToPartitionIndex == -1) {
            skipToPosition = rowCount;
            skipToPartitionIndex = 0;
        }

        long partitionRows = 0;
        while (skipToPartitionIndex < partitionCount) {
            partitionRows = getTableReader().openPartition(skipToPartitionIndex);
            if (partitionRows < 0) {
                skipToPartitionIndex++;
                continue;
            }
            if (partitionRows > skipToPosition) {
                break;
            }
            if (skipToPartitionIndex == partitionCount - 1) {
                skipToPosition = partitionRows;
                break;
            } else {
                skipToPosition -= partitionRows;
            }
            skipToPartitionIndex++;
        }

        frame.partitionIndex = skipToPartitionIndex;
        frame.rowHi = partitionRows;
        frame.rowLo = skipToPosition;
        this.partitionIndex = skipToPartitionIndex + 1;

        skipToPosition = -1;
        skipToPartitionIndex = -1;

        return frame;
    }

    public boolean supportsRandomAccess() {
        return true;
    }

    @Override
    public void toTop() {
        partitionIndex = 0;
        skipToPosition = -1;
        skipToPartitionIndex = -1;
    }
}
