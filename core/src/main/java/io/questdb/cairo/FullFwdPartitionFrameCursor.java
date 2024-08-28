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

package io.questdb.cairo;

import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.RecordCursor;
import org.jetbrains.annotations.Nullable;

public class FullFwdPartitionFrameCursor extends AbstractFullPartitionFrameCursor {

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        while (partitionIndex < partitionHi) {
            final long hi = getTableReader().openPartition(partitionIndex);
            if (hi > 0) {
                counter.add(hi);
            }
            partitionIndex++;
        }
    }

    @Override
    public @Nullable PartitionFrame next() {
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
    public boolean supportsSizeCalculation() {
        return true;
    }

    @Override
    public void toTop() {
        partitionIndex = 0;
    }
}
