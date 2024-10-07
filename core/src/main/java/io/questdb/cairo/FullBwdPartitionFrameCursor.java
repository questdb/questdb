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

import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.MemoryTag;

public class FullBwdPartitionFrameCursor extends AbstractFullPartitionFrameCursor {
    protected long rowHi; // used for Parquet frames generation

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        while (partitionIndex > -1) {
            final long hi = reader.openPartition(partitionIndex);
            if (hi > 0) {
                counter.add(hi);
            }
            partitionIndex--;
        }
    }

    @Override
    public PartitionFrame next() {
        if (rowGroupIndex > -1) {
            return prepareParquetFrame();
        }

        while (partitionIndex > -1) {
            final long hi = reader.openPartition(partitionIndex);
            if (hi < 1) {
                // this partition is missing, skip
                partitionIndex--;
            } else {
                final byte format = reader.getPartitionFormat(partitionIndex);

                if (format == PartitionFormat.PARQUET) {
                    final long fd = reader.getParquetFd(partitionIndex);
                    assert fd != -1;
                    parquetDecoder.of(fd);
                    rowGroupCount = parquetDecoder.getMetadata().rowGroupCount();
                    rowGroupIndex = rowGroupCount - 1;
                    rowHi = hi;
                    return prepareParquetFrame();
                }

                frame.partitionIndex = partitionIndex;
                frame.partitionFormat = PartitionFormat.NATIVE;
                frame.parquetFd = -1;
                frame.rowLo = 0;
                frame.rowHi = hi;
                frame.rowGroupIndex = -1;
                partitionIndex--;
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
        partitionIndex = partitionHi - 1;
        rowGroupIndex = -1;
        rowGroupCount = 0;
        rowHi = 0;
    }

    private FullTablePartitionFrame prepareParquetFrame() {
        frame.partitionIndex = partitionIndex;
        frame.partitionFormat = PartitionFormat.PARQUET;
        frame.parquetFd = parquetDecoder.getFd();
        frame.rowGroupLo = 0;
        frame.rowHi = rowHi;
        frame.rowLo = rowHi - parquetDecoder.getMetadata().rowGroupSize(rowGroupIndex);
        rowHi = frame.rowLo;
        if (--rowGroupIndex == -1) {
            // Proceed to the next partition on the next call.
            partitionIndex--;
        }
        return frame;
    }
}
