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
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import org.jetbrains.annotations.Nullable;

public class FullFwdPartitionFrameCursor extends AbstractFullPartitionFrameCursor {
    protected long rowGroupLo; // used for Parquet frames generation

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        while (partitionIndex < partitionHi) {
            final long hi = reader.openPartition(partitionIndex);
            if (hi > 0) {
                counter.add(hi);
            }
            partitionIndex++;
        }
    }

    @Override
    public @Nullable PartitionFrame next() {
        if (rowGroupIndex < rowGroupCount) {
            frame.partitionIndex = partitionIndex;
            frame.partitionFormat = PartitionFormat.PARQUET;
            frame.parquetFd = parquetDecoder.getFd();
            frame.rowLo = rowGroupLo;
            frame.rowHi = rowGroupLo + parquetDecoder.getMetadata().rowGroupSize(rowGroupIndex);
            rowGroupLo = frame.rowHi;
            if (++rowGroupIndex == rowGroupCount) {
                // Proceed to the next partition on the next call.
                partitionIndex++;
            }
            return frame;
        }

        while (partitionIndex < partitionHi) {
            final long hi = reader.openPartition(partitionIndex);
            if (hi < 1) {
                // this partition is missing, skip
                partitionIndex++;
            } else {
                final byte format = reader.getPartitionFormat(partitionIndex);

                if (format == PartitionFormat.PARQUET) {
                    parquetDecoder.of(reader.getParquetFd(partitionIndex));
                    final PartitionDecoder.Metadata metadata = parquetDecoder.getMetadata();
                    rowGroupIndex = 0;
                    rowGroupCount = metadata.rowGroupCount();

                    frame.partitionIndex = partitionIndex;
                    frame.partitionFormat = PartitionFormat.PARQUET;
                    frame.parquetFd = parquetDecoder.getFd();
                    frame.rowLo = 0;
                    frame.rowHi = metadata.rowGroupSize(rowGroupIndex);
                    rowGroupLo = frame.rowHi;
                    if (++rowGroupIndex == rowGroupCount) {
                        // Proceed to the next partition on the next call.
                        partitionIndex++;
                    }
                    return frame;
                }

                assert format == PartitionFormat.NATIVE;
                frame.partitionIndex = partitionIndex;
                frame.partitionFormat = PartitionFormat.NATIVE;
                frame.parquetFd = -1;
                frame.rowLo = 0;
                frame.rowHi = hi;
                frame.rowGroupIndex = -1;
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
        rowGroupIndex = 0;
        rowGroupCount = 0;
        rowGroupLo = 0;
    }
}
