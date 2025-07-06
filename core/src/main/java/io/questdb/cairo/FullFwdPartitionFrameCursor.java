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
import io.questdb.std.MemoryTag;
import org.jetbrains.annotations.Nullable;

public class FullFwdPartitionFrameCursor extends AbstractFullPartitionFrameCursor {

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
        while (partitionIndex < partitionHi) {
            final long hi = reader.openPartition(partitionIndex);
            if (hi < 1) {
                // this partition is missing, skip
                partitionIndex++;
            } else {
                frame.partitionIndex = partitionIndex;
                frame.rowLo = 0;
                frame.rowHi = hi;
                partitionIndex++;

                final byte format = reader.getPartitionFormat(frame.partitionIndex);
                if (format == PartitionFormat.PARQUET) {
                    final long addr = reader.getParquetAddr(frame.partitionIndex);
                    assert addr != 0;
                    final long parquetSize = reader.getParquetFileSize(frame.partitionIndex);
                    assert parquetSize > 0;
                    if (parquetDecoder == null) {
                        parquetDecoder = new PartitionDecoder();
                    }
                    parquetDecoder.of(addr, parquetSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    frame.format = PartitionFormat.PARQUET;
                    frame.parquetDecoder = parquetDecoder;
                    return frame;
                }

                assert format == PartitionFormat.NATIVE;
                frame.format = PartitionFormat.NATIVE;
                frame.parquetDecoder = null;
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
