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

package io.questdb.cairo;

import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.RecordCursor;

public class FullBwdPartitionFrameCursor extends AbstractFullPartitionFrameCursor {

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
    public PartitionFrame next(long skipTarget) {
        while (partitionIndex > -1) {
            final long hi = reader.getPartitionRowCountFromMetadata(partitionIndex);
            if (hi < 1) {
                // this partition is missing, skip
                partitionIndex--;
            } else {
                frame.partitionIndex = partitionIndex;
                frame.rowLo = 0;
                frame.rowHi = hi;
                if (hi <= skipTarget) {
                    partitionIndex--;
                    return frame;
                }
                return nextSlow();
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
    }

    private FullTablePartitionFrame nextSlow() {
        reader.openPartition(partitionIndex);

        // opening partition may produce "data unavailable errors", in which case we must not
        // change partition index prematurely
        partitionIndex--;

        final byte format = reader.getPartitionFormat(frame.partitionIndex);
        if (format == PartitionFormat.PARQUET) {
            frame.parquetDecoder = reader.getAndInitParquetPartitionDecoders(frame.partitionIndex);
            assert frame.parquetDecoder.getFileAddr() != 0 : "parquet decoder is not initialized";
            frame.format = PartitionFormat.PARQUET;
            return frame;
        }

        assert format == PartitionFormat.NATIVE;
        frame.format = PartitionFormat.NATIVE;
        frame.parquetDecoder = null;
        return frame;
    }
}
