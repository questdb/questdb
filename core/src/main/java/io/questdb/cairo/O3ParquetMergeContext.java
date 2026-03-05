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

import io.questdb.griffin.engine.table.parquet.OwnedMemoryPartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionUpdater;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.griffin.engine.table.parquet.RowGroupStatBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.io.Closeable;

public class O3ParquetMergeContext implements Closeable {
    private PartitionDescriptor chunkDescriptor;
    private LongList gapO3Ranges;
    private LongList mergeDstBufs;
    private ObjList<O3ParquetMergeStrategy.MergeAction> actionsBuf;
    private DirectIntList parquetColumns;
    private PartitionDecoder partitionDecoder;
    private OwnedMemoryPartitionDescriptor partitionDescriptor;
    private PartitionUpdater partitionUpdater;
    private LongList rgO3Ranges;
    private RowGroupBuffers rowGroupBuffers;
    private LongList rowGroupBounds;
    private RowGroupStatBuffers rowGroupStatBuffers;

    public O3ParquetMergeContext() {
        chunkDescriptor = new PartitionDescriptor();
        gapO3Ranges = new LongList();
        mergeDstBufs = new LongList();
        actionsBuf = new ObjList<>();
        parquetColumns = new DirectIntList(64, MemoryTag.NATIVE_O3);
        partitionDecoder = new PartitionDecoder();
        partitionDescriptor = new OwnedMemoryPartitionDescriptor();
        partitionUpdater = new PartitionUpdater();
        rgO3Ranges = new LongList();
        rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
        rowGroupBounds = new LongList();
        rowGroupStatBuffers = new RowGroupStatBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
    }

    public void clear() {
        chunkDescriptor.clear();
        gapO3Ranges.clear();
        mergeDstBufs.clear();
        parquetColumns.clear();
        partitionDescriptor.clear();
        rgO3Ranges.clear();
        rowGroupBounds.clear();
    }

    @Override
    public void close() {
        chunkDescriptor = Misc.free(chunkDescriptor);
        gapO3Ranges = null;
        mergeDstBufs = null;
        actionsBuf = null;
        parquetColumns = Misc.free(parquetColumns);
        partitionDecoder = Misc.free(partitionDecoder);
        partitionDescriptor = Misc.free(partitionDescriptor);
        partitionUpdater = Misc.free(partitionUpdater);
        rgO3Ranges = null;
        rowGroupBuffers = Misc.free(rowGroupBuffers);
        rowGroupBounds = null;
        rowGroupStatBuffers = Misc.free(rowGroupStatBuffers);
    }

    public PartitionDescriptor getChunkDescriptor() {
        return chunkDescriptor;
    }

    public LongList getGapO3Ranges() {
        return gapO3Ranges;
    }

    public ObjList<O3ParquetMergeStrategy.MergeAction> getActionsBuf() {
        return actionsBuf;
    }

    public LongList getMergeDstBufs(int colCount) {
        final int requiredLen = colCount * 4;
        mergeDstBufs.setPos(requiredLen);
        mergeDstBufs.fill(0, requiredLen, 0);
        return mergeDstBufs;
    }

    public DirectIntList getParquetColumns() {
        return parquetColumns;
    }

    public PartitionDecoder getPartitionDecoder() {
        return partitionDecoder;
    }

    public OwnedMemoryPartitionDescriptor getPartitionDescriptor() {
        return partitionDescriptor;
    }

    public PartitionUpdater getPartitionUpdater() {
        return partitionUpdater;
    }

    public LongList getRgO3Ranges() {
        return rgO3Ranges;
    }

    public RowGroupBuffers getRowGroupBuffers() {
        return rowGroupBuffers;
    }

    public LongList getRowGroupBounds() {
        return rowGroupBounds;
    }

    public RowGroupStatBuffers getRowGroupStatBuffers() {
        return rowGroupStatBuffers;
    }
}
