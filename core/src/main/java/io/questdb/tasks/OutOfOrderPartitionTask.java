/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.tasks;

import io.questdb.cairo.AppendMemory;
import io.questdb.cairo.ContiguousVirtualMemory;
import io.questdb.cairo.TableWriterMetadata;
import io.questdb.std.AbstractLockable;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class OutOfOrderPartitionTask extends AbstractLockable implements Closeable {
    private final Path path = new Path();
    private long txn;
    private long oooTimestampMax;
    private long oooIndexLo;
    private long oooIndexHi;
    private long oooIndexMax;
    private long lastPartitionIndexMax;
    private long partitionTimestampHi;
    private long timestampMergeIndex;
    private long tableMaxTimestamp;
    private long tableCeilOfMaxTimestamp;
    private long tableFloorOfMinTimestamp;
    private long tableFloorOfMaxTimestamp;
    private FilesFacade ff;
    // todo: rename
    private int rootLen;
    private int partitionBy;
    private int timestampIndex;
    private ObjList<AppendMemory> columns;
    private ObjList<ContiguousVirtualMemory> oooColumns;
    private TableWriterMetadata metadata;

    @Override
    public void close() {
        Misc.free(path);
    }

    public ObjList<AppendMemory> getColumns() {
        return columns;
    }

    public FilesFacade getFf() {
        return ff;
    }

    public long getLastPartitionIndexMax() {
        return lastPartitionIndexMax;
    }

    public TableWriterMetadata getMetadata() {
        return metadata;
    }

    public ObjList<ContiguousVirtualMemory> getOooColumns() {
        return oooColumns;
    }

    public long getOooIndexHi() {
        return oooIndexHi;
    }

    public long getOooIndexLo() {
        return oooIndexLo;
    }

    public long getOooIndexMax() {
        return oooIndexMax;
    }

    public long getOooTimestampMax() {
        return oooTimestampMax;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public long getPartitionTimestampHi() {
        return partitionTimestampHi;
    }

    public Path getPath() {
        return path;
    }

    public int getRootLen() {
        return rootLen;
    }

    public long getTableCeilOfMaxTimestamp() {
        return tableCeilOfMaxTimestamp;
    }

    public long getTableFloorOfMaxTimestamp() {
        return tableFloorOfMaxTimestamp;
    }

    public long getTableFloorOfMinTimestamp() {
        return tableFloorOfMinTimestamp;
    }

    public long getTableMaxTimestamp() {
        return tableMaxTimestamp;
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    public long getTimestampMergeIndex() {
        return timestampMergeIndex;
    }

    public long getTxn() {
        return txn;
    }
}
