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

package io.questdb.cairo.frm.file;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.frm.Frame;
import io.questdb.cairo.frm.FrameColumnPool;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class FrameFactory implements RecycleBin<FrameImpl>, Closeable {
    private final FrameColumnPool columnPool;
    private final ObjList<FrameImpl> framePool = new ObjList<>();
    private boolean closed;

    public FrameFactory(CairoConfiguration configuration) {
        this.columnPool = new ContiguousFileColumnPool(configuration);
    }

    @Override
    public void close() {
        closed = true;
        Misc.freeObjList(framePool);
        Misc.free(columnPool);
    }

    public Frame createRW(
            Path partitionPath,
            long partitionTimestamp,
            RecordMetadata metadata,
            ColumnVersionWriter cvw,
            long size
    ) {
        FrameImpl frame = getOrCreate();
        frame.createRW(partitionPath, partitionTimestamp, metadata, cvw, size);
        return frame;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    public Frame open(boolean rw, Path path, long targetPartition, RecordMetadata metadata, ColumnVersionWriter cvr, long size) {
        if (rw) {
            return openRW(path, targetPartition, metadata, cvr, size);
        } else {
            return openRO(path, targetPartition, metadata, cvr, size);
        }
    }

    public Frame openRO(
            Path partitionPath,
            long partitionTimestamp,
            RecordMetadata metadata,
            ColumnVersionReader cvr,
            long partitionRowCount
    ) {
        FrameImpl frame = getOrCreate();
        frame.openRO(partitionPath, partitionTimestamp, metadata, cvr, partitionRowCount);
        return frame;
    }

    public Frame openRW(
            Path partitionPath,
            long partitionTimestamp,
            RecordMetadata metadata,
            ColumnVersionWriter cvw,
            long size
    ) {
        FrameImpl frame = getOrCreate();
        frame.openRW(partitionPath, partitionTimestamp, metadata, cvw, size);
        return frame;
    }

    @Override
    public void put(FrameImpl frame) {
        assert !isClosed();
        framePool.add(frame);
    }

    private FrameImpl getOrCreate() {
        if (framePool.size() > 0) {
            FrameImpl frm = framePool.getLast();
            framePool.setPos(framePool.size() - 1);
            return frm;
        }
        FrameImpl frame = new FrameImpl(columnPool);
        frame.setRecycleBin(this);
        return frame;
    }
}
