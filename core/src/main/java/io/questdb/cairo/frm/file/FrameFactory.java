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

package io.questdb.cairo.frm.file;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.TableWriterMetadata;
import io.questdb.cairo.frm.Frame;
import io.questdb.cairo.frm.FrameColumnPool;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.mp.ConcurrentPool;
import io.questdb.std.Misc;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class FrameFactory implements RecycleBin<FrameImpl>, Closeable {
    private final FrameColumnPool columnPool;
    private final ConcurrentPool<FrameImpl> framePool = new ConcurrentPool<>();
    private boolean closed;

    public FrameFactory(CairoConfiguration configuration) {
        this.columnPool = new ContiguousFileColumnPool(configuration);
    }

    // This method is used to clear the frame pool and release all frames.
    // It is NOT thread safe.
    public void clear() {
        FrameImpl t;
        closed = true;
        while ((t = framePool.pop()) != null) {
            Misc.free(t);
        }
        closed = false;
    }

    @Override
    public void close() {
        closed = true;
        Misc.free(columnPool);
        clear();
    }

    /**
     * Creates a new frame for writing. This method is thread safe.
     *
     * @param partitionPath      the path to the partition directory
     * @param partitionTimestamp the timestamp of the partition
     * @param metadata           the metadata for the frame
     * @param cvw                the column version writer
     * @param size               the size of the frame, in row count
     * @return a new frame ready for writing
     */
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

    /**
     * Opens a frame for reading or writing. This method is thread safe.
     *
     * @param rw              true for read-write, false for read-only
     * @param path            the path to the partition directory
     * @param targetPartition the target partition timestamp
     * @param metadata        the metadata for the frame
     * @param cvr             the column version reader or writer, depending on rw
     * @param size            the size of the frame, in row count
     * @return a frame ready for reading or writing
     */
    public Frame open(boolean rw, Path path, long targetPartition, RecordMetadata metadata, ColumnVersionWriter cvr, long size) {
        if (rw) {
            return openRW(path, targetPartition, metadata, cvr, size);
        } else {
            return openRO(path, targetPartition, metadata, cvr, size);
        }
    }

    /**
     * Opens a frame from memory columns, used to create frames from the data to be committed. This method is thread safe.
     *
     * @param columns  the memory columns to create the frame from
     * @param metadata the metadata for the frame
     * @param rowCount the number of rows in the frame
     * @return a new frame created from the memory columns
     */
    public Frame openROFromMemoryColumns(ReadOnlyObjList<? extends MemoryCR> columns, TableWriterMetadata metadata, long rowCount) {
        FrameImpl frame = getOrCreate();
        frame.createROFromMemoryColumns(columns, metadata, rowCount);
        return frame;
    }

    /**
     * Opens a frame for reading. This method is thread safe.
     *
     * @param tablePath          the path to the table directory
     * @param partitionTimestamp the timestamp of the partition
     * @param partitionNameTxn   the transaction ID of the partition name
     * @param partitionBy        the partition by value
     * @param metadata           the metadata for the frame
     * @param cvr                the column version reader
     * @param partitionRowCount  the number of rows in the partition
     * @return a new frame ready for reading
     */
    public Frame openRO(
            @Transient Path tablePath,
            long partitionTimestamp,
            long partitionNameTxn,
            int partitionBy,
            RecordMetadata metadata,
            ColumnVersionReader cvr,
            long partitionRowCount
    ) {
        FrameImpl frame = getOrCreate();
        frame.openRO(tablePath, partitionTimestamp, partitionNameTxn, partitionBy, metadata, cvr, partitionRowCount);
        return frame;
    }

    /**
     * Opens a frame for reading. This method is thread safe.
     *
     * @param partitionPath      the path to the partition directory
     * @param partitionTimestamp the timestamp of the partition
     * @param metadata           the metadata for the frame
     * @param cvr                the column version reader
     * @param partitionRowCount  the number of rows in the partition
     * @return a new frame ready for reading
     */
    public Frame openRO(
            @Transient Path partitionPath,
            long partitionTimestamp,
            RecordMetadata metadata,
            ColumnVersionReader cvr,
            long partitionRowCount
    ) {
        FrameImpl frame = getOrCreate();
        frame.openRO(partitionPath, partitionTimestamp, metadata, cvr, partitionRowCount);
        return frame;
    }

    /**
     * Opens a frame for reading and writing. This method is thread safe.
     *
     * @param partitionPath      the path to the partition directory
     * @param partitionTimestamp the timestamp of the partition
     * @param metadata           the metadata for the frame
     * @param cvw                the column version writer
     * @param size               the size of the frame, in row count
     * @return a new frame ready for reading and writing
     */
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

    /**
     * Returns the frame to the pool used by this factory. This method is thread safe.
     */
    @Override
    public void put(FrameImpl frame) {
        assert !isClosed();
        framePool.push(frame);
    }

    private FrameImpl getOrCreate() {
        FrameImpl frm = framePool.pop();
        if (frm != null) {
            return frm;
        }
        FrameImpl frame = new FrameImpl(columnPool);
        frame.setRecycleBin(this);
        return frame;
    }
}
