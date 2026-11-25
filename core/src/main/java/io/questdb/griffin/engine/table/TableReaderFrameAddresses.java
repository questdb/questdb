/*+*****************************************************************************
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.IntList;

/**
 * The column addresses of a native table reader page frame, computed apart from the frame's
 * enumeration: what {@link FwdTableReaderPageFrameCursor} and {@link BwdTableReaderPageFrameCursor}
 * compute in their {@code computeNativeFrame()} for the frame of rows {@code [frameLo, frameHi)},
 * written into a {@link PageFrameAddressCache} entry. It reads only the reader's column memory, column
 * tops and metadata, which stay fixed while the partition is open, so any thread may run it, for
 * any frame, concurrently with others.
 */
final class TableReaderFrameAddresses {

    private TableReaderFrameAddresses() {
    }

    static void fill(
            TableReader reader,
            IntList columnIndexes,
            IntList columnSizeShifts,
            int partitionIndex,
            long frameLo,
            long frameHi,
            PageFrameAddressCache cache,
            int frameIndex
    ) {
        final int base = reader.getColumnBase(partitionIndex);
        for (int i = 0, n = columnIndexes.size(); i < n; i++) {
            final int columnIndex = columnIndexes.getQuick(i);
            final int readerColIndex = TableReader.getPrimaryColumnIndex(base, columnIndex);
            final MemoryR colMem = reader.getColumn(readerColIndex);
            // when the entire column is NULL we make it skip the whole of the frame
            final long top = colMem instanceof NullMemoryCMR ? frameHi : reader.getColumnTop(base, columnIndex);
            final long loAdjusted = frameLo - top;
            final long hiAdjusted = frameHi - top;
            final int sh = columnSizeShifts.getQuick(i);
            if (hiAdjusted > 0) {
                if (sh > -1) {
                    // the reader maps the whole column as one page; a fixed-size column
                    final long address = colMem.getPageAddress(0);
                    final long offset = loAdjusted << sh;
                    cache.setFrameColumnAddresses(frameIndex, i, address + offset, (hiAdjusted << sh) - offset, 0, 0);
                } else {
                    final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(reader.getMetadata().getColumnType(columnIndex));
                    final long auxAddress = reader.getColumn(readerColIndex + 1).getPageAddress(0);
                    final long auxOffsetLo = columnTypeDriver.getAuxVectorOffset(loAdjusted);
                    final long auxOffsetHi = columnTypeDriver.getAuxVectorOffset(hiAdjusted);
                    final long dataSize = columnTypeDriver.getDataVectorSizeAt(auxAddress, hiAdjusted - 1);
                    // some var-size columns may not have data memory (fully inlined)
                    final long dataAddress = dataSize > 0 ? colMem.getPageAddress(0) : 0;
                    cache.setFrameColumnAddresses(frameIndex, i, dataAddress, dataSize, auxAddress + auxOffsetLo, auxOffsetHi - auxOffsetLo);
                }
            } else {
                // a column top: the data page size is the row count's size hint, see computeNativeFrame()
                cache.setFrameColumnAddresses(frameIndex, i, 0, (hiAdjusted - loAdjusted) << (sh > -1 ? sh : 0), 0, 0);
            }
        }
    }
}
