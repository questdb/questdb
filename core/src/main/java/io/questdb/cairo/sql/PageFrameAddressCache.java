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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.std.*;

/**
 * Holds formats, addresses and sizes for native (mmapped) page frames.
 * <p>
 * Once initialized, this cache is thread-safe.
 * <p>
 * Meant to be used along with {@link PageFrameMemoryPool}.
 */
public class PageFrameAddressCache implements Mutable {
    private final ObjList<LongList> auxPageAddresses = new ObjList<>();
    private final ObjList<LongList> auxPageSizes = new ObjList<>();
    private final IntList columnTypes = new IntList();
    private final ByteList frameFormats = new ByteList();
    private final LongList frameSizes = new LongList();
    private final ObjectPool<LongList> longListPool = new ObjectPool<>(LongList::new, 64);
    private final long nativeCacheSizeThreshold;
    private final ObjList<LongList> pageAddresses = new ObjList<>();
    private final ObjList<LongList> pageSizes = new ObjList<>();
    // Makes it possible to determine real row id, not the one relative to the page.
    private final LongList rowIdOffsets = new LongList();
    // Sum of all LongList sizes.
    private long cacheSize;
    private int columnCount;

    public PageFrameAddressCache(CairoConfiguration configuration) {
        this.nativeCacheSizeThreshold = configuration.getSqlJitPageAddressCacheThreshold() / Long.BYTES;
    }

    public void add(int frameIndex, @Transient PageFrame frame) {
        if (frameSizes.size() >= frameIndex + 1) {
            return; // The page frame is already cached
        }

        if (frame.getFormat() == PageFrame.NATIVE_FORMAT) {
            final LongList framePageAddresses = longListPool.next();
            final LongList framePageSizes = longListPool.next();
            final LongList frameAuxPageAddresses = longListPool.next();
            final LongList frameAuxPageSizes = longListPool.next();
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                framePageAddresses.add(frame.getPageAddress(columnIndex));
                framePageSizes.add(frame.getPageSize(columnIndex));
                if (ColumnType.isVarSize(columnTypes.getQuick(columnIndex))) {
                    frameAuxPageAddresses.add(frame.getAuxPageAddress(columnIndex));
                    frameAuxPageSizes.add(frame.getAuxPageSize(columnIndex));
                } else {
                    frameAuxPageAddresses.add(0);
                    frameAuxPageSizes.add(0);
                }
            }
            pageAddresses.add(framePageAddresses);
            cacheSize += framePageAddresses.capacity();
            pageSizes.add(framePageSizes);
            cacheSize += framePageSizes.capacity();
            auxPageAddresses.add(frameAuxPageAddresses);
            cacheSize += frameAuxPageAddresses.capacity();
            auxPageSizes.add(frameAuxPageSizes);
            cacheSize += frameAuxPageSizes.capacity();
        } else {
            pageAddresses.add(null);
            pageSizes.add(null);
            auxPageAddresses.add(null);
            auxPageSizes.add(null);
        }

        frameSizes.add(frame.getPartitionHi() - frame.getPartitionLo());
        frameFormats.add(frame.getFormat());
        rowIdOffsets.add(Rows.toRowID(frame.getPartitionIndex(), frame.getPartitionLo()));
    }

    @Override
    public void clear() {
        frameSizes.clear();
        frameFormats.clear();
        pageAddresses.clear();
        auxPageAddresses.clear();
        pageSizes.clear();
        auxPageSizes.clear();
        rowIdOffsets.clear();
        if (cacheSize < nativeCacheSizeThreshold) {
            longListPool.clear();
        } else {
            longListPool.resetCapacity();
        }
        cacheSize = 0;
    }

    public LongList getAuxPageAddresses(int frameIndex) {
        return auxPageAddresses.getQuick(frameIndex);
    }

    public LongList getAuxPageSizes(int frameIndex) {
        return auxPageSizes.getQuick(frameIndex);
    }

    public int getColumnCount() {
        return columnCount;
    }

    public IntList getColumnTypes() {
        return columnTypes;
    }

    public byte getFrameFormat(int frameIndex) {
        return frameFormats.getQuick(frameIndex);
    }

    public long getFrameSize(int frameIndex) {
        return frameSizes.getQuick(frameIndex);
    }

    public LongList getPageAddresses(int frameIndex) {
        return pageAddresses.getQuick(frameIndex);
    }

    public LongList getPageSizes(int frameIndex) {
        return pageSizes.getQuick(frameIndex);
    }

    public long getRowIdOffset(int frameIndex) {
        return rowIdOffsets.getQuick(frameIndex);
    }

    public boolean hasColumnTops(int frameIndex) {
        final byte frameFormat = frameFormats.getQuick(frameIndex);
        assert frameFormat == PageFrame.NATIVE_FORMAT;
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            if (pageAddresses.getQuick(frameIndex).getQuick(columnIndex) == 0
                    // VARCHAR column that contains short strings will have zero data vector,
                    // so for such columns we also need to check that the aux (index) vector is zero.
                    && auxPageAddresses.getQuick(frameIndex).getQuick(columnIndex) == 0) {
                return true;
            }
        }
        return false;
    }

    public boolean isVarSizeColumn(int columnIndex) {
        return ColumnType.isVarSize(columnTypes.getQuick(columnIndex));
    }

    public void of(@Transient RecordMetadata metadata) {
        columnCount = metadata.getColumnCount();
        columnTypes.clear();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            columnTypes.add(metadata.getColumnType(columnIndex));
        }
        clear();
    }
}
