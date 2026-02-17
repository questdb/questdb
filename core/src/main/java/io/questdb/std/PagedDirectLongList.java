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

package io.questdb.std;


import java.io.Closeable;

public class PagedDirectLongList implements Closeable {
    private final int memoryTag;
    private final ObjList<DirectLongList> pages = new ObjList<>();
    private int blockSize;
    private int pageCapacity;

    public PagedDirectLongList(int memoryTag) {
        this.memoryTag = memoryTag;
    }

    public long allocateBlock() {
        int currentPage = pages.size() - 1;
        if (pages.size() > 0) {
            DirectLongList page = pages.get(currentPage);
            if (page.size() + blockSize < page.getCapacity()) {
                page.setPos(page.size() + blockSize);
                return page.getAddress() + (page.size() - blockSize) * Long.BYTES;
            }
        }
        DirectLongList page = new DirectLongList(pageCapacity, memoryTag);
        pages.add(page);

        page.setPos(page.size() + blockSize);
        return page.getAddress() + (page.size() - blockSize) * Long.BYTES;
    }

    public void clear() {
        if (pages.size() > 0) {
            for (int i = 0, n = pages.size(); i < n; i++) {
                if (i > 0) {
                    pages.get(i).close();
                } else {
                    pages.getQuick(i).clear();
                }
            }
            pages.setPos(1);
        }
    }

    @Override
    public void close() {
        Misc.freeObjList(pages);
    }

    public long getBlockAddress(long blockIndex) {
        int blockPage = Numbers.decodeLowInt(blockIndex);
        long offset = Numbers.decodeHighInt(blockIndex);

        return pages.getQuick(blockPage).getAddress() + offset * Long.BYTES;
    }

    public long nextBlockIndex(long prevBlockIndex) {
        int blockPage = prevBlockIndex < 0 ? 0 : Numbers.decodeLowInt(prevBlockIndex);
        int prevBlockOffset = prevBlockIndex < 0 ? -blockSize : Numbers.decodeHighInt(prevBlockIndex);

        long newIndex = getBlockIndex(blockSize, blockPage, prevBlockOffset + blockSize);
        if (newIndex > -1L) {
            return newIndex;
        }

        return getBlockIndex(blockSize, ++blockPage, 0);
    }

    public void setBlockSize(int blockSize) {
        if (pages.size() > 1 || (pages.size() == 1 && pages.getQuick(0).size() > 0)) {
            throw new UnsupportedOperationException("list must be clear when changing block size");
        }
        this.blockSize = blockSize;
        this.pageCapacity = Math.max(blockSize, (4096 / blockSize) * blockSize);
    }

    private long getBlockIndex(int blockSize, int blockPage, int probeOffset) {
        if (blockPage < pages.size()) {
            DirectLongList page = pages.getQuick(blockPage);
            if (page.size() >= probeOffset + blockSize) {
                return Numbers.encodeLowHighInts(blockPage, probeOffset);
            }
        }
        return -1L;
    }
}
