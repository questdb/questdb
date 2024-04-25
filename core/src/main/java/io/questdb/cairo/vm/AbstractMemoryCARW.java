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

package io.questdb.cairo.vm;

import io.questdb.cairo.vm.api.MemoryCARW;

public abstract class AbstractMemoryCARW extends AbstractMemoryCR implements MemoryCARW {
    protected long appendAddress = 0;
    private long shiftRightOffset = 0;

    public AbstractMemoryCARW(boolean stableStrings) {
        super(stableStrings);
    }

    @Override
    public long addressOf(long offset) {
        offset -= shiftRightOffset;
        assert offset <= size : "offset=" + offset + ", size=" + size;
        return pageAddress + offset;
    }

    @Override
    public long appendAddressFor(long offset, long bytes) {
        offset -= shiftRightOffset;
        checkAndExtend(pageAddress + offset + bytes);
        return pageAddress + offset;
    }

    @Override
    public final long getAppendOffset() {
        return appendAddress - pageAddress + shiftRightOffset;
    }

    @Override
    public boolean hasBytes(long offset, long bytes) {
        return offset - shiftRightOffset + bytes <= size;
    }

    /**
     * Updates append pointer with address for the given offset. All put* functions will be
     * appending from this offset onwards effectively overwriting data. Size of virtual memory remains
     * unaffected until the moment memory has to be extended.
     *
     * @param offset position from 0 in virtual memory.
     */
    @Override
    public void jumpTo(long offset) {
        offset -= shiftRightOffset;
        checkAndExtend(pageAddress + offset);
        appendAddress = pageAddress + offset;
    }

    @Override
    public void shiftOffsetRight(long shiftRightOffset) {
        this.shiftRightOffset = shiftRightOffset;
    }

    protected abstract void checkAndExtend(long address);
}
