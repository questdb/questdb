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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.model.WindowColumn;
import io.questdb.std.LongList;
import io.questdb.std.Vect;

public abstract class AbstractWindowFunctionFactory implements FunctionFactory {

    protected long rowsLo;
    protected long rowsHi;
    protected WindowContext windowContext;

    @Override
    public boolean isWindow() {
        return true;
    }

    protected boolean supportNullsDesc() {
        return false;
    }

    protected void checkWindowParameter(int position, SqlExecutionContext sqlExecutionContext) throws SqlException {
        windowContext = sqlExecutionContext.getWindowContext();
        if (windowContext.isEmpty()) {
            throw SqlException.emptyWindowContext(position);
        }

        if (windowContext.getNullsDescPos() > 0 && !this.supportNullsDesc()) {
            throw SqlException.$(windowContext.getNullsDescPos(), "RESPECT/IGNORE NULLS is not supported for current window function");
        }

        rowsLo = windowContext.getRowsLo();
        rowsHi = windowContext.getRowsHi();

        if (!windowContext.isDefaultFrame()) {
            if (rowsLo > 0) {
                throw SqlException.$(windowContext.getRowsLoKindPos(), "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only");
            }
            if (rowsHi > 0) {
                if (rowsHi != Long.MAX_VALUE) {
                    throw SqlException.$(windowContext.getRowsHiKindPos(), "frame end supports _number_ PRECEDING and CURRENT ROW only");
                } else if (rowsLo != Long.MIN_VALUE) {
                    throw SqlException.$(windowContext.getRowsHiKindPos(), "frame end supports UNBOUNDED FOLLOWING only when frame start is UNBOUNDED PRECEDING");
                }
            }
        }

        int exclusionKind = windowContext.getExclusionKind();
        int exclusionKindPos = windowContext.getExclusionKindPos();
        if (exclusionKind != WindowColumn.EXCLUDE_NO_OTHERS
                && exclusionKind != WindowColumn.EXCLUDE_CURRENT_ROW) {
            throw SqlException.$(exclusionKindPos, "only EXCLUDE NO OTHERS and EXCLUDE CURRENT ROW exclusion modes are supported");
        }

        if (exclusionKind == WindowColumn.EXCLUDE_CURRENT_ROW) {
            // assumes frame doesn't use 'following'
            if (rowsHi == Long.MAX_VALUE) {
                throw SqlException.$(exclusionKindPos, "EXCLUDE CURRENT ROW not supported with UNBOUNDED FOLLOWING frame boundary");
            }

            if (rowsHi == 0) {
                rowsHi = -1;
            }
            if (rowsHi < rowsLo) {
                throw SqlException.$(exclusionKindPos, "end of window is higher than start of window due to exclusion mode");
            }
        }

        if (windowContext.getFramingMode() == WindowColumn.FRAMING_GROUPS) {
            throw SqlException.$(position, "function not implemented for given window parameters");
        }
    }

    static class RingBufferDesc {
        long capacity;
        long startOffset;
        long size;
        long firstIdx;
        LongList freeList;

        void reset(long capacity, long startOffset, long size, long firstIdx, LongList freeList) {
            this.capacity = capacity;
            this.startOffset = startOffset;
            this.size = size;
            this.firstIdx = firstIdx;
            this.freeList = freeList;
        }
    }

    static void expandRingBuffer(MemoryARW memory, RingBufferDesc desc, int recordSize) {
        desc.capacity <<= 1;
        long oldAddress = memory.getPageAddress(0) + desc.startOffset;
        long newAddress = -1;

        // try to find matching block in free list
        for (int i = 0, n = desc.freeList.size(); i < n; i += 2) {
            if (desc.freeList.getQuick(i) == desc.capacity) {
                newAddress = memory.getPageAddress(0) + desc.freeList.getQuick(i + 1);
                // replace block info with ours
                desc.freeList.setQuick(i, desc.size);
                desc.freeList.setQuick(i + 1, desc.startOffset);
                break;
            }
        }

        if (newAddress == -1) {
            newAddress = memory.appendAddressFor(desc.capacity * recordSize);
            // call above can end up resizing and thus changing memory start address
            oldAddress = memory.getPageAddress(0) + desc.startOffset;
            desc.freeList.add(desc.size, desc.startOffset);
        }

        if (desc.firstIdx == 0) {
            Vect.memcpy(newAddress, oldAddress, desc.size * recordSize);
        } else {
            desc.firstIdx %= desc.size;
            //we can't simply copy because that'd leave a gap in the middle
            long firstPieceSize = (desc.size - desc.firstIdx) * recordSize;
            Vect.memcpy(newAddress, oldAddress + desc.firstIdx * recordSize, firstPieceSize);
            Vect.memcpy(newAddress + firstPieceSize, oldAddress, desc.firstIdx * recordSize);
            desc.firstIdx = 0;
        }

        desc.startOffset = newAddress - memory.getPageAddress(0);
    }
}
