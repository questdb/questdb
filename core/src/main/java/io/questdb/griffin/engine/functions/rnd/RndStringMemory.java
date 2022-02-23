/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryAR;
import io.questdb.griffin.SqlException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;

import java.io.Closeable;

class RndStringMemory implements Closeable {
    private final MemoryAR strMem;
    private final MemoryAR idxMem;
    private final int count;
    private final int lo;
    private final int hi;

    RndStringMemory(String signature, int count, int lo, int hi, int position, CairoConfiguration configuration) throws SqlException {
        this.count = count;
        this.lo = lo;
        this.hi = hi;

        final int pageSize = configuration.getRndFunctionMemoryPageSize();
        final int maxPages = configuration.getRndFunctionMemoryMaxPages();
        final long actualMem = (long) maxPages * pageSize;
        // check against worst case, the highest possible mem usage
        final long hiMem = count * (Vm.getStorageLength(hi) + Long.BYTES);
        if (hiMem > actualMem) {
            throw SqlException.position(position)
                    .put("not enough memory for ").put(signature)
                    .put(" [pageSize=").put(pageSize)
                    .put(", maxPages=").put(maxPages)
                    .put(", actualMem=").put(actualMem)
                    .put(", requiredMem=").put(hiMem)
                    .put(']');
        }

        final int idxPages = count * 8 / pageSize + 1;
        strMem = Vm.getARInstance(pageSize, maxPages - idxPages, MemoryTag.NATIVE_DEFAULT);
        idxMem = Vm.getARInstance(pageSize, idxPages, MemoryTag.NATIVE_DEFAULT);
    }

    @Override
    public void close() {
        Misc.free(strMem);
        Misc.free(idxMem);
    }

    CharSequence getStr(long index) {
        if (index < 0) {
            return null;
        }
        return strMem.getStr(getStrAddress(index));
    }

    CharSequence getStr2(long index) {
        if (index < 0) {
            return null;
        }
        return strMem.getStr2(getStrAddress(index));
    }

    private long getStrAddress(long index) {
        return idxMem.getLong(index * Long.BYTES);
    }

    void init(Rnd rnd) {
        strMem.jumpTo(0);
        idxMem.jumpTo(0);

        if (lo == hi) {
            initFixedLength(rnd);
        } else {
            initVariableLength(rnd);
        }
    }

    private void initFixedLength(Rnd rnd) {
        final long storageLength = Vm.getStorageLength(lo);
        for (int i = 0; i < count; i++) {
            final long o = strMem.putStr(rnd.nextChars(lo));
            idxMem.putLong(o - storageLength);
        }
    }

    private void initVariableLength(Rnd rnd) {
        for (int i = 0; i < count; i++) {
            final int len = lo + rnd.nextPositiveInt() % (hi - lo + 1);
            final long o = strMem.putStr(rnd.nextChars(len));
            idxMem.putLong(o - Vm.getStorageLength(len));
        }
    }
}
