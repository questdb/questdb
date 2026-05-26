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

package io.questdb.cairo.wal.sortedruns;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.LPSZ;

/**
 * Per-partition read-side handle for the {@code _sortedruns.idx} file.
 * <p>
 * The file is a flat array of 64-bit physRowIds in ts-ascending order:
 * <pre>
 *   entry[i] = physRowId of the i-th logical (ts-ascending) row in the partition.
 * </pre>
 * No header, no blocks; size is exactly {@code partitionRowCount * 8} bytes.
 * <p>
 * The file is memory-mapped read-only on open. Pages are not pre-faulted -
 * the OS brings them into RSS on demand as the read path dereferences the
 * mapping via {@link io.questdb.std.Unsafe}.
 */
public final class SortedRunsIndex implements QuietCloseable {
    public static final String FILE_NAME = "_sortedruns.idx";
    private MemoryCMR mem;
    private long rowCount;

    @Override
    public void close() {
        mem = Misc.free(mem);
        rowCount = 0;
    }

    /**
     * Returns the base mmap address of the index file. Reading the i-th
     * physRowId is {@code Unsafe.getLong(getAddress() + i * 8)}.
     */
    public long getAddress() {
        return mem.addressOf(0);
    }

    public long getRowCount() {
        return rowCount;
    }

    public SortedRunsIndex of(FilesFacade ff, LPSZ idxFile, long rowCount) {
        this.rowCount = rowCount;
        if (mem == null) {
            mem = Vm.getCMRInstance();
        }
        mem.of(ff, idxFile, ff.getPageSize(), rowCount * Long.BYTES, MemoryTag.MMAP_DEFAULT);
        return this;
    }
}
