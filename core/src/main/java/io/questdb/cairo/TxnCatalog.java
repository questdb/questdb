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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public class TxnCatalog implements Closeable {
    private final FilesFacade ff;
    private final MemoryCMARW txnListMem = Vm.getCMARWInstance();
    private final int RECORD_SIZE = Integer.BYTES + Long.BYTES + Long.BYTES;
    private final int HEADER_SIZE = Integer.BYTES + Long.BYTES;
    private final long COUNT_OFFSET = Integer.BYTES;
    private long txnCount;

    TxnCatalog(FilesFacade ff) {
        this.ff = ff;
    }

    @Override
    public void close() {
        Misc.free(txnListMem);
    }

    void open(Path path) {
        openSmallFile(ff, path, path.length(), txnListMem, CATALOG_FILE_NAME, MemoryTag.MMAP_SEQUENCER);
        txnCount = txnListMem.getLong(COUNT_OFFSET);
        if (txnCount == 0) {
            txnListMem.putInt(WalWriter.WAL_FORMAT_VERSION);
        } else {
            txnListMem.jumpTo(HEADER_SIZE + txnCount * RECORD_SIZE);
        }
    }

    long addEntry(int walId, long segmentId, long segmentTxn) {
        txnListMem.putInt(walId);
        txnListMem.putLong(segmentId);
        txnListMem.putLong(segmentTxn);

        Unsafe.getUnsafe().storeFence();
        txnListMem.putLong(COUNT_OFFSET, ++txnCount);
        return txnCount - 1;
    }

    // Can be used in parallel thread with no synchronisation between it and  addEntry
    long maxTxn() {
        Unsafe.getUnsafe().loadFence();
        long txnCount = txnListMem.getLong(COUNT_OFFSET);
        return txnCount - 1L;
    }
}
