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
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.CATALOG_FILE_NAME;
import static io.questdb.cairo.TableUtils.openSmallFile;

public class TxnCatalog implements Closeable {
    private final FilesFacade ff;
    private final MemoryMAR metaMem = Vm.getMARInstance();

    TxnCatalog(FilesFacade ff) {
        this.ff = ff;
    }

    @Override
    public void close() {
        Misc.free(metaMem);
    }

    private long calcOffsetForTxn(long txn) {
        return Integer.BYTES + (txn - 1) * (Long.BYTES + Integer.BYTES + Long.BYTES);
    }

    void open(Path path, int pathLen, long startTxn) {
        openSmallFile(ff, path, pathLen, metaMem, CATALOG_FILE_NAME, MemoryTag.MMAP_SEQUENCER);
        if (startTxn == 0) {
            metaMem.putInt(WalWriter.WAL_FORMAT_VERSION);
        } else {
            metaMem.jumpTo(Integer.BYTES + startTxn * (Long.BYTES + Integer.BYTES + Long.BYTES));
        }
    }

    void setEntry(long txn, int walId, long segmentId) {
        metaMem.jumpTo(calcOffsetForTxn(txn));
        metaMem.putLong(txn);
        metaMem.putInt(walId);
        metaMem.putLong(segmentId);
    }
}
