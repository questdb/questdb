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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.std.Pool;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.Nullable;

public class WalTableUpdateDetails extends TableUpdateDetails {
    /**
     * Sequencer transaction this table's writer was last checked for pending
     * structure changes at. {@link Long#MIN_VALUE} forces the first check.
     * <p>
     * Read and written from the QWP ingest path only, which is single-threaded
     * per cache, so it needs no synchronisation.
     */
    private long lastStructureCheckSeqTxn = Long.MIN_VALUE;

    public WalTableUpdateDetails(
            CairoEngine engine,
            @Nullable SecurityContext securityContext,
            TableWriterAPI writer,
            DefaultColumnTypes defaultColumnTypes,
            Utf8String tableNameUtf8,
            Pool<SymbolCache> symbolCachePool,
            long commitInterval,
            boolean commitOnClose,
            long maxUncommittedRows
    ) {
        super(engine, securityContext, writer, -1, defaultColumnTypes, tableNameUtf8, symbolCachePool, commitInterval, commitOnClose, maxUncommittedRows);
    }

    public long getLastStructureCheckSeqTxn() {
        return lastStructureCheckSeqTxn;
    }

    @Override
    public ThreadLocalDetails getThreadLocalDetails(int workerId) {
        return super.getThreadLocalDetails(0);
    }

    public void setLastStructureCheckSeqTxn(long seqTxn) {
        lastStructureCheckSeqTxn = seqTxn;
    }
}
