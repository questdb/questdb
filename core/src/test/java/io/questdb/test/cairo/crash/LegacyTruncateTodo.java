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

package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.std.TestFilesFacadeImpl;

/**
 * The {@code _todo_} entry that a TRUNCATE of an earlier build armed before it was interrupted. TRUNCATE no longer
 * arms one, see {@code TableWriter.truncate0()}, but the writer open still completes the entry it finds. The entry is
 * stamped with the live {@code _txn} txn, the one the interrupted TRUNCATE started from: the open acts on it only then.
 */
final class LegacyTruncateTodo {
    // TableUtils.TODO_TRUNCATE: TRUNCATE emptied the symbol maps before it armed the entry
    static final int TRUNCATE = 1;
    // TableUtils.TODO_TRUNCATE_KEEP_SYMBOL_MAPS: TRUNCATE ... KEEP SYMBOL MAPS, armed by builds that had it
    static final int TRUNCATE_KEEP_SYMBOL_MAPS = 3;

    private LegacyTruncateTodo() {
    }

    /**
     * Arms the entry of an interrupted TRUNCATE of {@code table}, which must have no writer open, and returns the
     * {@code _txn} txn it is stamped with.
     */
    static long arm(CairoEngine engine, String table, int partitionBy, boolean isKeepSymbolMaps) {
        final TableToken token = engine.verifyTableName(table);
        final long txn;
        try (
                TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade());
                Path path = new Path().of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME)
        ) {
            txReader.ofRO(path.$(), ColumnType.TIMESTAMP, partitionBy);
            TableUtils.safeReadTxn(txReader, engine.getConfiguration().getMillisecondClock(), engine.getConfiguration().getSpinLockTimeout());
            txn = txReader.getTxn();
        }
        try (
                MemoryCMARW mem = Vm.getCMARWInstance();
                Path path = new Path().of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.TODO_FILE_NAME)
        ) {
            mem.smallFile(TestFilesFacadeImpl.INSTANCE, path.$(), MemoryTag.MMAP_DEFAULT);
            mem.putLong(0, txn);
            mem.putLong(24, txn);
            mem.putLong(32, 1);
            mem.putLong(40, isKeepSymbolMaps ? TRUNCATE_KEEP_SYMBOL_MAPS : TRUNCATE);
            mem.jumpTo(48);
        }
        return txn;
    }
}
