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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.SymbolValueCountCollector;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;

import static io.questdb.cairo.TableUtils.charFileName;
import static io.questdb.cairo.TableUtils.offsetFileName;

/**
 * The {@code _todo_} entry that a TRUNCATE of an earlier build armed before it was interrupted. TRUNCATE no longer
 * arms one, see {@code TableWriter.truncate0()}, but the writer open still completes the entry it finds. Those builds
 * armed the same entry with and without KEEP SYMBOL MAPS; without it, the TRUNCATE emptied the symbol maps before it
 * armed the entry. The entry is stamped with the live {@code _txn} txn, the one the interrupted TRUNCATE started from:
 * the open acts on it only then.
 */
final class LegacyTruncateTodo {
    private static final Log LOG = LogFactory.getLog(LegacyTruncateTodo.class);
    // TableUtils.TODO_TRUNCATE, the code of the entry with and without KEEP SYMBOL MAPS
    private static final int TRUNCATE = 1;

    private LegacyTruncateTodo() {
    }

    /**
     * Arms the entry of an interrupted TRUNCATE of {@code table}, which must have no writer open, and returns the
     * {@code _txn} txn it is stamped with. The symbol maps of {@code emptiedColumns}, or of every SYMBOL column when
     * none is named, are left in {@code symbolMapState}, the others intact. No query may open a SYMBOL column of an
     * emptied map before a writer open completes the TRUNCATE: {@code _txn} still counts the symbols the map held.
     */
    static long arm(
            CairoEngine engine,
            String table,
            int partitionBy,
            SymbolMapState symbolMapState,
            String... emptiedColumns
    ) {
        final TableToken token = engine.verifyTableName(table);
        if (symbolMapState == SymbolMapState.INTACT) {
            Assert.assertEquals("intact symbol maps, but columns to empty", 0, emptiedColumns.length);
        } else {
            emptySymbolMaps(engine, token, symbolMapState == SymbolMapState.EMPTIED_KILLED, emptiedColumns);
        }
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
            mem.putLong(40, TRUNCATE);
            mem.jumpTo(48);
        }
        return txn;
    }

    private static void emptySymbolMaps(
            CairoEngine engine,
            TableToken token,
            boolean isKilled,
            String[] emptiedColumns
    ) {
        // The writer of the interrupted TRUNCATE held each map open with the count _txn names. Read the counts, and the
        // name txns the maps live under, before any map is empty: a reader of an emptied map that _txn counts symbols
        // in trips an assertion in SymbolMapReaderImpl.
        final ObjList<String> names = new ObjList<>();
        final LongList nameTxns = new LongList();
        final IntList symbolCounts = new IntList();
        try (TableReader reader = engine.getReader(token)) {
            final TableReaderMetadata metadata = reader.getMetadata();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                    final String name = metadata.getColumnName(i);
                    if (emptiedColumns.length == 0 || isNamed(emptiedColumns, name)) {
                        names.add(name);
                        nameTxns.add(reader.getColumnVersionReader().getSymbolTableNameTxn(metadata.getWriterIndex(i)));
                        symbolCounts.add(reader.getSymbolMapReader(i).getSymbolCount());
                    }
                }
            }
        }
        engine.releaseAllReaders();
        if (emptiedColumns.length > 0) {
            Assert.assertEquals("SYMBOL columns to empty", emptiedColumns.length, names.size());
        }

        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path().of(configuration.getDbRoot()).concat(token)) {
            final int tableDirLen = path.size();
            for (int i = 0, n = names.size(); i < n; i++) {
                final String name = names.getQuick(i);
                final long nameTxn = nameTxns.getQuick(i);
                final long offsetFileSize;
                final long charFileSize;
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path.trimTo(tableDirLen),
                                name,
                                nameTxn,
                                symbolCounts.getQuick(i),
                                -1,
                                SymbolValueCountCollector.NOOP,
                                -1
                        )
                ) {
                    writer.truncate();
                    offsetFileSize = ff.length(offsetFileName(path.trimTo(tableDirLen), name, nameTxn));
                    charFileSize = ff.length(charFileName(path.trimTo(tableDirLen), name, nameTxn));
                }
                if (isKilled) {
                    // The process died with the map open: its files keep the size the emptied map had, which the
                    // close above cut. Both hold zeros past the header.
                    extend(configuration, offsetFileName(path.trimTo(tableDirLen), name, nameTxn), offsetFileSize);
                    extend(configuration, charFileName(path.trimTo(tableDirLen), name, nameTxn), charFileSize);
                }
            }
        }
    }

    // Extends the file with zeros
    private static void extend(CairoConfiguration configuration, LPSZ file, long size) {
        final FilesFacade ff = configuration.getFilesFacade();
        final long fd = TableUtils.openRW(ff, file, LOG, configuration.getWriterFileOpenOpts());
        try {
            Assert.assertTrue("the close grew the file", ff.length(fd) <= size);
            Assert.assertTrue(ff.truncate(fd, size));
        } finally {
            ff.close(fd);
        }
    }

    private static boolean isNamed(String[] columns, CharSequence name) {
        for (String column : columns) {
            if (Chars.equalsIgnoreCase(column, name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The symbol maps an interrupted TRUNCATE of an earlier build left behind its {@code _todo_} entry.
     */
    enum SymbolMapState {
        // TRUNCATE ... KEEP SYMBOL MAPS: the maps hold the symbols _txn counts
        INTACT,
        // TRUNCATE without KEEP SYMBOL MAPS killed after it armed the entry: the emptied maps as the open writer left
        // them, header and zeros
        EMPTIED_KILLED,
        // TRUNCATE without KEEP SYMBOL MAPS that failed after it armed the entry, and the pool closed its writer: the
        // emptied maps as the writer close cut them
        EMPTIED_CLOSED
    }
}
