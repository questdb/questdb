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

package io.questdb.cairo;

import io.questdb.std.SimpleReadWriteLock;
import org.jetbrains.annotations.NotNull;


// For show tables
// require id, designatedTimestamp, partitionBy, maxUncommittedRows, o3MaxLag, walEnabled, directoryName, dedup


// designated timestamp and partition by are final
public class CairoTable {
    private TableToken token;
    private SimpleReadWriteLock tokenLock;

    public CairoTable(@NotNull TableToken token) {
        this.tokenLock = new SimpleReadWriteLock();
        this.token = token;
    }

    
    public @NotNull String getName() {
        tokenLock.readLock().lock();
        final String tbl = token.getTableName();
        tokenLock.readLock().unlock();
        return tbl;
    }

    /**
     * Get table token, not thread safe.
     *
     * @return
     */
    private TableToken getTokenUnsafe() {
        return token;
    }

//
//    @NotNull
//    private final GcUtf8String dirName;
//    private final boolean isProtected;
//    private final boolean isPublic;
//    private final boolean isSystem;
//    private final boolean isWal;
//    private final int tableId;
//    @NotNull
//    private final String tableName;

}


//private static final Log LOG = LogFactory.getLog(TableReader.class);
//private static final int PARTITIONS_SLOT_OFFSET_COLUMN_VERSION = 3;
//private static final int PARTITIONS_SLOT_OFFSET_NAME_TXN = 2;
//private static final int PARTITIONS_SLOT_OFFSET_SIZE = 1;
//private static final int PARTITIONS_SLOT_SIZE = 4;
//private static final int PARTITIONS_SLOT_SIZE_MSB = Numbers.msb(PARTITIONS_SLOT_SIZE);
//private final MillisecondClock clock;
//private final ColumnVersionReader columnVersionReader;
//private final CairoConfiguration configuration;
//private final int dbRootSize;
//private final FilesFacade ff;
//private final int maxOpenPartitions;
//private final MessageBus messageBus;
//private final TableReaderMetadata metadata;
//private final LongList openPartitionInfo;
//private final int partitionBy;
//private final Path path;
//private final TableReaderRecordCursor recordCursor = new TableReaderRecordCursor();
//private final int rootLen;
//private final ObjList<SymbolMapReader> symbolMapReaders = new ObjList<>();
//private final MemoryMR todoMem = Vm.getCMRInstance();
//private final TxReader txFile;
//private final TxnScoreboard txnScoreboard;
//private ObjList<BitmapIndexReader> bitmapIndexes;
//private int columnCount;
//private int columnCountShl;
//private LongList columnTops;
//private ObjList<MemoryCMR> columns;
//private int openPartitionCount;
//private int partitionCount;
//private long rowCount;
//private TableToken tableToken;
//private long tempMem8b = Unsafe.malloc(8, MemoryTag.NATIVE_TABLE_READER);
//private long txColumnVersion = -1;
//private long txPartitionVersion = -1;
//private long txTruncateVersion = -1;
//private long txn = TableUtils.INITIAL_TXN;
//private boolean txnAcquired = false;