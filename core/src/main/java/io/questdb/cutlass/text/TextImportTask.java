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

package io.questdb.cutlass.text;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableWriter;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;

public class TextImportTask {

    public static final byte PHASE_BOUNDARY_CHECK = 1;
    public static final byte PHASE_INDEXING = 2;
    public static final byte PHASE_PARTITION_IMPORT = 3;
    public static final byte PHASE_SYMBOL_TABLE_MERGE = 4;
    public static final byte PHASE_UPDATE_SYMBOL_KEYS = 5;

    private CountDownLatchSPI doneLatch;
    private byte phase;
    private FileIndexer.TaskContext context;
    private int index;
    private long lo;
    private long hi;
    private long lineNumber;
    private LongList chunkStats;
    private LongList partitionKeys;
    private ObjList<CharSequence> partitionNames;
    private long partitionSize;
    private long partitionTimestamp;
    private CharSequence symbolColumnName;
    private int symbolCount;
    private CairoConfiguration cfg;
    private CharSequence tableName;
    private int columnIndex;
    private int tmpTableCount;
    private int partitionBy;
    private TableWriter writer;
    private int symbolColumnIndex;
    private CharSequence importRoot;

    public void of(
            CountDownLatchSPI doneLatch,
            byte phase,
            FileIndexer.TaskContext context,
            int index,
            long lo,
            long hi,
            long lineNumber,
            LongList chunkStats,
            LongList partitionKeys
    ) {
        this.doneLatch = doneLatch;
        this.phase = phase;
        this.context = context;
        this.index = index;
        this.lo = lo;
        this.hi = hi;
        this.lineNumber = lineNumber;
        this.chunkStats = chunkStats;
        this.partitionKeys = partitionKeys;
    }

    public void of(
            CountDownLatchSPI doneLatch,
            byte phase,
            FileIndexer.TaskContext context,
            int index,
            long lo,
            long hi,
            ObjList<CharSequence> partitionNames
    ) {
        this.doneLatch = doneLatch;
        this.phase = phase;
        this.context = context;
        this.index = index;
        this.lo = lo;
        this.hi = hi;
        this.partitionNames = partitionNames;
    }

    public void of(CountDownLatchSPI doneLatch,
                   byte phase,
                   FileIndexer.TaskContext context,
                   int index,
                   long partitionSize,
                   long partitionTimestamp,
                   CharSequence symbolColumnName,
                   int symbolCount) {
        this.doneLatch = doneLatch;
        this.phase = phase;
        this.context = context;
        this.index = index;
        this.partitionSize = partitionSize;
        this.partitionTimestamp = partitionTimestamp;
        this.symbolColumnName = symbolColumnName;
        this.symbolCount = symbolCount;
    }

    public void of(CountDownLatchSPI doneLatch,
                   byte phase,
                   final CharSequence importRoot,
                   final CairoConfiguration cfg,
                   final TableWriter writer,
                   final CharSequence tableName,
                   final CharSequence symbolColumnName,
                   int columnIndex,
                   int symbolColumnIndex,
                   int tmpTableCount,
                   int partitionBy
    ) {
        this.doneLatch = doneLatch;
        this.phase = phase;
        this.cfg = cfg;
        this.writer = writer;
        this.tableName = tableName;
        this.symbolColumnName = symbolColumnName;
        this.columnIndex = columnIndex;
        this.symbolColumnIndex = symbolColumnIndex;
        this.tmpTableCount = tmpTableCount;
        this.partitionBy = partitionBy;
        this.importRoot = importRoot;
    }

    public boolean run() {
        try {
            if (phase == PHASE_BOUNDARY_CHECK) {
                context.countQuotesStage(index, lo, hi, chunkStats);
            } else if (phase == PHASE_INDEXING) {
                context.buildIndexStage(lo, hi, lineNumber, chunkStats, index, partitionKeys);
            } else if (phase == PHASE_PARTITION_IMPORT) {
                context.importPartitionStage(index, lo, hi, partitionNames);
            } else if (phase == PHASE_SYMBOL_TABLE_MERGE) {
                FileIndexer.mergeColumnSymbolTables(cfg, importRoot, writer, tableName, symbolColumnName, columnIndex, symbolColumnIndex, tmpTableCount, partitionBy);
            } else if (phase == PHASE_UPDATE_SYMBOL_KEYS) {
                context.updateSymbolKeys(index, partitionSize, partitionTimestamp, symbolColumnName, symbolCount);
            } else {
                throw TextException.$("Unexpected phase ").put(phase);
            }
        } catch (Throwable t) {
            t.printStackTrace();//TODO: how can we react to job failing
            return false;
        } finally {
            doneLatch.countDown();
        }
        return true;
    }
}
