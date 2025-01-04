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

import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.std.Misc;
import org.jetbrains.annotations.TestOnly;

public abstract class AbstractFullPartitionFrameCursor implements PartitionFrameCursor {
    protected final FullTablePartitionFrame frame = new FullTablePartitionFrame();
    protected PartitionDecoder parquetDecoder;
    protected int partitionHi;
    protected int partitionIndex;
    protected TableReader reader;

    @Override
    public void close() {
        // avoid double-close in case of cursor not closing the reader, query progress catching the leak
        // and then factory is trying to close the cursor and the reader
        if (reader != null && reader.isActive()) {
            reader = Misc.free(reader);
        }
        Misc.free(parquetDecoder);
    }

    @Override
    public SymbolMapReader getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    @Override
    public TableReader getTableReader() {
        return reader;
    }

    @Override
    public StaticSymbolTable newSymbolTable(int columnIndex) {
        return reader.newSymbolTable(columnIndex);
    }

    public PartitionFrameCursor of(TableReader reader) {
        partitionHi = reader.getPartitionCount();
        toTop();
        this.reader = reader;
        return this;
    }

    @TestOnly
    @Override
    public boolean reload() {
        boolean moreData = reader.reload();
        partitionHi = reader.getPartitionCount();
        toTop();
        return moreData;
    }

    @Override
    public long size() {
        return reader.size();
    }

    protected static class FullTablePartitionFrame implements PartitionFrame {
        protected byte format;
        protected PartitionDecoder parquetDecoder;
        protected int partitionIndex;
        protected long rowHi;
        protected long rowLo;

        @Override
        public PartitionDecoder getParquetDecoder() {
            return parquetDecoder;
        }

        @Override
        public byte getPartitionFormat() {
            return format;
        }

        @Override
        public int getPartitionIndex() {
            return partitionIndex;
        }

        @Override
        public long getRowHi() {
            return rowHi;
        }

        @Override
        public long getRowLo() {
            return rowLo;
        }
    }
}
