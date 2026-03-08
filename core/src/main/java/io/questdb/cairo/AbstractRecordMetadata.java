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

package io.questdb.cairo;


import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

public abstract class AbstractRecordMetadata implements RecordMetadata, Mutable {
    protected final ObjList<TableColumnMetadata> columnMetadata = new ObjList<>();
    protected final LowerCaseCharSequenceIntHashMap columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    protected int columnCount;
    protected int timestampIndex = -1;

    @Override
    public void clear() {
        columnMetadata.clear();
        columnNameIndexMap.clear();
        columnCount = 0;
        timestampIndex = -1;
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getColumnIndexQuiet(CharSequence columnName, int lo, int hi) {
        final int index = columnNameIndexMap.keyIndex(columnName, lo, hi);
        if (index < 0) {
            return columnNameIndexMap.valueAt(index);
        }
        return -1;
    }

    @Override
    public TableColumnMetadata getColumnMetadata(int index) {
        return columnMetadata.getQuick(index);
    }

    @Override
    public String getColumnName(int columnIndex) {
        return getColumnMetadata(columnIndex).getColumnName();
    }

    @Override
    public int getColumnType(int columnIndex) {
        return getColumnMetadata(columnIndex).getColumnType();
    }

    @Override
    public int getIndexValueBlockCapacity(int columnIndex) {
        return getColumnMetadata(columnIndex).getIndexValueBlockCapacity();
    }

    @Override
    public RecordMetadata getMetadata(int columnIndex) {
        return getColumnMetadata(columnIndex).getMetadata();
    }

    @Override
    public int getTimestampIndex() {
        return timestampIndex;
    }

    @Override
    public int getWriterIndex(int columnIndex) {
        return getColumnMetadata(columnIndex).getWriterIndex();
    }

    @Override
    public boolean hasColumn(int columnIndex) {
        final TableColumnMetadata columnMeta = columnMetadata.getQuiet(columnIndex);
        return columnMeta != null && !columnMeta.isDeleted();
    }

    @Override
    public boolean isColumnIndexed(int columnIndex) {
        return getColumnMetadata(columnIndex).isSymbolIndexFlag();
    }

    @Override
    public boolean isDedupKey(int columnIndex) {
        return getColumnMetadata(columnIndex).isDedupKeyFlag();
    }

    @Override
    public boolean isSymbolTableStatic(int columnIndex) {
        return getColumnMetadata(columnIndex).isSymbolTableStatic();
    }
}
