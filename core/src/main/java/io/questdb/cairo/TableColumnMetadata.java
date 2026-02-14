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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import org.jetbrains.annotations.Nullable;

public class TableColumnMetadata implements Plannable {
    @Nullable
    private final RecordMetadata metadata;
    private final int replacingIndex;
    private final int symbolCapacity;
    private final boolean symbolTableStatic;
    private final int writerIndex;
    private String columnName;
    private int columnType;
    private boolean dedupKeyFlag;
    private int indexValueBlockCapacity;
    private boolean symbolCacheFlag;
    private boolean symbolIndexFlag;

    public TableColumnMetadata(String columnName, int columnType) {
        this(columnName, columnType, null);
    }

    public TableColumnMetadata(String columnName, int columnType, @Nullable RecordMetadata metadata) {
        this(
                columnName,
                columnType,
                false,
                0,
                false,
                metadata,
                -1,
                false,
                0,
                true,
                0
        );
        // Do not allow using this constructor for symbol types.
        // Use version where you specify symbol table parameters
        assert !ColumnType.isSymbol(columnType);
    }

    public TableColumnMetadata(String columnName, int columnType, @Nullable RecordMetadata metadata, boolean symbolTableStatic, boolean symbolCacheFlag, int symbolCapacity) {
        this(
                columnName,
                columnType,
                false,
                0,
                symbolTableStatic,
                metadata,
                -1,
                false,
                0,
                symbolCacheFlag,
                symbolCapacity
        );
    }

    public TableColumnMetadata(
            String columnName,
            int columnType,
            boolean indexFlag,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            @Nullable RecordMetadata metadata
    ) {
        this(
                columnName,
                columnType,
                indexFlag,
                indexValueBlockCapacity,
                symbolTableStatic,
                metadata,
                -1,
                false,
                0,
                true,
                0
        );
    }

    public TableColumnMetadata(
            String columnName,
            int columnType,
            boolean symbolIndexFlag,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            @Nullable RecordMetadata metadata,
            int writerIndex,
            boolean dedupKeyFlag
    ) {
        this(
                columnName,
                columnType,
                symbolIndexFlag,
                indexValueBlockCapacity,
                symbolTableStatic,
                metadata,
                writerIndex,
                dedupKeyFlag,
                0,
                true,
                0
        );
    }

    public TableColumnMetadata(
            String columnName,
            int columnType,
            boolean symbolIndexFlag,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            @Nullable RecordMetadata metadata,
            int writerIndex,
            boolean dedupKeyFlag,
            int replacingIndex,
            boolean symbolCacheFlag,
            int symbolCapacity
    ) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.symbolIndexFlag = symbolIndexFlag;
        this.indexValueBlockCapacity = indexValueBlockCapacity;
        this.symbolTableStatic = symbolTableStatic;
        this.metadata = GenericRecordMetadata.copyOf(metadata);
        this.writerIndex = writerIndex;
        this.dedupKeyFlag = dedupKeyFlag;
        this.replacingIndex = replacingIndex;
        this.symbolCacheFlag = symbolCacheFlag;
        this.symbolCapacity = symbolCapacity;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getColumnType() {
        return columnType;
    }

    public int getIndexValueBlockCapacity() {
        return indexValueBlockCapacity;
    }

    @Nullable
    public RecordMetadata getMetadata() {
        return metadata;
    }

    public int getReplacingIndex() {
        return replacingIndex;
    }

    public int getSymbolCapacity() {
        return symbolCapacity;
    }

    public int getWriterIndex() {
        return writerIndex;
    }

    public boolean isDedupKeyFlag() {
        return dedupKeyFlag;
    }

    public boolean isDeleted() {
        return columnType < 0;
    }

    public boolean isSymbolCacheFlag() {
        return symbolCacheFlag;
    }

    public boolean isSymbolIndexFlag() {
        return symbolIndexFlag;
    }

    public boolean isSymbolTableStatic() {
        return symbolTableStatic;
    }

    public void markDeleted() {
        columnType = -Math.abs(columnType);
    }

    public void rename(String name) {
        this.columnName = name;
    }

    public void setDedupKeyFlag(boolean dedupKeyFlag) {
        this.dedupKeyFlag = dedupKeyFlag;
    }

    public void setIndexValueBlockCapacity(int indexValueBlockCapacity) {
        this.indexValueBlockCapacity = indexValueBlockCapacity;
    }

    public void setSymbolCacheFlag(boolean cache) {
        this.symbolCacheFlag = cache;
    }

    public void setSymbolIndexFlag(boolean value) {
        symbolIndexFlag = value;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(columnName);
    }
}
