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

import io.questdb.cairo.sql.RecordMetadata;
import org.jetbrains.annotations.Nullable;

public class TableColumnMetadata {
    @Nullable
    private final RecordMetadata metadata;
    private final boolean symbolTableStatic;
    private final int writerIndex;
    private int indexValueBlockCapacity;
    private boolean indexed;
    private String name;
    private int type;

    public TableColumnMetadata(String name, int type) {
        this(name, type, null);
    }

    public TableColumnMetadata(String name, int type, @Nullable RecordMetadata metadata) {
        this(name, type, false, 0, false, metadata, -1);
        // Do not allow using this constructor for symbol types.
        // Use version where you specify symbol table parameters
        assert !ColumnType.isSymbol(type);
    }

    public TableColumnMetadata(
            String name,
            int type,
            boolean indexFlag,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            @Nullable RecordMetadata metadata
    ) {
        this(name, type, indexFlag, indexValueBlockCapacity, symbolTableStatic, metadata, -1);
    }

    public TableColumnMetadata(
            String name,
            int type,
            boolean indexFlag,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            @Nullable RecordMetadata metadata,
            int writerIndex
    ) {
        this.name = name;
        this.type = type;
        this.indexed = indexFlag;
        this.indexValueBlockCapacity = indexValueBlockCapacity;
        this.symbolTableStatic = symbolTableStatic;
        this.metadata = GenericRecordMetadata.copyOf(metadata);
        this.writerIndex = writerIndex;
    }

    public int getIndexValueBlockCapacity() {
        return indexValueBlockCapacity;
    }

    @Nullable
    public RecordMetadata getMetadata() {
        return metadata;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public int getWriterIndex() {
        return writerIndex;
    }

    public boolean isDeleted() {
        return type < 0;
    }

    public boolean isIndexed() {
        return indexed;
    }

    public boolean isSymbolTableStatic() {
        return symbolTableStatic;
    }

    public void markDeleted() {
        type = -Math.abs(type);
    }

    public void setIndexValueBlockCapacity(int indexValueBlockCapacity) {
        this.indexValueBlockCapacity = indexValueBlockCapacity;
    }

    public void setIndexed(boolean value) {
        indexed = value;
    }

    public void setName(String name) {
        this.name = name;
    }
}
