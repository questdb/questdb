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
    private final int writerIndex;
    private final long hash;
    private final boolean symbolTableStatic;
    @Nullable
    private final RecordMetadata metadata;
    private int type;
    private String name;
    private int indexValueBlockCapacity;
    private boolean indexed;

    public TableColumnMetadata(String name, long hash, int type) {
        this(name, hash, type, null);
    }

    public TableColumnMetadata(String name, long hash, int type, @Nullable RecordMetadata metadata) {
        this(name, hash, type, false, 0, false, metadata, -1);
        // Do not allow using this constructor for symbol types.
        // Use version where you specify symbol table parameters
        assert !ColumnType.isSymbol(type);
    }

    public TableColumnMetadata(
            String name,
            long hash,
            int type,
            boolean indexFlag,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            @Nullable RecordMetadata metadata
    ) {
        this(name, hash, type, indexFlag, indexValueBlockCapacity, symbolTableStatic, metadata, -1);
    }

    public TableColumnMetadata(
            String name,
            long hash,
            int type,
            boolean indexFlag,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            @Nullable RecordMetadata metadata,
            int writerIndex
    ) {
        this.name = name;
        this.hash = hash;
        this.type = type;
        this.indexed = indexFlag;
        this.indexValueBlockCapacity = indexValueBlockCapacity;
        this.symbolTableStatic = symbolTableStatic;
        this.metadata = GenericRecordMetadata.copyOf(metadata);
        this.writerIndex = writerIndex;
    }

    public long getHash() {
        return hash;
    }

    public int getIndexValueBlockCapacity() {
        return indexValueBlockCapacity;
    }

    public int getWriterIndex() {
        return writerIndex;
    }

    public void markDeleted() {
        type = Math.abs(type);
    }

    public void setIndexValueBlockCapacity(int indexValueBlockCapacity) {
        this.indexValueBlockCapacity = indexValueBlockCapacity;
    }

    @Nullable
    public RecordMetadata getMetadata() {
        return metadata;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public boolean isIndexed() {
        return indexed;
    }

    public void setIndexed(boolean value) {
        indexed = value;
    }

    public boolean isSymbolTableStatic() {
        return symbolTableStatic;
    }
}
