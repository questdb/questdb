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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

public class CairoColumn implements Sinkable {
    public static final Log LOG = LogFactory.getLog(CairoEngine.class);
    private boolean dedupKey;
    private boolean designated;
    private int indexBlockCapacity;
    private byte indexType;
    private long metadataVersion;
    private CharSequence name;
    private int position;
    private boolean symbolCached;
    private int symbolCapacity;
    private boolean symbolTableStatic;
    private int type;
    private int writerIndex;

    public CairoColumn() {
    }

    public void copyTo(@NotNull CairoColumn target) {
        target.designated = this.designated;
        target.indexBlockCapacity = this.indexBlockCapacity;
        target.dedupKey = this.dedupKey;
        target.indexType = this.indexType;
        target.symbolTableStatic = this.symbolTableStatic;
        target.name = this.name;
        target.position = this.position;
        target.symbolCached = this.symbolCached;
        target.symbolCapacity = this.symbolCapacity;
        target.type = this.type;
        target.writerIndex = this.writerIndex;
        target.metadataVersion = this.metadataVersion;
    }

    public int getIndexBlockCapacity() {
        return indexBlockCapacity;
    }

    public byte getIndexType() {
        return indexType;
    }

    public CharSequence getName() {
        return name;
    }

    public int getPosition() {
        return position;
    }

    public int getSymbolCapacity() {
        return symbolCapacity;
    }

    public int getType() {
        return type;
    }

    public int getWriterIndex() {
        return writerIndex;
    }

    public boolean isDedupKey() {
        return dedupKey;
    }

    public boolean isDesignated() {
        return designated;
    }

    public boolean isIndexed() {
        return IndexType.isIndexed(indexType);
    }

    public boolean isSymbolCached() {
        return symbolCached;
    }

    public boolean isSymbolTableStatic() {
        return symbolTableStatic;
    }

    public void setDedupKeyFlag(boolean dedupKey) {
        this.dedupKey = dedupKey;
    }

    public void setDesignatedFlag(boolean designated) {
        this.designated = designated;
    }

    public void setIndexBlockCapacity(int indexBlockCapacity) {
        this.indexBlockCapacity = indexBlockCapacity;
    }

    public void setIndexType(byte indexType) {
        this.indexType = indexType;
    }

    public void setName(CharSequence name) {
        this.name = name;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public void setSymbolCached(boolean symbolCached) {
        this.symbolCached = symbolCached;
    }

    public void setSymbolCapacity(int symbolCapacity) {
        this.symbolCapacity = symbolCapacity;
    }

    public void setSymbolTableStaticFlag(boolean symbolTableStatic) {
        this.symbolTableStatic = symbolTableStatic;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setWriterIndex(int writerIndex) {
        this.writerIndex = writerIndex;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put("CairoColumn [");
        sink.put("name=").put(getName()).put(", ");
        sink.put("position=").put(getPosition()).put(", ");
        sink.put("type=").put(ColumnType.nameOf(getType())).put(", ");
        sink.put("isDedupKey=").put(isDedupKey()).put(", ");
        sink.put("isDesignated=").put(isDesignated()).put(", ");
        sink.put("isSymbolTableStatic=").put(isSymbolTableStatic()).put(", ");
        sink.put("symbolCached=").put(isSymbolCached()).put(", ");
        sink.put("symbolCapacity=").put(getSymbolCapacity()).put(", ");
        sink.put("indexType=").put(IndexType.nameOf(getIndexType())).put(", ");
        sink.put("indexBlockCapacity=").put(getIndexBlockCapacity()).put(", ");
        sink.put("writerIndex=").put(getWriterIndex()).put("]");
    }
}
