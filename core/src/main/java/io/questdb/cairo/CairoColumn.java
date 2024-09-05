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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

public class CairoColumn implements Sinkable {
    public static final Log LOG = LogFactory.getLog(CairoEngine.class);
    private int indexBlockCapacity;
    private boolean isDedupKey;
    private boolean isDesignated;
    private boolean isIndexed;
    private boolean isSymbolTableStatic;
    private long metadataVersion;
    private CharSequence name;
    private int position;
    private boolean symbolCached;
    private int symbolCapacity;
    private int type;
    private int writerIndex;

    public CairoColumn() {
    }

    public void copyTo(@NotNull CairoColumn target) {
        target.isDesignated = this.isDesignated;
        target.indexBlockCapacity = this.indexBlockCapacity;
        target.isDedupKey = this.isDedupKey;
        target.isIndexed = this.isIndexed;
        target.isSymbolTableStatic = this.isSymbolTableStatic;
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

    public boolean getIsDedupKey() {
        return isDedupKey;
    }

    public boolean getIsDesignated() {
        return isDesignated;
    }

    public boolean getIsIndexed() {
        return isIndexed;
    }

    public boolean getIsSymbolTableStatic() {
        return isSymbolTableStatic;
    }

    public CharSequence getName() {
        return name;
    }

    public int getPosition() {
        return position;
    }

    public boolean getSymbolCached() {
        return symbolCached;
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

    public void setIndexBlockCapacity(int indexBlockCapacity) {
        this.indexBlockCapacity = indexBlockCapacity;
    }

    public void setIsDedupKey(boolean isDedupKey) {
        this.isDedupKey = isDedupKey;
    }

    public void setIsDesignated(boolean isDesignated) {
        this.isDesignated = isDesignated;
    }

    public void setIsIndexed(boolean isIndexed) {
        this.isIndexed = isIndexed;
    }

    public void setIsSymbolTableStatic(boolean symbolTableStatic) {
        isSymbolTableStatic = symbolTableStatic;
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
        sink.put("isDedupKey=").put(getIsDedupKey()).put(", ");
        sink.put("isDesignated=").put(getIsDesignated()).put(", ");
        sink.put("isSymbolTableStatic=").put(getIsSymbolTableStatic()).put(", ");
        sink.put("symbolCached=").put(getSymbolCached()).put(", ");
        sink.put("symbolCapacity=").put(getSymbolCapacity()).put(", ");
        sink.put("isIndexed=").put(getIsIndexed()).put(", ");
        sink.put("indexBlockCapacity=").put(getIndexBlockCapacity()).put(", ");
        sink.put("writerIndex=").put(getWriterIndex()).put("]");
    }

}
