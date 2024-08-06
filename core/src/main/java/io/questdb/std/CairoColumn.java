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

package io.questdb.std;

import io.questdb.cairo.TableColumnMetadata;
import org.jetbrains.annotations.NotNull;

public class CairoColumn {
    private int denseSymbolIndex;
    private boolean designated;
    private int indexBlockCapacity;
    private boolean isDedupKey;
    private boolean isIndexed;
    private boolean isSymbolTableStatic;
    private CharSequence name;
    private int position;
    private int stableIndex;
    private boolean symbolCached;
    private int symbolCapacity;
    private int type;
    private int writerIndex;

    public CairoColumn() {
    }

    public CairoColumn(@NotNull TableColumnMetadata metadata, boolean designated, int position) {
        updateMetadata(metadata, designated, position);
    }

    public void copyTo(@NotNull CairoColumn target) {
        target.denseSymbolIndex = this.denseSymbolIndex;
        target.designated = this.designated;
        target.indexBlockCapacity = this.indexBlockCapacity;
        target.isDedupKey = this.isDedupKey;
        target.isIndexed = this.isIndexed;
        target.isSymbolTableStatic = this.isSymbolTableStatic;
        target.name = this.name;
        target.position = this.position;
        target.stableIndex = this.stableIndex;
        target.symbolCached = this.symbolCached;
        target.symbolCapacity = this.symbolCapacity;
        target.type = this.type;
        target.writerIndex = this.writerIndex;
    }

    public int getDenseSymbolIndexUnsafe() {
        return denseSymbolIndex;
    }

    public boolean getDesignated() {
        return designated;
    }

    public boolean getDesignatedUnsafe() {
        return designated;
    }

    public int getIndexBlockCapacityUnsafe() {
        return indexBlockCapacity;
    }

    public boolean getIsDedupKeyUnsafe() {
        return isDedupKey;
    }

    public boolean getIsIndexedUnsafe() {
        return isIndexed;
    }

    public boolean getIsSymbolTableStaticUnsafe() {
        return isSymbolTableStatic;
    }

    public CharSequence getNameUnsafe() {
        return name;
    }

    public int getPositionUnsafe() {
        return position;
    }

    public int getStableIndex() {
        return stableIndex;
    }

    public boolean getSymbolCachedUnsafe() {
        return symbolCached;
    }

    public int getSymbolCapacityUnsafe() {
        return symbolCapacity;
    }

    public int getTypeUnsafe() {
        return type;
    }

    // todo: review naming
    public boolean getUpsertKeyUnsafe() {
        return isDedupKey;
    }

    public int getWriterIndexUnsafe() {
        return writerIndex;
    }

    public void setDenseSymbolIndexUnsafe(int denseSymbolIndex) {
        this.denseSymbolIndex = denseSymbolIndex;
    }

    public void setDesignated(boolean designated) {
        this.designated = designated;
    }

    public void setDesignatedUnsafe(boolean designated) {
        this.designated = designated;
    }

    public void setIndexBlockCapacityUnsafe(int indexBlockCapacity) {
        this.indexBlockCapacity = indexBlockCapacity;
    }

    public void setIsDedupKeyUnsafe(boolean isDedupKey) {
        this.isDedupKey = isDedupKey;
    }

    public void setIsIndexedUnsafe(boolean isIndexed) {
        this.isIndexed = isIndexed;
    }

    public void setIsSymbolTableStaticUnsafe(boolean symbolTableStatic) {
        isSymbolTableStatic = symbolTableStatic;
    }

    public void setNameUnsafe(CharSequence name) {
        this.name = name;
    }

    public void setPositionUnsafe(int position) {
        this.position = position;
    }

    public void setStableIndex(int stableIndex) {
        this.stableIndex = stableIndex;
    }

    public void setSymbolCachedUnsafe(boolean symbolCached) {
        this.symbolCached = symbolCached;
    }

    public void setSymbolCapacityUnsafe(int symbolCapacity) {
        this.symbolCapacity = symbolCapacity;
    }

    public void setTypeUnsafe(int type) {
        this.type = type;
    }

    public void setUpsertKeyUnsafe(boolean upsertKey) {
        this.isDedupKey = upsertKey;
    }

    public void setWriterIndexUnsafe(int writerIndex) {
        this.writerIndex = writerIndex;
    }

    public void updateMetadata(@NotNull TableColumnMetadata tableColumnMetadata, boolean designated, int position) {
        name = tableColumnMetadata.getName();
        type = tableColumnMetadata.getType();
        isDedupKey = tableColumnMetadata.isDedupKey();
        isIndexed = tableColumnMetadata.isIndexed();
        indexBlockCapacity = tableColumnMetadata.getIndexValueBlockCapacity();
        this.designated = designated;
        this.position = position;


//        private boolean symbolCached;
//        private int symbolCapacity;

    }
}
