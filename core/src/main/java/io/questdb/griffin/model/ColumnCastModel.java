/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.model;

import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;

public class ColumnCastModel implements Mutable {
    public static final ObjectFactory<ColumnCastModel> FACTORY = ColumnCastModel::new;
    private int columnNamePos;
    private int columnType;
    private int columnTypePos;
    private int indexValueBlockSize;
    private boolean indexed;
    private ExpressionNode name;
    private boolean symbolCacheFlag;
    private int symbolCapacity;

    private ColumnCastModel() {
    }

    @Override
    public void clear() {
        symbolCapacity = 0;
    }

    public int getColumnNamePos() {
        return columnNamePos;
    }

    public int getColumnType() {
        return columnType;
    }

    public int getColumnTypePos() {
        return columnTypePos;
    }

    public int getIndexValueBlockSize() {
        return indexValueBlockSize;
    }

    public ExpressionNode getName() {
        return name;
    }

    public boolean getSymbolCacheFlag() {
        return symbolCacheFlag;
    }

    public int getSymbolCapacity() {
        return symbolCapacity;
    }

    public boolean isIndexed() {
        return indexed;
    }

    public void setIndexValueBlockSize(int indexValueBlockSize) {
        this.indexValueBlockSize = indexValueBlockSize;
    }

    public void setIndexed(boolean indexed) {
        this.indexed = indexed;
    }

    public void setName(ExpressionNode name) {
        this.name = name;
    }

    public void setSymbolCacheFlag(boolean symbolCacheFlag) {
        this.symbolCacheFlag = symbolCacheFlag;
    }

    public void setSymbolCapacity(int symbolCapacity) {
        this.symbolCapacity = symbolCapacity;
    }

    public void setType(int columnType, int columnNamePos, int columnTypePos) {
        this.columnType = columnType;
        this.columnNamePos = columnNamePos;
        this.columnTypePos = columnTypePos;
    }
}
