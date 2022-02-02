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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RowCursor;

public class SymbolIndexRowCursorFactory implements SymbolFunctionRowCursorFactory {
    private final int columnIndex;
    private int symbolKey;
    private final boolean cachedIndexReaderCursor;
    private final int indexDirection;
    private final Function symbolFunction;

    public SymbolIndexRowCursorFactory(
            int columnIndex,
            int symbolKey,
            boolean cachedIndexReaderCursor,
            int indexDirection,
            Function symbolFunction
    ) {
        this.columnIndex = columnIndex;
        this.symbolKey = TableUtils.toIndexKey(symbolKey);
        this.cachedIndexReaderCursor = cachedIndexReaderCursor;
        this.indexDirection = indexDirection;
        this.symbolFunction = symbolFunction;
    }

    @Override
    public void of(int symbolKey) {
        this.symbolKey = TableUtils.toIndexKey(symbolKey);
    }

    @Override
    public RowCursor getCursor(DataFrame dataFrame) {
        return dataFrame
                .getBitmapIndexReader(columnIndex, indexDirection)
                .getCursor(cachedIndexReaderCursor, symbolKey, dataFrame.getRowLo(), dataFrame.getRowHi() - 1);
    }

    @Override
    public boolean isEntity() {
        return false;
    }

    @Override
    public Function getFunction() {
        return symbolFunction;
    }
}
