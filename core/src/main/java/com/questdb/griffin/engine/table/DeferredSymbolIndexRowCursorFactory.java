/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.table;

import com.questdb.cairo.BitmapIndexReader;
import com.questdb.cairo.EmptyRowCursor;
import com.questdb.cairo.TableReader;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.DataFrame;
import com.questdb.cairo.sql.RowCursor;
import com.questdb.cairo.sql.RowCursorFactory;
import com.questdb.cairo.sql.SymbolTable;

public class DeferredSymbolIndexRowCursorFactory implements RowCursorFactory {
    private final int columnIndex;
    private final boolean cachedIndexReaderCursor;
    private final String symbol;
    private int symbolKey;

    public DeferredSymbolIndexRowCursorFactory(int columnIndex, String symbol, boolean cachedIndexReaderCursor) {
        this.columnIndex = columnIndex;
        this.symbolKey = SymbolTable.VALUE_NOT_FOUND;
        this.symbol = symbol;
        this.cachedIndexReaderCursor = cachedIndexReaderCursor;
    }

    @Override
    public RowCursor getCursor(DataFrame dataFrame) {
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            return EmptyRowCursor.INSTANCE;
        }

        return dataFrame
                .getBitmapIndexReader(columnIndex, BitmapIndexReader.DIR_FORWARD)
                .getCursor(cachedIndexReaderCursor, symbolKey, dataFrame.getRowLo(), dataFrame.getRowHi() - 1);
    }

    @Override
    public void prepareCursor(TableReader tableReader) {
        int symbolKey = tableReader.getSymbolMapReader(columnIndex).getQuick(symbol);
        if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
            this.symbolKey = TableUtils.toIndexKey(symbolKey);
        }
    }
}
