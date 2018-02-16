/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.cairo.TableReader;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.cairo.sql.DataFrame;
import com.questdb.cairo.sql.RowCursorFactory;
import com.questdb.common.RowCursor;

public class SymbolIndexRowCursorFactory implements RowCursorFactory {
    private final int columnIndex;
    private final int symbolKey;

    public SymbolIndexRowCursorFactory(CairoEngine engine, CharSequence tableName, CharSequence columnName, CharSequence value) {
        try (TableReader reader = engine.getReader(tableName)) {
            this.columnIndex = reader.getMetadata().getColumnIndex(columnName);
            this.symbolKey = reader.getSymbolMapReader(this.columnIndex).getQuick(value) + 1;
        }
    }

    @Override
    public RowCursor getCursor(DataFrame dataFrame) {
        return dataFrame.getBitmapIndexReader(columnIndex).getCursor(symbolKey, dataFrame.getRowHi() - 1);
    }
}
