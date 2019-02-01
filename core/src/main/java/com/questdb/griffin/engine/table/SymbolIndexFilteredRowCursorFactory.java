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

import com.questdb.cairo.TableReader;
import com.questdb.cairo.sql.DataFrame;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.RowCursor;
import com.questdb.cairo.sql.RowCursorFactory;

public class SymbolIndexFilteredRowCursorFactory implements RowCursorFactory {
    private final SymbolIndexFilteredRowCursor cursor;

    public SymbolIndexFilteredRowCursorFactory(int columnIndex, int symbolKey, Function filter, boolean cachedIndexReaderCursor) {
        this.cursor = new SymbolIndexFilteredRowCursor(columnIndex, symbolKey, filter, cachedIndexReaderCursor);
    }

    @Override
    public RowCursor getCursor(DataFrame dataFrame) {
        return cursor.of(dataFrame);
    }

    @Override
    public void prepareCursor(TableReader tableReader) {
        this.cursor.setTableReader(tableReader);
    }
}
