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

package com.questdb.griffin.engine.groupby;

import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.SymbolTable;
import com.questdb.griffin.engine.functions.SymbolFunction;
import com.questdb.griffin.engine.functions.bind.BindVariableService;

public class MapSymbolColumn extends SymbolFunction {
    private final int columnIndex;
    private SymbolTable symbolTable;

    public MapSymbolColumn(int position, int columnIndex) {
        super(position);
        this.columnIndex = columnIndex;
    }

    @Override
    public int getInt(Record rec) {
        return rec.getInt(columnIndex);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return symbolTable.value(getInt(rec));
    }

    @Override
    public void init(RecordCursor recordCursor, BindVariableService bindVariableService) {
        this.symbolTable = recordCursor.getSymbolTable(columnIndex);
    }
}
