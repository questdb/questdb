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

package com.questdb.griffin.model;

import com.questdb.griffin.SqlNode;
import com.questdb.std.Mutable;
import com.questdb.std.ObjectFactory;

public class ColumnCastModel implements Mutable {
    public static final ObjectFactory<ColumnCastModel> FACTORY = ColumnCastModel::new;

    private SqlNode name;
    private int columnType;
    private int columnTypePos;
    private int symbolCapacity;

    @Override
    public void clear() {
        symbolCapacity = 0;
    }

    public int getColumnType() {
        return columnType;
    }

    public int getColumnTypePos() {
        return columnTypePos;
    }

    public SqlNode getName() {
        return name;
    }

    public void setName(SqlNode name) {
        this.name = name;
    }

    public int getSymbolCapacity() {
        return symbolCapacity;
    }

    public void setSymbolCapacity(int symbolCapacity) {
        this.symbolCapacity = symbolCapacity;
    }

    public void setType(int columnType, int columnTypePos) {
        this.columnType = columnType;
        this.columnTypePos = columnTypePos;
    }
}
