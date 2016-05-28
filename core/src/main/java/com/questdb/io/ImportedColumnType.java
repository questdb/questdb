/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.io;

import com.questdb.store.ColumnType;

public enum ImportedColumnType {

    BOOLEAN(ColumnType.BOOLEAN),
    BYTE(ColumnType.BYTE),
    DOUBLE(ColumnType.DOUBLE),
    FLOAT(ColumnType.FLOAT),
    INT(ColumnType.INT),
    LONG(ColumnType.LONG),
    SHORT(ColumnType.SHORT),
    STRING(ColumnType.STRING),
    SYMBOL(ColumnType.SYMBOL),
    DATE_ISO(ColumnType.DATE),
    DATE_1(ColumnType.DATE),
    DATE_2(ColumnType.DATE),
    DATE_3(ColumnType.DATE);

    private final ColumnType columnType;

    ImportedColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }

    public ColumnType getColumnType() {
        return columnType;
    }
}
