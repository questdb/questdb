/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.io;

import com.nfsdb.store.ColumnType;

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
