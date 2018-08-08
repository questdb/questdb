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

package com.questdb.ql.join;

import com.questdb.store.StorageFacade;
import com.questdb.store.SymbolTable;

public class SplitRecordStorageFacade implements StorageFacade {
    private final int split;
    private StorageFacade a;
    private StorageFacade b;

    public SplitRecordStorageFacade(int split) {
        this.split = split;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return columnIndex < split ? a.getSymbolTable(columnIndex) : b.getSymbolTable(columnIndex - split);
    }

    public void prepare(StorageFacade a, StorageFacade b) {
        this.a = a;
        this.b = b;
    }
}
