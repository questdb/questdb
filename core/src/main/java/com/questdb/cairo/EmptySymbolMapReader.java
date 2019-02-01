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

package com.questdb.cairo;

import com.questdb.cairo.sql.SymbolTable;

public class EmptySymbolMapReader implements SymbolMapReader {

    public static final EmptySymbolMapReader INSTANCE = new EmptySymbolMapReader();

    @Override
    public int getQuick(CharSequence value) {
        return SymbolTable.VALUE_NOT_FOUND;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public CharSequence value(int key) {
        return null;
    }

    @Override
    public int getSymbolCapacity() {
        return 0;
    }

    @Override
    public boolean isCached() {
        return false;
    }

    @Override
    public boolean isDeleted() {
        return true;
    }

    @Override
    public void updateSymbolCount(int count) {
    }
}
