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

package com.questdb.ql;

import com.questdb.store.RecordColumnMetadata;
import com.questdb.store.SymbolTable;

public class RecordColumnMetadataImpl implements RecordColumnMetadata {
    private final String name;
    private final int type;
    private final SymbolTable symbolTable;
    private final int buckets;
    private final boolean indexed;

    public RecordColumnMetadataImpl(String name, int type) {
        this(name, type, null, 0, false);
    }

    public RecordColumnMetadataImpl(String name, int type, SymbolTable symbolTable, int buckets, boolean indexed) {
        this.name = name;
        this.type = type;
        this.symbolTable = symbolTable;
        this.buckets = buckets;
        this.indexed = indexed;
    }

    @Override
    public int getBucketCount() {
        return buckets;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public SymbolTable getSymbolTable() {
        return symbolTable;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public boolean isIndexed() {
        return indexed;
    }
}
