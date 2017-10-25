/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.store.factory.configuration;

public abstract class AbstractMetadataBuilder<T> {
    final ColumnMetadata meta;
    private final JournalMetadataBuilder<T> parent;

    AbstractMetadataBuilder(JournalMetadataBuilder<T> parent, ColumnMetadata meta) {
        this.parent = parent;
        this.meta = meta;
    }

    public BinaryBuilder $bin(String name) {
        return parent.$bin(name);
    }

    public JournalMetadataBuilder<T> $date(String name) {
        return parent.$date(name);
    }

    public IntBuilder $int(String name) {
        return parent.$int(name);
    }

    public StringBuilder $str(String name) {
        return parent.$str(name);
    }

    public SymbolBuilder $sym(String name) {
        return parent.$sym(name);
    }

    public JournalMetadataBuilder<T> $ts() {
        return parent.$ts();
    }

    public JournalMetadataBuilder<T> $ts(String name) {
        return parent.$ts(name);
    }
}
