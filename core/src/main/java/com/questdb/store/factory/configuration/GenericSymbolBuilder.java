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

package com.questdb.store.factory.configuration;

import com.questdb.store.ColumnType;

public class GenericSymbolBuilder extends GenericIndexedBuilder {
    public GenericSymbolBuilder(JournalStructure parent, ColumnMetadata meta) {
        super(parent, meta, ColumnType.SYMBOL, 4);
    }

    public GenericSymbolBuilder noCache() {
        this.meta.noCache = true;
        return this;
    }

    public GenericSymbolBuilder sameAs(String name) {
        this.meta.sameAs = name;
        return this;
    }

    public GenericSymbolBuilder size(int size) {
        this.meta.avgSize = size;
        return this;
    }
}
