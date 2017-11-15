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

import com.questdb.std.Numbers;

public class GenericIndexedBuilder extends AbstractGenericMetadataBuilder {

    public GenericIndexedBuilder(JournalStructure parent, ColumnMetadata meta, int type, int size) {
        super(parent, meta);
        this.meta.type = type;
        this.meta.size = size;
    }

    public GenericIndexedBuilder buckets(int buckets) {
        this.meta.distinctCountHint = Numbers.ceilPow2(buckets) - 1;
        return this;
    }

    public GenericIndexedBuilder index() {
        this.meta.indexed = true;
        return this;
    }
}
