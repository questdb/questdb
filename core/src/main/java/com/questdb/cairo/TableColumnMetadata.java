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

public class TableColumnMetadata {
    private final String name;
    private final int type;
    private final boolean indexed;
    private final int indexValueBlockCapacity;

    public TableColumnMetadata(String name, int type) {
        this(name, type, false, 0);
    }

    public TableColumnMetadata(String name, int type, boolean indexFlag, int indexValueBlockCapacity) {
        this.name = name;
        this.type = type;
        this.indexed = indexFlag;
        this.indexValueBlockCapacity = indexValueBlockCapacity;
    }

    public int getIndexValueBlockCapacity() {
        return indexValueBlockCapacity;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public boolean isIndexed() {
        return indexed;
    }
}
