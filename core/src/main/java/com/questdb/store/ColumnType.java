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

package com.questdb.store;

import java.nio.ByteBuffer;

public enum ColumnType {
    BOOLEAN(boolean.class, 1),
    BYTE(byte.class, 1),
    DOUBLE(double.class, 8),
    FLOAT(float.class, 4),
    INT(int.class, 4),
    LONG(long.class, 8),
    SHORT(short.class, 2),
    STRING(String.class, 0),
    SYMBOL(null, 4),
    BINARY(ByteBuffer.class, 0),
    DATE(long.class, 8),
    PARAMETER(null, 0);

    private final Class type;
    private final int size;

    ColumnType(Class type, int size) {
        this.type = type;
        this.size = size;
    }

    public boolean matches(Class type) {
        return this.type == type;
    }

    public int size() {
        return size;
    }
}
