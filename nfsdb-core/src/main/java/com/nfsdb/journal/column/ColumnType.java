/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.journal.column;

import java.nio.ByteBuffer;

public enum ColumnType {
    BOOLEAN(boolean.class, 1),
    BYTE(byte.class, 1),
    DOUBLE(double.class, 8),
    INT(int.class, 4),
    LONG(long.class, 8),
    SHORT(short.class, 2),
    STRING(String.class, 0),
    SYMBOL(null, 4),
    BINARY(ByteBuffer.class, 0),
    DATE(long.class, 8);

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
