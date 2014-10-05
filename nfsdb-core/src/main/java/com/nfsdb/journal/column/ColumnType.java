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
    BOOLEAN(boolean.class, 1, true),
    BYTE(byte.class, 1, true),
    DOUBLE(double.class, 8, true),
    INT(int.class, 4, true),
    LONG(long.class, 8, true),
    SHORT(short.class, 2, true),
    STRING(String.class, 0, false),
    SYMBOL(null, 4, true),
    BINARY(ByteBuffer.class, 0, false),
    DATE(long.class, 8, true);

    private final Class type;
    private final boolean primitive;
    private final int size;

    public boolean matches(Class type) {
        return this.type == type;
    }

    public boolean primitive() {
        return primitive;
    }

    public int size() {
        return size;
    }


    ColumnType(Class type, int size, boolean primitive) {
        this.type = type;
        this.size = size;
        this.primitive = primitive;
    }
}
