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

package com.nfsdb.journal.factory.configuration;

import com.nfsdb.journal.column.ColumnType;

public class ColumnMetadata {
    public String name;
    public ColumnType type;
    public long offset;
    public int size;
    public int avgSize = Constants.DEFAULT_STRING_AVG_SIZE;
    public boolean indexed;
    public int bitHint;
    public int indexBitHint;
    public int distinctCountHint;
    public String sameAs;

    @Override
    public String toString() {
        return "ColumnMetadata{" +
                "name*='" + name + '\'' +
                ", type*=" + type +
                ", offset=" + offset +
                ", size*=" + size +
                ", avgSize=" + avgSize +
                ", indexed*=" + indexed +
                ", bitHint=" + bitHint +
                ", indexBitHint=" + indexBitHint +
                ", distinctCountHint*=" + distinctCountHint +
                ", sameAs*='" + sameAs + '\'' +
                '}';
    }

    public void copy(ColumnMetadata from) {
        this.name = from.name;
        this.type = from.type;
        this.offset = from.offset;
        this.size = from.size;
        this.avgSize = from.avgSize;
        this.indexed = from.indexed;
        this.bitHint = from.bitHint;
        this.indexBitHint = from.indexBitHint;
        this.distinctCountHint = from.distinctCountHint;
        this.sameAs = from.sameAs;
    }
}
