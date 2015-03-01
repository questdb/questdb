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

package com.nfsdb.factory.configuration;

import com.nfsdb.storage.ColumnType;
import com.nfsdb.storage.HugeBuffer;
import com.nfsdb.storage.SymbolTable;

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
    public boolean noCache = false;
    public SymbolTable symbolTable;

    public ColumnMetadata copy(ColumnMetadata from) {
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
        this.noCache = from.noCache;
        return this;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + size;
        result = 31 * result + avgSize;
        result = 31 * result + (indexed ? 1 : 0);
        result = 31 * result + bitHint;
        result = 31 * result + indexBitHint;
        result = 31 * result + distinctCountHint;
        result = 31 * result + (sameAs != null ? sameAs.hashCode() : 0);
        result = 31 * result + (noCache ? 1 : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnMetadata that = (ColumnMetadata) o;

        return avgSize == that.avgSize
                && bitHint == that.bitHint
                && distinctCountHint == that.distinctCountHint
                && indexBitHint == that.indexBitHint
                && indexed == that.indexed
                && noCache == that.noCache
                && size == that.size
                && name.equals(that.name)
                && !(sameAs != null ? !sameAs.equals(that.sameAs) : that.sameAs != null)
                && type == that.type;

    }

    @Override
    public String toString() {
        return "ColumnMetadata{" +
                "name*='" + name + '\'' +
                ", type*=" + type +
                ", offset=" + offset +
                ", size*=" + size +
                ", avgSize=" + avgSize +
                ", indexed=" + indexed +
                ", bitHint=" + bitHint +
                ", indexBitHint=" + indexBitHint +
                ", distinctCountHint=" + distinctCountHint +
                ", sameAs='" + sameAs + '\'' +
                ", noCache=" + noCache +
                '}';
    }

    public void read(HugeBuffer buf) {
        name = buf.getStr();
        type = ColumnType.valueOf(buf.getStr());
        size = buf.getInt();
        avgSize = buf.getInt();
        indexed = buf.getBool();
        bitHint = buf.getInt();
        indexBitHint = buf.getInt();
        distinctCountHint = buf.getInt();
        sameAs = buf.getStr();
        noCache = buf.getBool();
    }

    public ColumnMetadata setName(String name) {
        this.name = name;
        return this;
    }

    public ColumnMetadata setType(ColumnType type) {
        this.type = type;
        return this;
    }

    public void write(HugeBuffer buf) {
        buf.put(name);
        buf.put(type.name());
        buf.put(size);
        buf.put(avgSize);
        buf.put(indexed);
        buf.put(bitHint);
        buf.put(indexBitHint);
        buf.put(distinctCountHint);
        buf.put(sameAs);
        buf.put(noCache);
    }
}
