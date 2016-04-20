/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.factory.configuration;

import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;
import com.questdb.store.UnstructuredFile;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
public class ColumnMetadata implements RecordColumnMetadata {
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
    public int getBucketCount() {
        return distinctCountHint;
    }

    @Override
    public String getName() {
        return name;
    }

    public ColumnMetadata setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public SymbolTable getSymbolTable() {
        return symbolTable;
    }

    @Override
    public ColumnType getType() {
        return type;
    }

    public ColumnMetadata setType(ColumnType type) {
        this.type = type;
        return this;
    }

    @Override
    public boolean isIndexed() {
        return indexed;
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
        return 31 * result + (noCache ? 1 : 0);
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

    public void read(UnstructuredFile buf) {
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

    public void write(UnstructuredFile buf) {
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
