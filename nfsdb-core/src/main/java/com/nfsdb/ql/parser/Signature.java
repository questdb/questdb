/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.parser;

import com.nfsdb.collections.IntList;
import com.nfsdb.collections.ObjList;
import com.nfsdb.storage.ColumnType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public final class Signature {
    public final ObjList<ColumnType> paramTypes = new ObjList<>();
    public final IntList constParams = new IntList();
    private final StringBuilder b = new StringBuilder();
    public CharSequence name;
    public int paramCount;

    public void clear() {
        paramTypes.clear();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Signature)) return false;
        Signature that = (Signature) o;
        return paramCount == that.paramCount && name.equals(that.name) && typesEqual(that);
    }

    @Override
    public int hashCode() {
        return typesHashCode(31 * name.hashCode() + paramCount);
    }

    public Signature paramType(int pos, ColumnType type, boolean constant) {
        paramTypes.setQuick(pos, type);
        constParams.setQuick(pos, constant ? 1 : 0);
        return this;
    }

    public Signature setName(CharSequence name) {
        this.name = name;
        return this;
    }

    public Signature setParamCount(int paramCount) {
        this.paramCount = paramCount;
        this.paramTypes.ensureCapacity(paramCount);
        this.constParams.ensureCapacity(paramCount);
        return this;
    }

    public CharSequence userReadable() {
        b.setLength(0);
        b.append('\'');
        b.append(name);
        b.append('\'');
        b.append('(');
        for (int i = 0, n = paramCount; i < n; i++) {
            if (i > 0) {
                b.append(", ");
            }
            if (constParams.getQuick(i) == 1) {
                b.append("const ");
            }
            b.append(paramTypes.getQuick(i));

        }
        b.append(')');
        return b;
    }

    @SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
    private boolean typesEqual(Signature that) {
        int k;
        if ((k = this.paramTypes.size()) != that.paramTypes.size()) {
            return false;
        }

        for (int i = 0; i < k; i++) {
            if (this.paramTypes.getQuick(i) != that.paramTypes.getQuick(i) || this.constParams.getQuick(i) != that.constParams.getQuick(i)) {
                return false;
            }
        }
        return true;
    }

    @SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
    private int typesHashCode(int h) {
        for (int i = 0, k = paramTypes.size(); i < k; i++) {
            h = h * 32 + paramTypes.getQuick(i).ordinal();
            h = h * 32 + constParams.getQuick(i);
        }
        return h;
    }
}
