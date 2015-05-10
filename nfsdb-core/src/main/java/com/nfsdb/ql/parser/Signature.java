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

package com.nfsdb.ql.parser;

import com.nfsdb.collections.ObjList;
import com.nfsdb.storage.ColumnType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class Signature {
    public final ObjList<ColumnType> paramTypes = new ObjList<>();
    public CharSequence name;
    public int paramCount;

    public void clear() {
        paramTypes.clear();
    }

    @Override
    public int hashCode() {
        return typesHashCode(31 * name.hashCode() + paramCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Signature)) return false;
        Signature that = (Signature) o;
        return paramCount == that.paramCount && name.equals(that.name) && typesEqual(that);
    }

    @Override
    public String toString() {
        return "Signature{" +
                "name='" + name + "'" +
                ", paramTypes=" + paramTypes +
                '}';
    }

    public Signature paramType(int pos, ColumnType type) {
        paramTypes.setQuick(pos, type);
        return this;
    }

    public Signature setName(CharSequence name) {
        this.name = name;
        return this;
    }

    public Signature setParamCount(int paramCount) {
        this.paramCount = paramCount;
        this.paramTypes.ensureCapacity(paramCount);
        return this;
    }

    @SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
    private boolean typesEqual(Signature that) {
        int k;
        if ((k = this.paramTypes.size()) != that.paramTypes.size()) {
            return false;
        }

        for (int i = 0; i < k; i++) {
            if (this.paramTypes.getQuick(i) != that.paramTypes.getQuick(i)) {
                return false;
            }
        }
        return true;
    }

    @SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
    private int typesHashCode(int h) {
        for (int i = 0, k = paramTypes.size(); i < k; i++) {
            h = h * 32 + paramTypes.getQuick(i).ordinal();
        }
        return h;
    }
}
