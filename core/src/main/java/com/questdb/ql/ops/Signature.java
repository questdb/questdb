/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.ops;

import com.questdb.std.IntList;
import com.questdb.std.Misc;
import com.questdb.std.Mutable;
import com.questdb.std.str.StringSink;
import com.questdb.store.ColumnType;

public final class Signature implements Mutable {
    public final IntList paramTypes = new IntList(2);
    private final IntList constParams = new IntList(2);
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

    public Signature paramType(int pos, int columnType, boolean constant) {
        paramTypes.setQuick(pos, columnType);
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
        StringSink b = Misc.getThreadLocalBuilder();
        b.put('\'');
        b.put(name);
        b.put('\'');
        b.put('(');
        for (int i = 0, n = paramCount; i < n; i++) {
            if (i > 0) {
                b.put(", ");
            }
            if (constParams.getQuick(i) == 1) {
                b.put("const ");
            }
            b.put(ColumnType.nameOf(paramTypes.getQuick(i)));

        }
        b.put(')');
        return b;
    }

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

    private int typesHashCode(int h) {
        for (int i = 0, k = paramTypes.size(); i < k; i++) {
            h = h * 32 + paramTypes.getQuick(i);
            h = h * 32 + constParams.getQuick(i);
        }
        return h;
    }
}
