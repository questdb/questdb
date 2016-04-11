/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 *
 ******************************************************************************/

package com.nfsdb.ql.ops;

import com.nfsdb.std.IntList;
import com.nfsdb.std.Mutable;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.ColumnType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public final class Signature implements Mutable {
    public final ObjList<ColumnType> paramTypes = new ObjList<>();
    private final IntList constParams = new IntList();
    private final StringBuilder b = new StringBuilder();
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
