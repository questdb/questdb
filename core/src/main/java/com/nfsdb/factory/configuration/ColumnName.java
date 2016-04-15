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

package com.nfsdb.factory.configuration;

import com.nfsdb.misc.Chars;
import com.nfsdb.std.FlyweightCharSequence;
import org.jetbrains.annotations.NotNull;

class ColumnName implements CharSequence {
    private final FlyweightCharSequence alias = new FlyweightCharSequence();
    private final FlyweightCharSequence name = new FlyweightCharSequence();
    private CharSequence underlying;

    public CharSequence alias() {
        return alias;
    }

    @Override
    public int hashCode() {
        return Chars.hashCode(underlying);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ColumnName)) {
            return false;
        }
        ColumnName that = (ColumnName) obj;
        return Chars.equals(alias, that.alias()) && Chars.equals(name, that.name());
    }

    @Override
    public
    @NotNull
    String toString() {
        return underlying == null ? "null" : underlying.toString();
    }

    public boolean isNull() {
        return alias.length() == 0 && name.length() == 0;
    }

    @Override
    public int length() {
        return underlying.length();
    }

    @Override
    public char charAt(int index) {
        return underlying.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        throw new UnsupportedOperationException();
    }

    public CharSequence name() {
        return name;
    }

    public void of(CharSequence that) {
        this.underlying = that;
        int dot = Chars.indexOf(that, '.');
        if (dot == -1) {
            alias.of(null, 0, 0);
            name.of(that, 0, that.length());
        } else {
            alias.of(that, 0, dot);
            name.of(that, dot + 1, that.length() - dot - 1);
        }
    }
}
