/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.factory.configuration;

import com.nfsdb.collections.FlyweightCharSequence;
import com.nfsdb.utils.Chars;

public class ColumnName {
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
    public String toString() {
        return underlying == null ? "null" : underlying.toString();
    }

    public boolean isNull() {
        return alias.length() == 0 && name.length() == 0;
    }

    public CharSequence name() {
        return name;
    }

    public ColumnName of(CharSequence that) {
        this.underlying = that;
        int dot = Chars.indexOf(that, '.');
        if (dot == -1) {
            alias.of(null, 0, 0);
            name.of(that, 0, that.length());
        } else {
            alias.of(that, 0, dot);
            name.of(that, dot + 1, that.length() - dot - 1);
        }
        return this;
    }
}
