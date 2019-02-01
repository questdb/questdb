/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.std.str;

import com.questdb.std.Chars;
import com.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractCharSequence implements CharSequence, CloneableMutable {

    public static String getString(CharSequence cs) {
        final CharSink b = Misc.getThreadLocalBuilder();
        b.put(cs);
        return b.toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T copy() {
        return (T) this.toString();
    }

    @Override
    public int hashCode() {
        return Chars.hashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof CharSequence && Chars.equals(this, (CharSequence) obj);
    }

    @NotNull
    @Override
    public String toString() {
        return getString(this);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        throw new UnsupportedOperationException();
    }
}
