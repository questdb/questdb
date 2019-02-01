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
import com.questdb.std.Mutable;
import org.jetbrains.annotations.NotNull;

public class StringSink extends AbstractCharSink implements CharSequence, Mutable, CloneableMutable {
    private final StringBuilder builder = new StringBuilder();

    public void clear(int pos) {
        builder.setLength(pos);
    }

    public void clear() {
        builder.setLength(0);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T copy() {
        return (T) toString();
    }

    @Override
    public int hashCode() {
        return Chars.hashCode(builder);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CharSequence && Chars.equals(builder, (CharSequence) obj);
    }

    /* Either IDEA or FireBug complain, annotation galore */
    @NotNull
    @Override
    public String toString() {
        return builder.toString();
    }

    @Override
    public int length() {
        return builder.length();
    }

    @Override
    public char charAt(int index) {
        return builder.charAt(index);
    }

    @Override
    public CharSequence subSequence(int lo, int hi) {
        return builder.subSequence(lo, hi);
    }

    @Override
    public CharSink put(CharSequence cs) {
        if (cs != null) {
            builder.append(cs);
        }
        return this;
    }

    @Override
    public CharSink put(CharSequence cs, int start, int end) {
        if (cs != null) {
            builder.append(cs, start, end);
        }
        return this;
    }

    @Override
    public CharSink put(char c) {
        builder.append(c);
        return this;
    }

    public CharSink put(char c, int n) {
        for (int i = 0; i < n; i++) {
            builder.append(c);
        }
        return this;
    }
}
