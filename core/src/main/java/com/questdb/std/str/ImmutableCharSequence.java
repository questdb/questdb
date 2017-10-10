/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

import com.questdb.misc.Chars;

public final class ImmutableCharSequence extends AbstractCharSequence {

    private final char[] chars;

    private ImmutableCharSequence(CharSequence that) {
        int len = that.length();
        this.chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = that.charAt(i);
        }
    }

    public static CharSequence of(CharSequence charSequence) {
        if (charSequence instanceof ImmutableCharSequence) {
            return charSequence;
        }
        return new ImmutableCharSequence(charSequence);
    }

    @Override
    public int hashCode() {
        return Chars.hashCode(this);
    }

    @Override
    public boolean equals(Object that) {
        return this == that || that instanceof CharSequence && Chars.equals(this, (CharSequence) that);
    }

    @Override
    public int length() {
        return chars.length;
    }

    @Override
    public char charAt(int index) {
        return chars[index];
    }
}
