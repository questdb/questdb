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

public final class ImmutableCharSequence extends AbstractCharSequence {

    private final char[] chars;
    private final int len;

    private ImmutableCharSequence(CharSequence that) {
        this.len = that.length();
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
    public int length() {
        return len;
    }

    @Override
    public char charAt(int index) {
        return chars[index];
    }
}
