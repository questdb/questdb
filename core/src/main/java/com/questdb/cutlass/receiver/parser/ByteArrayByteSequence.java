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

package com.questdb.cutlass.receiver.parser;

import com.questdb.std.str.AbstractCharSequence;
import com.questdb.std.str.ByteSequence;

public class ByteArrayByteSequence extends AbstractCharSequence implements ByteSequence {

    private final byte[] array;
    private int top = 0;
    private int len;

    public ByteArrayByteSequence(byte[] array) {
        this.array = array;
        this.len = array.length;
    }

    @Override
    public byte byteAt(int index) {
        return array[top + index];
    }

    @Override
    public int length() {
        return len;
    }

    @Override
    public char charAt(int index) {
        return (char) byteAt(index);
    }

    ByteArrayByteSequence limit(int top, int len) {
        this.top = top;
        this.len = len;
        return this;
    }
}
