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

package com.nfsdb.std;

import com.nfsdb.misc.Os;
import com.nfsdb.misc.Unsafe;

import java.io.Closeable;

public final class Path extends AbstractCharSequence implements Closeable, LPSZ {
    private long ptr = 0;
    private int capacity;
    private int len;

    public Path() {
        alloc(128);
    }

    public Path(CharSequence str) {
        int l = str.length();
        alloc(l);
        copy(str, l);
    }

    @Override
    public long address() {
        return ptr;
    }

    @Override
    public void close() {
        if (ptr != 0) {
            Unsafe.getUnsafe().freeMemory(ptr);
            ptr = 0;
        }
    }

    @Override
    public int length() {
        return len;
    }

    @Override
    public char charAt(int index) {
        return (char) Unsafe.getUnsafe().getByte(ptr + index);
    }

    public Path of(CharSequence str) {
        int l = str.length();
        if (l >= capacity) {
            Unsafe.getUnsafe().freeMemory(ptr);
            alloc(l);
        }
        copy(str, l);
        this.len = l;
        return this;
    }

    private void alloc(int len) {
        this.capacity = len;
        this.ptr = Unsafe.getUnsafe().allocateMemory(len + 1);
    }

    private void copy(CharSequence str, int len) {
        for (int i = 0; i < len; i++) {
            char c = str.charAt(i);
            Unsafe.getUnsafe().putByte(ptr + i, (byte) (Os.type == Os.WINDOWS && c == '/' ? '\\' : c));
        }
        Unsafe.getUnsafe().putByte(ptr + len, (byte) 0);
    }
}
