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

package io.questdb.std;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

public final class ByteBuffers {


    private ByteBuffers() {
    }

    public static void copy(ByteBuffer from, ByteBuffer to) {
        int x = from.remaining();
        int y = to.remaining();
        int d = Math.min(x, y);
        if ((from instanceof DirectBuffer) && (to instanceof DirectBuffer)) {
            Unsafe.getUnsafe().copyMemory(getAddress(from) + from.position(), getAddress(to) + to.position(), d);
            from.position(from.position() + d);
            to.position(to.position() + d);
        } else {
            to.put(from);
        }
    }

    @SuppressWarnings("unused")
    public static void dump(ByteBuffer b) {
        int p = b.position();
        while (b.hasRemaining()) {
            System.out.print((char) b.get());
        }
        b.position(p);
    }

    public static long getAddress(ByteBuffer buffer) {
        return ((DirectBuffer) buffer).address();
    }

    public static void putStr(ByteBuffer buffer, CharSequence value) {
        int p = buffer.position();
        for (int i = 0; i < value.length(); i++) {
            buffer.putChar(p, value.charAt(i));
            p += 2;
        }
        buffer.position(p);
    }

    /**
     * Releases ByteBuffer if possible. Call semantics should be as follows:
     * <p>
     * ByteBuffer buffer = ....
     * <p>
     * buffer = release(buffer);
     *
     * @param <T>    ByteBuffer subclass
     * @param buffer direct byte buffer
     * @return null if buffer is released or same buffer if release is not possible.
     */
    public static <T extends ByteBuffer> T release(final T buffer) {
        if (buffer instanceof DirectBuffer) {
            ((DirectBuffer) buffer).cleaner().clean();
            return null;
        }
        return buffer;
    }
}
