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

package com.nfsdb.net.ha;

import com.nfsdb.ex.JournalNetworkException;
import com.nfsdb.misc.ByteBuffers;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public abstract class AbstractObjectProducer<T> implements ChannelProducer {

    private ByteBuffer buffer;

    @Override
    public void free() {
        buffer = ByteBuffers.release(buffer);
    }

    @Override
    public final boolean hasContent() {
        return buffer != null && buffer.hasRemaining();
    }

    @Override
    public final void write(WritableByteChannel channel) throws JournalNetworkException {
        ByteBuffers.copy(buffer, channel);
    }

    public void setValue(T value) {
        if (value != null) {
            int sz = getBufferSize(value);
            int bufSz = sz + 4;
            if (buffer == null || buffer.capacity() < bufSz) {
                ByteBuffers.release(buffer);
                buffer = ByteBuffer.allocateDirect(bufSz).order(ByteOrder.LITTLE_ENDIAN);
            }
            buffer.limit(bufSz);
            buffer.rewind();
            buffer.putInt(sz);
            write(value, buffer);
            buffer.flip();
        }
    }

    public final void write(WritableByteChannel channel, T value) throws JournalNetworkException {
        setValue(value);
        write(channel);
    }

    abstract protected int getBufferSize(T value);

    abstract protected void write(T value, ByteBuffer buffer);
}
