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
import com.nfsdb.misc.Unsafe;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

public abstract class AbstractObjectConsumer extends AbstractChannelConsumer {

    private final ByteBuffer header = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
    private final long headerAddress = ByteBuffers.getAddress(header);
    private ByteBuffer valueBuffer;

    @Override
    public void free() {
        valueBuffer = ByteBuffers.release(valueBuffer);
        ByteBuffers.release(header);
    }

    @Override
    protected final void doRead(ReadableByteChannel channel) throws JournalNetworkException {
        header.position(0);
        ByteBuffers.copy(channel, header);

        int bufSz = Unsafe.getUnsafe().getInt(headerAddress);
        if (valueBuffer == null || valueBuffer.capacity() < bufSz) {
            ByteBuffers.release(valueBuffer);
            valueBuffer = ByteBuffer.allocateDirect(bufSz).order(ByteOrder.LITTLE_ENDIAN);
        } else {
            valueBuffer.rewind();
        }
        valueBuffer.limit(bufSz);
        ByteBuffers.copy(channel, valueBuffer, bufSz);
    }

    final ByteBuffer getValueBuffer() {
        return valueBuffer;
    }
}
