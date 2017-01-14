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

package com.questdb.net.ha.comsumer;

import com.questdb.ex.JournalNetworkException;
import com.questdb.misc.ByteBuffers;
import com.questdb.misc.Unsafe;
import com.questdb.net.ha.AbstractChannelConsumer;
import com.questdb.store.AbstractColumn;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

public class FixedColumnDeltaConsumer extends AbstractChannelConsumer {

    private final ByteBuffer header = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
    private final long headerAddress = ByteBuffers.getAddress(header);
    private final AbstractColumn column;
    private long targetOffset = -1;

    public FixedColumnDeltaConsumer(AbstractColumn column) {
        this.column = column;
    }

    public void free() {
        ByteBuffers.release(header);
    }

    @Override
    protected void commit() {
        column.preCommit(targetOffset);
    }

    @Override
    protected void doRead(ReadableByteChannel channel) throws JournalNetworkException {
        header.position(0);
        ByteBuffers.copy(channel, header);
        long offset = column.getOffset();
        targetOffset = offset + Unsafe.getUnsafe().getInt(headerAddress);

        while (offset < targetOffset) {
            int sz = ByteBuffers.copy(channel, column.getBuffer(offset), targetOffset - offset);
            // using non-blocking IO it should be possible not to read anything
            // we need to give up here and let the rest of execution continue
            if (sz == 0) {
                break;
            }
            offset += sz;
        }
    }
}
