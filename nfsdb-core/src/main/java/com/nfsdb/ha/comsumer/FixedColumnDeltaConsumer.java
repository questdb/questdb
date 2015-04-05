/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ha.comsumer;

import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.ha.AbstractChannelConsumer;
import com.nfsdb.storage.AbstractColumn;
import com.nfsdb.utils.ByteBuffers;
import com.nfsdb.utils.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

public class FixedColumnDeltaConsumer extends AbstractChannelConsumer {

    private final ByteBuffer header = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
    private final long headerAddress = ((DirectBuffer) header).address();
    private final AbstractColumn column;
    private long targetOffset = -1;

    public FixedColumnDeltaConsumer(AbstractColumn column) {
        this.column = column;
    }

    @Override
    public void free() {
        ByteBuffers.release(header);
        super.free();
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
            int sz = ByteBuffers.copy(channel, column.getBuffer(offset, 1), targetOffset - offset);
            // using non-blocking IO it should be possible not to read anything
            // we need to give up here and let the rest of execution continue
            if (sz == 0) {
                break;
            }
            offset += sz;
        }
    }
}
