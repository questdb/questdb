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

package com.nfsdb.net.comsumer;

import com.nfsdb.column.AbstractColumn;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.net.AbstractChannelConsumer;
import com.nfsdb.utils.ByteBuffers;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

public class FixedColumnDeltaConsumer extends AbstractChannelConsumer {

    private final ByteBuffer header = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
    private final AbstractColumn column;
    private long appendOffset;
    private long targetOffset = -1;
    private boolean headerProcessed = false;

    public FixedColumnDeltaConsumer(AbstractColumn column) {
        this.column = column;
        this.appendOffset = column.getOffset();
    }

    @Override
    public void free() {
        ByteBuffers.release(header);
        super.free();
    }

    @Override
    public boolean isComplete() {
        return appendOffset == targetOffset;
    }

    @Override
    public void reset() {
        super.reset();
        appendOffset = column.getOffset();
        targetOffset = -1;
        headerProcessed = false;
        header.rewind();
    }

    @Override
    protected void doRead(ReadableByteChannel channel) throws JournalNetworkException {
        ByteBuffers.copy(channel, header);

        // if header is complete, extract target column size and release header
        // so we don't attempt to fill it up again
        if (!headerProcessed && header.remaining() == 0) {
            header.flip();
            this.targetOffset = appendOffset + header.getLong();
            headerProcessed = true;
        }

        while (!isComplete()) {
            ByteBuffer target = column.getBuffer(appendOffset, 1);
            int sz = ByteBuffers.copy(channel, target, targetOffset - appendOffset);
            // using non-blocking IO it should be possible not to read anything
            // we need to give up here and let the rest of execution continue
            if (sz == 0) {
                break;
            }
            appendOffset += sz;
        }
    }

    @Override
    protected void commit() {
        column.preCommit(targetOffset);
    }
}
