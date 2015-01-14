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

package com.nfsdb.net.producer;

import com.nfsdb.column.AbstractColumn;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.utils.ByteBuffers;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class FixedColumnDeltaProducer implements ColumnDeltaProducer {

    private static final int REPLICATION_FRAGMENT_HEADER_SIZE = 8;
    private final ByteBuffer header = ByteBuffer.allocateDirect(REPLICATION_FRAGMENT_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    private final AbstractColumn column;
    private long offset;
    private long targetOffset;
    private long nextOffset;
    private boolean hasContent = false;

    public FixedColumnDeltaProducer(AbstractColumn column) {
        this.column = column;
    }

    public void configure(long localRowID, long limit) {
        long sz = column.size();
        this.offset = localRowID >= sz ? column.getOffset() : column.getOffset(localRowID);
        this.targetOffset = limit >= sz ? column.getOffset() : column.getOffset(limit);
        this.header.rewind();
        this.header.putLong(targetOffset - offset);
        this.header.flip();
        this.nextOffset = offset;
        this.hasContent = targetOffset - offset > 0;
    }

    @Override
    public boolean hasContent() {
        return hasContent;
    }

    @Override
    public void write(WritableByteChannel channel) throws JournalNetworkException {
        if (hasContent()) {
            ByteBuffers.copy(header, channel);
            while (offset < targetOffset) {
                ByteBuffer b = column.getBuffer(offset, 1);
                offset += ByteBuffers.copy(b, channel, (int) (targetOffset - offset));
            }
            hasContent = false;
        }
    }

    @Override
    public String toString() {
        return "ColumnDelta{" +
                "offset=" + offset +
                ", targetOffset=" + targetOffset +
                ", nextOffset=" + nextOffset +
                ", column=" + column +
                '}';
    }
}