/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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
 ******************************************************************************/

package com.questdb.net.ha.producer;

import com.questdb.ex.JournalNetworkException;
import com.questdb.misc.ByteBuffers;
import com.questdb.store.AbstractColumn;

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
        long sz = column.size() - 1;
        this.offset = localRowID > sz ? column.getOffset() : column.getOffset(localRowID);
        this.targetOffset = limit > sz ? column.getOffset() : column.getOffset(limit);
        this.header.rewind();
        this.header.putLong(targetOffset - offset);
        this.header.flip();
        this.nextOffset = offset;
        this.hasContent = targetOffset - offset > 0;
    }

    @Override
    public void free() {
        ByteBuffers.release(header);
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
                offset += ByteBuffers.copy(column.getBuffer(offset, 1), channel, targetOffset - offset);
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