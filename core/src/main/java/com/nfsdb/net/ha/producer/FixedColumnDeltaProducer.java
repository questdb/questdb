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

package com.nfsdb.net.ha.producer;

import com.nfsdb.ex.JournalNetworkException;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.store.AbstractColumn;

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